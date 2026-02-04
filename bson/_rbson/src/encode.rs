// Copyright 2025-present MongoDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! BSON encoding functions
//!
//! This module contains all functions for encoding Python objects to BSON bytes.

use bson::{doc, Bson, Document};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyAny, PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use std::io::Cursor;

use crate::errors::{invalid_document_error, invalid_document_error_with_doc};
use crate::types::{
    TYPE_CACHE, BINARY_TYPE_MARKER, CODE_TYPE_MARKER, DATETIME_TYPE_MARKER, DBPOINTER_TYPE_MARKER,
    DBREF_TYPE_MARKER, DECIMAL128_TYPE_MARKER, INT64_TYPE_MARKER, MAXKEY_TYPE_MARKER,
    MINKEY_TYPE_MARKER, OBJECTID_TYPE_MARKER, REGEX_TYPE_MARKER, SYMBOL_TYPE_MARKER,
    TIMESTAMP_TYPE_MARKER,
};
use crate::utils::{datetime_to_millis, int_flags_to_str, validate_key, write_cstring, write_string};

#[pyfunction]
#[pyo3(signature = (obj, check_keys, _codec_options))]
pub fn _dict_to_bson(
    py: Python,
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    _codec_options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    let codec_options = Some(_codec_options);

    // Use python_mapping_to_bson_doc for efficient encoding
    // This uses items() method and efficient tuple extraction
    // See PR #2695 for implementation details and performance analysis
    let doc = python_mapping_to_bson_doc(obj, check_keys, codec_options, true)
        .map_err(|e| {
            // Match C extension behavior: TypeError for non-mapping types, InvalidDocument for encoding errors
            let err_str = e.to_string();

            // If it's a TypeError about mapping type, pass it through unchanged (matches C extension)
            if err_str.contains("encoder expected a mapping type") {
                return e;
            }

            // For other errors, wrap in InvalidDocument with document property
            if err_str.contains("cannot encode object:") || err_str.contains("Object must be a dict") {
                // Strip "InvalidDocument: " prefix if present, then add "Invalid document: "
                let msg = if let Some(stripped) = err_str.strip_prefix("InvalidDocument: ") {
                    format!("Invalid document: {}", stripped)
                } else {
                    format!("Invalid document: {}", err_str)
                };
                invalid_document_error_with_doc(py, msg, obj)
            } else {
                e
            }
        })?;

    // Use to_writer() to write directly to buffer
    // This is faster than bson::to_vec() which creates an intermediate Vec
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)
        .map_err(|e| invalid_document_error(py, format!("Failed to serialize BSON: {}", e)))?;

    Ok(PyBytes::new(py, &buf).into())
}

/// Encode a Python dictionary to BSON bytes WITHOUT using Bson types
/// This version writes bytes directly from Python objects for better performance
#[pyfunction]
#[pyo3(signature = (obj, check_keys, _codec_options))]
pub fn _dict_to_bson_direct(
    py: Python,
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    _codec_options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    let codec_options = Some(_codec_options);

    // Write directly to bytes without converting to Bson types
    let mut buf = Vec::new();
    write_document_bytes_direct(&mut buf, obj, check_keys, codec_options, true)
        .map_err(|e| {
            // Match C extension behavior: TypeError for non-mapping types, InvalidDocument for encoding errors
            let err_str = e.to_string();

            // If it's a TypeError about mapping type, pass it through unchanged (matches C extension)
            if err_str.contains("encoder expected a mapping type") {
                return e;
            }

            // For other errors, wrap in InvalidDocument with document property
            if err_str.contains("cannot encode object:") {
                let msg = format!("Invalid document: {}", err_str);
                invalid_document_error_with_doc(py, msg, obj)
            } else {
                e
            }
        })?;

    Ok(PyBytes::new(py, &buf).into())
}

/// Read a BSON document directly from bytes and convert to Python dict

fn write_document_bytes(
    buf: &mut Vec<u8>,
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
    is_top_level: bool,
) -> PyResult<()> {
    use std::io::Write;

    // Reserve space for document size (will be filled in at the end)
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Handle _id field first if this is top-level
    let mut id_written = false;

    // FAST PATH: Check if it's a PyDict first (most common case)
    if let Ok(dict) = obj.downcast::<PyDict>() {
        // First pass: write _id if present at top level
        if is_top_level {
            if let Some(id_value) = dict.get_item("_id")? {
                write_element(buf, "_id", &id_value, check_keys, codec_options)?;
                id_written = true;
            }
        }

        // Second pass: write all other fields
        for (key, value) in dict {
            let key_str: String = key.extract()?;

            // Skip _id if we already wrote it
            if is_top_level && id_written && key_str == "_id" {
                continue;
            }

            // Validate key
            validate_key(&key_str, check_keys)?;

            write_element(buf, &key_str, &value, check_keys, codec_options)?;
        }
    } else {
        // SLOW PATH: Use items() method for SON, OrderedDict, etc.
        if let Ok(items_method) = obj.getattr("items") {
            if let Ok(items_result) = items_method.call0() {
                // Collect items into a vector
                let items: Vec<(String, Bound<'_, PyAny>)> = if let Ok(items_list) = items_result.downcast::<pyo3::types::PyList>() {
                    items_list.iter()
                        .map(|item| {
                            let tuple = item.downcast::<pyo3::types::PyTuple>()?;
                            let key: String = tuple.get_item(0)?.extract()?;
                            let value = tuple.get_item(1)?;
                            Ok((key, value))
                        })
                        .collect::<PyResult<Vec<_>>>()?
                } else {
                    return Err(PyTypeError::new_err("items() must return a list"));
                };

                // First pass: write _id if present at top level
                if is_top_level {
                    for (key, value) in &items {
                        if key == "_id" {
                            write_element(buf, "_id", value, check_keys, codec_options)?;
                            id_written = true;
                            break;
                        }
                    }
                }

                // Second pass: write all other fields
                for (key, value) in items {
                    // Skip _id if we already wrote it
                    if is_top_level && id_written && key == "_id" {
                        continue;
                    }

                    // Validate key
                    validate_key(&key, check_keys)?;

                    write_element(buf, &key, &value, check_keys, codec_options)?;
                }
            } else {
                return Err(PyTypeError::new_err("items() call failed"));
            }
        } else {
            return Err(PyTypeError::new_err(format!("encoder expected a mapping type but got: {}", obj)));
        }
    }

    // Write null terminator
    buf.push(0);

    // Write document size at the beginning
    let doc_size = (buf.len() - size_pos) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&doc_size.to_le_bytes());

    Ok(())
}

fn write_document_bytes_direct(
    buf: &mut Vec<u8>,
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
    is_top_level: bool,
) -> PyResult<()> {
    // Reserve space for document size (will be filled in at the end)
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]);

    // Handle _id field first if this is top-level
    let mut id_written = false;

    // FAST PATH: Check if it's a PyDict first (most common case)
    if let Ok(dict) = obj.downcast::<PyDict>() {
        // First pass: write _id if present at top level
        if is_top_level {
            if let Some(id_value) = dict.get_item("_id")? {
                write_element_direct(buf, "_id", &id_value, check_keys, codec_options)?;
                id_written = true;
            }
        }

        // Second pass: write all other fields
        for (key, value) in dict {
            let key_str: String = key.extract()?;

            // Skip _id if we already wrote it
            if is_top_level && id_written && key_str == "_id" {
                continue;
            }

            // Validate key
            validate_key(&key_str, check_keys)?;

            write_element_direct(buf, &key_str, &value, check_keys, codec_options)?;
        }
    } else {
        // SLOW PATH: Use items() method for SON, OrderedDict, etc.
        if let Ok(items_method) = obj.getattr("items") {
            if let Ok(items_result) = items_method.call0() {
                // Collect items into a vector
                let items: Vec<(String, Bound<'_, PyAny>)> = if let Ok(items_list) = items_result.downcast::<pyo3::types::PyList>() {
                    items_list.iter()
                        .map(|item| {
                            let tuple = item.downcast::<pyo3::types::PyTuple>()?;
                            let key: String = tuple.get_item(0)?.extract()?;
                            let value = tuple.get_item(1)?;
                            Ok((key, value))
                        })
                        .collect::<PyResult<Vec<_>>>()?
                } else {
                    return Err(PyTypeError::new_err("items() must return a list"));
                };

                // First pass: write _id if present at top level
                if is_top_level {
                    for (key, value) in &items {
                        if key == "_id" {
                            write_element_direct(buf, "_id", value, check_keys, codec_options)?;
                            id_written = true;
                            break;
                        }
                    }
                }

                // Second pass: write all other fields
                for (key, value) in items {
                    // Skip _id if we already wrote it
                    if is_top_level && id_written && key == "_id" {
                        continue;
                    }

                    // Validate key
                    validate_key(&key, check_keys)?;

                    write_element_direct(buf, &key, &value, check_keys, codec_options)?;
                }
            } else {
                return Err(PyTypeError::new_err("items() call failed"));
            }
        } else {
            return Err(PyTypeError::new_err(format!("encoder expected a mapping type but got: {}", obj)));
        }
    }

    // Write null terminator
    buf.push(0);

    // Write document size at the beginning
    let doc_size = (buf.len() - size_pos) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&doc_size.to_le_bytes());

    Ok(())
}

fn write_element(
    buf: &mut Vec<u8>,
    key: &str,
    value: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    use pyo3::types::{PyList, PyLong, PyTuple};
    use std::io::Write;

    // FAST PATH: Check for common Python types FIRST
    if value.is_none() {
        // Type 0x0A: Null
        buf.push(0x0A);
        write_cstring(buf, key);
        return Ok(());
    } else if let Ok(v) = value.extract::<bool>() {
        // Type 0x08: Boolean
        buf.push(0x08);
        write_cstring(buf, key);
        buf.push(if v { 1 } else { 0 });
        return Ok(());
    } else if value.is_instance_of::<PyLong>() {
        // Try i32 first, then i64
        if let Ok(v) = value.extract::<i32>() {
            // Type 0x10: Int32
            buf.push(0x10);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
            return Ok(());
        } else if let Ok(v) = value.extract::<i64>() {
            // Type 0x12: Int64
            buf.push(0x12);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
            return Ok(());
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
                "MongoDB can only handle up to 8-byte ints"
            ));
        }
    } else if let Ok(v) = value.extract::<f64>() {
        // Type 0x01: Double
        buf.push(0x01);
        write_cstring(buf, key);
        buf.extend_from_slice(&v.to_le_bytes());
        return Ok(());
    } else if let Ok(v) = value.extract::<String>() {
        // Type 0x02: String
        buf.push(0x02);
        write_cstring(buf, key);
        write_string(buf, &v);
        return Ok(());
    }

    // Check for dict/list BEFORE converting to Bson (much faster for nested structures)
    if let Ok(dict) = value.downcast::<PyDict>() {
        // Type 0x03: Embedded document
        buf.push(0x03);
        write_cstring(buf, key);
        write_document_bytes(buf, value, check_keys, codec_options, false)?;
        return Ok(());
    } else if let Ok(list) = value.downcast::<PyList>() {
        // Type 0x04: Array
        buf.push(0x04);
        write_cstring(buf, key);
        write_array_bytes(buf, list, check_keys, codec_options)?;
        return Ok(());
    } else if let Ok(tuple) = value.downcast::<PyTuple>() {
        // Type 0x04: Array (tuples are treated as arrays)
        buf.push(0x04);
        write_cstring(buf, key);
        write_tuple_bytes(buf, tuple, check_keys, codec_options)?;
        return Ok(());
    } else if value.hasattr("items")? {
        // Type 0x03: Embedded document (SON, OrderedDict, etc.)
        buf.push(0x03);
        write_cstring(buf, key);
        write_document_bytes(buf, value, check_keys, codec_options, false)?;
        return Ok(());
    }

    // SLOW PATH: Handle BSON types and other Python types
    // Convert to Bson and then write
    let bson_value = python_to_bson(value.clone(), check_keys, codec_options)?;
    write_bson_value(buf, key, &bson_value)?;

    Ok(())
}

fn write_element_direct(
    buf: &mut Vec<u8>,
    key: &str,
    value: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    use pyo3::types::{PyList, PyLong, PyTuple};
    let py = value.py();

    // FAST PATH: Check for common Python types FIRST
    if value.is_none() {
        // Type 0x0A: Null
        buf.push(0x0A);
        write_cstring(buf, key);
        return Ok(());
    } else if let Ok(v) = value.extract::<bool>() {
        // Type 0x08: Boolean
        buf.push(0x08);
        write_cstring(buf, key);
        buf.push(if v { 1 } else { 0 });
        return Ok(());
    } else if value.is_instance_of::<PyLong>() {
        // Try i32 first, then i64
        if let Ok(v) = value.extract::<i32>() {
            // Type 0x10: Int32
            buf.push(0x10);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
            return Ok(());
        } else if let Ok(v) = value.extract::<i64>() {
            // Type 0x12: Int64
            buf.push(0x12);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
            return Ok(());
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
                "MongoDB can only handle up to 8-byte ints"
            ));
        }
    } else if let Ok(v) = value.extract::<f64>() {
        // Type 0x01: Double
        buf.push(0x01);
        write_cstring(buf, key);
        buf.extend_from_slice(&v.to_le_bytes());
        return Ok(());
    } else if let Ok(v) = value.extract::<String>() {
        // Type 0x02: String
        buf.push(0x02);
        write_cstring(buf, key);
        write_string(buf, &v);
        return Ok(());
    }

    // Check for dict/list BEFORE checking BSON types
    if let Ok(dict) = value.downcast::<PyDict>() {
        // Type 0x03: Embedded document
        buf.push(0x03);
        write_cstring(buf, key);
        write_document_bytes_direct(buf, value, check_keys, codec_options, false)?;
        return Ok(());
    } else if let Ok(list) = value.downcast::<PyList>() {
        // Type 0x04: Array
        buf.push(0x04);
        write_cstring(buf, key);
        write_array_bytes_direct(buf, list, check_keys, codec_options)?;
        return Ok(());
    } else if let Ok(tuple) = value.downcast::<PyTuple>() {
        // Type 0x04: Array (tuples are treated as arrays)
        buf.push(0x04);
        write_cstring(buf, key);
        write_tuple_bytes_direct(buf, tuple, check_keys, codec_options)?;
        return Ok(());
    }

    // Check for BSON types with _type_marker and write directly
    if let Ok(type_marker) = value.getattr("_type_marker") {
        if let Ok(marker) = type_marker.extract::<i32>() {
            return write_bson_type_direct(buf, key, value, marker, check_keys, codec_options);
        }
    }

    // Check for bytes (Python bytes type)
    if let Ok(bytes_data) = value.extract::<Vec<u8>>() {
        // Type 0x05: Binary (subtype 0 for generic binary)
        buf.push(0x05);
        write_cstring(buf, key);
        buf.extend_from_slice(&(bytes_data.len() as i32).to_le_bytes());
        buf.push(0); // subtype 0
        buf.extend_from_slice(&bytes_data);
        return Ok(());
    }

    // Check for mapping types (SON, OrderedDict, etc.)
    if value.hasattr("items")? {
        // Type 0x03: Embedded document
        buf.push(0x03);
        write_cstring(buf, key);
        write_document_bytes_direct(buf, value, check_keys, codec_options, false)?;
        return Ok(());
    }

    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        format!("cannot encode object: {:?}", value)
    ))
}

fn write_bson_type_direct(
    buf: &mut Vec<u8>,
    key: &str,
    value: &Bound<'_, PyAny>,
    marker: i32,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    match marker {
        BINARY_TYPE_MARKER => {
            // Type 0x05: Binary
            let subtype: u8 = value.getattr("subtype")?.extract()?;
            let bytes_data: Vec<u8> = value.extract()?;
            buf.push(0x05);
            write_cstring(buf, key);
            buf.extend_from_slice(&(bytes_data.len() as i32).to_le_bytes());
            buf.push(subtype);
            buf.extend_from_slice(&bytes_data);
            Ok(())
        }
        OBJECTID_TYPE_MARKER => {
            // Type 0x07: ObjectId
            let binary: Vec<u8> = value.getattr("binary")?.extract()?;
            if binary.len() != 12 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "ObjectId must be 12 bytes"
                ));
            }
            buf.push(0x07);
            write_cstring(buf, key);
            buf.extend_from_slice(&binary);
            Ok(())
        }
        DATETIME_TYPE_MARKER => {
            // Type 0x09: DateTime (UTC datetime as milliseconds since epoch)
            let millis: i64 = value.getattr("_value")?.extract()?;
            buf.push(0x09);
            write_cstring(buf, key);
            buf.extend_from_slice(&millis.to_le_bytes());
            Ok(())
        }
        REGEX_TYPE_MARKER => {
            // Type 0x0B: Regular expression
            let pattern_obj = value.getattr("pattern")?;
            let pattern: String = if let Ok(s) = pattern_obj.extract::<String>() {
                s
            } else if let Ok(b) = pattern_obj.extract::<Vec<u8>>() {
                String::from_utf8_lossy(&b).to_string()
            } else {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Regex pattern must be str or bytes"
                ));
            };

            let flags_obj = value.getattr("flags")?;
            let flags_str = if let Ok(flags_int) = flags_obj.extract::<i32>() {
                int_flags_to_str(flags_int)
            } else {
                flags_obj.extract::<String>().unwrap_or_default()
            };

            buf.push(0x0B);
            write_cstring(buf, key);
            write_cstring(buf, &pattern);
            write_cstring(buf, &flags_str);
            Ok(())
        }
        CODE_TYPE_MARKER => {
            // Type 0x0D: JavaScript code or 0x0F: JavaScript code with scope
            let code_str: String = value.extract()?;

            if let Ok(scope_obj) = value.getattr("scope") {
                if !scope_obj.is_none() {
                    // Type 0x0F: Code with scope
                    buf.push(0x0F);
                    write_cstring(buf, key);

                    // Reserve space for total size
                    let size_pos = buf.len();
                    buf.extend_from_slice(&[0u8; 4]);

                    // Write code string
                    write_string(buf, &code_str);

                    // Write scope document
                    write_document_bytes_direct(buf, &scope_obj, check_keys, codec_options, false)?;

                    // Write total size
                    let total_size = (buf.len() - size_pos) as i32;
                    buf[size_pos..size_pos + 4].copy_from_slice(&total_size.to_le_bytes());

                    return Ok(());
                }
            }

            // Type 0x0D: Code without scope
            buf.push(0x0D);
            write_cstring(buf, key);
            write_string(buf, &code_str);
            Ok(())
        }
        TIMESTAMP_TYPE_MARKER => {
            // Type 0x11: Timestamp
            let time: u32 = value.getattr("time")?.extract()?;
            let inc: u32 = value.getattr("inc")?.extract()?;
            buf.push(0x11);
            write_cstring(buf, key);
            buf.extend_from_slice(&inc.to_le_bytes());
            buf.extend_from_slice(&time.to_le_bytes());
            Ok(())
        }
        INT64_TYPE_MARKER => {
            // Type 0x12: Int64
            let val: i64 = value.extract()?;
            buf.push(0x12);
            write_cstring(buf, key);
            buf.extend_from_slice(&val.to_le_bytes());
            Ok(())
        }
        DECIMAL128_TYPE_MARKER => {
            // Type 0x13: Decimal128
            let bid: Vec<u8> = value.getattr("bid")?.extract()?;
            if bid.len() != 16 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "Decimal128 must be 16 bytes"
                ));
            }
            buf.push(0x13);
            write_cstring(buf, key);
            buf.extend_from_slice(&bid);
            Ok(())
        }
        MAXKEY_TYPE_MARKER => {
            // Type 0x7F: MaxKey
            buf.push(0x7F);
            write_cstring(buf, key);
            Ok(())
        }
        MINKEY_TYPE_MARKER => {
            // Type 0xFF: MinKey
            buf.push(0xFF);
            write_cstring(buf, key);
            Ok(())
        }
        _ => {
            Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                format!("Unknown BSON type marker: {}", marker)
            ))
        }
    }
}


fn write_array_bytes(
    buf: &mut Vec<u8>,
    list: &Bound<'_, pyo3::types::PyList>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    // Arrays are encoded as documents with numeric string keys ("0", "1", "2", ...)
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // Reserve space for size

    for (i, item) in list.iter().enumerate() {
        write_element(buf, &i.to_string(), &item, check_keys, codec_options)?;
    }

    buf.push(0); // null terminator

    let arr_size = (buf.len() - size_pos) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&arr_size.to_le_bytes());

    Ok(())
}

fn write_tuple_bytes(
    buf: &mut Vec<u8>,
    tuple: &Bound<'_, pyo3::types::PyTuple>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    // Arrays are encoded as documents with numeric string keys ("0", "1", "2", ...)
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // Reserve space for size

    for (i, item) in tuple.iter().enumerate() {
        write_element(buf, &i.to_string(), &item, check_keys, codec_options)?;
    }

    buf.push(0); // null terminator

    let arr_size = (buf.len() - size_pos) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&arr_size.to_le_bytes());

    Ok(())
}

fn write_array_bytes_direct(
    buf: &mut Vec<u8>,
    list: &Bound<'_, pyo3::types::PyList>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    // Arrays are encoded as documents with numeric string keys ("0", "1", "2", ...)
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // Reserve space for size

    for (i, item) in list.iter().enumerate() {
        write_element_direct(buf, &i.to_string(), &item, check_keys, codec_options)?;
    }

    buf.push(0); // null terminator

    let arr_size = (buf.len() - size_pos) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&arr_size.to_le_bytes());

    Ok(())
}

fn write_tuple_bytes_direct(
    buf: &mut Vec<u8>,
    tuple: &Bound<'_, pyo3::types::PyTuple>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    // Arrays are encoded as documents with numeric string keys ("0", "1", "2", ...)
    let size_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // Reserve space for size

    for (i, item) in tuple.iter().enumerate() {
        write_element_direct(buf, &i.to_string(), &item, check_keys, codec_options)?;
    }

    buf.push(0); // null terminator

    let arr_size = (buf.len() - size_pos) as i32;
    buf[size_pos..size_pos + 4].copy_from_slice(&arr_size.to_le_bytes());

    Ok(())
}

fn write_bson_value(buf: &mut Vec<u8>, key: &str, value: &Bson) -> PyResult<()> {
    use std::io::Write;

    match value {
        Bson::Double(v) => {
            buf.push(0x01);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Bson::String(v) => {
            buf.push(0x02);
            write_cstring(buf, key);
            write_string(buf, v);
        }
        Bson::Document(doc) => {
            buf.push(0x03);
            write_cstring(buf, key);
            // Serialize the document
            let mut doc_buf = Vec::new();
            doc.to_writer(&mut doc_buf)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Failed to encode nested document: {}", e)
                ))?;
            buf.extend_from_slice(&doc_buf);
        }
        Bson::Array(arr) => {
            buf.push(0x04);
            write_cstring(buf, key);
            // Arrays are encoded as documents with numeric string keys
            let size_pos = buf.len();
            buf.extend_from_slice(&[0u8; 4]);

            for (i, item) in arr.iter().enumerate() {
                write_bson_value(buf, &i.to_string(), item)?;
            }

            buf.push(0); // null terminator

            let arr_size = (buf.len() - size_pos) as i32;
            buf[size_pos..size_pos + 4].copy_from_slice(&arr_size.to_le_bytes());
        }
        Bson::Binary(bin) => {
            buf.push(0x05);
            write_cstring(buf, key);
            buf.extend_from_slice(&(bin.bytes.len() as i32).to_le_bytes());
            buf.push(bin.subtype.into());
            buf.extend_from_slice(&bin.bytes);
        }
        Bson::ObjectId(oid) => {
            buf.push(0x07);
            write_cstring(buf, key);
            buf.extend_from_slice(&oid.bytes());
        }
        Bson::Boolean(v) => {
            buf.push(0x08);
            write_cstring(buf, key);
            buf.push(if *v { 1 } else { 0 });
        }
        Bson::DateTime(dt) => {
            buf.push(0x09);
            write_cstring(buf, key);
            buf.extend_from_slice(&dt.timestamp_millis().to_le_bytes());
        }
        Bson::Null => {
            buf.push(0x0A);
            write_cstring(buf, key);
        }
        Bson::RegularExpression(regex) => {
            buf.push(0x0B);
            write_cstring(buf, key);
            write_cstring(buf, &regex.pattern);
            write_cstring(buf, &regex.options);
        }
        Bson::Int32(v) => {
            buf.push(0x10);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Bson::Timestamp(ts) => {
            buf.push(0x11);
            write_cstring(buf, key);
            buf.extend_from_slice(&ts.time.to_le_bytes());
            buf.extend_from_slice(&ts.increment.to_le_bytes());
        }
        Bson::Int64(v) => {
            buf.push(0x12);
            write_cstring(buf, key);
            buf.extend_from_slice(&v.to_le_bytes());
        }
        Bson::Decimal128(dec) => {
            buf.push(0x13);
            write_cstring(buf, key);
            buf.extend_from_slice(&dec.bytes());
        }
        _ => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Unsupported BSON type: {:?}", value)
            ));
        }
    }

    Ok(())
}

/// Encode a Python dictionary to BSON bytes

fn python_to_bson(
    obj: Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bson> {
    let py = obj.py();

    // Check if this is a BSON type with a _type_marker FIRST
    // This must come before string/int checks because Code inherits from str, Int64 inherits from int, etc.
    if let Ok(type_marker) = obj.getattr("_type_marker") {
        if let Ok(marker) = type_marker.extract::<i32>() {
            return handle_bson_type_marker(obj, marker, check_keys, codec_options);
        }
    }

    // FAST PATH: Check for common Python types (int, str, float, bool, None)
    // This avoids expensive module/attribute lookups for the majority of values
    use pyo3::types::PyLong;

    if obj.is_none() {
        return Ok(Bson::Null);
    } else if let Ok(v) = obj.extract::<bool>() {
        return Ok(Bson::Boolean(v));
    } else if obj.is_instance_of::<PyLong>() {
        // It's a Python int - try to fit it in i32 or i64
        if let Ok(v) = obj.extract::<i32>() {
            return Ok(Bson::Int32(v));
        } else if let Ok(v) = obj.extract::<i64>() {
            return Ok(Bson::Int64(v));
        } else {
            // Integer doesn't fit in i64 - raise OverflowError
            return Err(PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
                "MongoDB can only handle up to 8-byte ints"
            ));
        }
    } else if let Ok(v) = obj.extract::<f64>() {
        return Ok(Bson::Double(v));
    } else if let Ok(v) = obj.extract::<String>() {
        return Ok(Bson::String(v));
    }

    // Check for Python UUID objects (uuid.UUID) - use cached type
    if let Ok(uuid_class) = TYPE_CACHE.get_uuid_class(py) {
        if obj.is_instance(&uuid_class.bind(py))? {
            // Check uuid_representation from codec_options
            let uuid_representation = if let Some(opts) = codec_options {
                if let Ok(uuid_rep) = opts.getattr("uuid_representation") {
                    uuid_rep.extract::<i32>().unwrap_or(0)
                } else {
                    0
                }
            } else {
                0
            };

            // UNSPECIFIED = 0, cannot encode native UUID
            if uuid_representation == 0 {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    "cannot encode native uuid.UUID with UuidRepresentation.UNSPECIFIED. \
                     UUIDs can be manually converted to bson.Binary instances using \
                     bson.Binary.from_uuid() or a different UuidRepresentation can be \
                     configured. See the documentation for UuidRepresentation for more information."
                ));
            }

            // Convert UUID to Binary with appropriate subtype based on representation
            // UNSPECIFIED = 0, PYTHON_LEGACY = 3, STANDARD = 4, JAVA_LEGACY = 5, CSHARP_LEGACY = 6
            let uuid_bytes: Vec<u8> = obj.getattr("bytes")?.extract()?;
            let subtype = match uuid_representation {
                3 => bson::spec::BinarySubtype::UuidOld,  // PYTHON_LEGACY (subtype 3)
                4 => bson::spec::BinarySubtype::Uuid,     // STANDARD (subtype 4)
                5 => bson::spec::BinarySubtype::UuidOld,  // JAVA_LEGACY (subtype 3)
                6 => bson::spec::BinarySubtype::UuidOld,  // CSHARP_LEGACY (subtype 3)
                _ => bson::spec::BinarySubtype::Uuid,     // Default to STANDARD
            };

            return Ok(Bson::Binary(bson::Binary {
                subtype,
                bytes: uuid_bytes,
            }));
        }
    }

    // Check for compiled regex Pattern objects - use cached type
    if let Ok(pattern_class) = TYPE_CACHE.get_pattern_class(py) {
        if obj.is_instance(&pattern_class.bind(py))? {
            // Extract pattern and flags from re.Pattern
            if obj.hasattr("pattern")? && obj.hasattr("flags")? {
                let pattern_obj = obj.getattr("pattern")?;
                let pattern: String = if let Ok(s) = pattern_obj.extract::<String>() {
                    s
                } else if let Ok(b) = pattern_obj.extract::<Vec<u8>>() {
                    // Pattern is bytes, convert to string
                    String::from_utf8_lossy(&b).to_string()
                } else {
                    return Err(invalid_document_error(py,
                        "Invalid document: Regex pattern must be str or bytes".to_string()));
                };
                let flags: i32 = obj.getattr("flags")?.extract()?;
                let flags_str = int_flags_to_str(flags);
                return Ok(Bson::RegularExpression(bson::Regex {
                    pattern,
                    options: flags_str,
                }));
            }
        }
    }

    // Check for Python datetime objects - use cached type
    if let Ok(datetime_class) = TYPE_CACHE.get_datetime_class(py) {
        if obj.is_instance(&datetime_class.bind(py))? {
            // Convert Python datetime to milliseconds since epoch (inline)
            let millis = datetime_to_millis(py, &obj)?;
            return Ok(Bson::DateTime(bson::DateTime::from_millis(millis)));
        }
    }

    // Handle remaining Python types (bytes, lists, dicts)
    handle_remaining_python_types(obj, check_keys, codec_options)
}


fn python_mapping_to_bson_doc(
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
    is_top_level: bool,
) -> PyResult<Document> {
    let mut doc = Document::new();
    let mut has_id = false;
    let mut id_value: Option<Bson> = None;

    // FAST PATH: Check if it's a PyDict first (most common case)
    // Iterate directly over dict items - much faster than calling items()
    if let Ok(dict) = obj.downcast::<PyDict>() {
        for (key, value) in dict {
            // Check if key is bytes - this is not allowed
            if key.extract::<Vec<u8>>().is_ok() {
                let py = obj.py();
                let key_repr = key.repr()?.to_string();
                return Err(invalid_document_error(py,
                    format!("documents must have only string keys, key was {}", key_repr)));
            }

            // Extract key as string
            let key_str: String = if let Ok(s) = key.extract::<String>() {
                s
            } else {
                let py = obj.py();
                return Err(invalid_document_error(py,
                    format!("Dictionary keys must be strings, got {}",
                        key.get_type().name()?)));
            };

            // Check keys if requested
            if check_keys {
                if key_str.starts_with('$') {
                    let py = obj.py();
                    return Err(invalid_document_error(py,
                        format!("key '{}' must not start with '$'", key_str)));
                }
                if key_str.contains('.') {
                    let py = obj.py();
                    return Err(invalid_document_error(py,
                        format!("key '{}' must not contain '.'", key_str)));
                }
            }

            let bson_value = python_to_bson(value, check_keys, codec_options)?;

            // Handle _id field ordering
            if key_str == "_id" {
                has_id = true;
                id_value = Some(bson_value);
            } else {
                doc.insert(key_str, bson_value);
            }
        }

        // Insert _id first if present and at top level
        if has_id {
            if let Some(id_val) = id_value {
                if is_top_level {
                    // At top level, move _id to the front
                    let mut new_doc = Document::new();
                    new_doc.insert("_id", id_val);
                    for (k, v) in doc {
                        new_doc.insert(k, v);
                    }
                    return Ok(new_doc);
                } else {
                    // Not at top level, just insert _id in normal position
                    doc.insert("_id", id_val);
                }
            }
        }

        return Ok(doc);
    }

    // SLOW PATH: Fall back to mapping protocol for SON, OrderedDict, etc.
    // Use items() method for efficient iteration
    if let Ok(items_method) = obj.getattr("items") {
        if let Ok(items_result) = items_method.call0() {
            // Try to downcast to PyList or PyTuple first for efficient iteration
            if let Ok(items_list) = items_result.downcast::<pyo3::types::PyList>() {
                for item in items_list {
                    process_mapping_item(
                        &item,
                        &mut doc,
                        &mut has_id,
                        &mut id_value,
                        check_keys,
                        codec_options,
                    )?;
                }
            } else if let Ok(items_tuple) = items_result.downcast::<pyo3::types::PyTuple>() {
                for item in items_tuple {
                    process_mapping_item(
                        &item,
                        &mut doc,
                        &mut has_id,
                        &mut id_value,
                        check_keys,
                        codec_options,
                    )?;
                }
            } else {
                // Fall back to generic iteration using PyIterator
                let py = obj.py();
                let iter = items_result.call_method0("__iter__")?;
                loop {
                    match iter.call_method0("__next__") {
                        Ok(item) => {
                            process_mapping_item(
                                &item,
                                &mut doc,
                                &mut has_id,
                                &mut id_value,
                                check_keys,
                                codec_options,
                            )?;
                        }
                        Err(e) => {
                            // Check if it's StopIteration
                            if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                                break;
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
            }

            // Insert _id first if present and at top level
            if has_id {
                if let Some(id_val) = id_value {
                    if is_top_level {
                        // At top level, move _id to the front
                        let mut new_doc = Document::new();
                        new_doc.insert("_id", id_val);
                        for (k, v) in doc {
                            new_doc.insert(k, v);
                        }
                        return Ok(new_doc);
                    } else {
                        // Not at top level, just insert _id in normal position
                        doc.insert("_id", id_val);
                    }
                }
            }

            return Ok(doc);
        }
    }

    // Match C extension behavior: raise TypeError for non-mapping types
    Err(PyTypeError::new_err(format!("encoder expected a mapping type but got: {}", obj)))
}

/// Extract a single item from a PyDict and return (key, value)

fn process_mapping_item(
    item: &Bound<'_, PyAny>,
    doc: &mut Document,
    has_id: &mut bool,
    id_value: &mut Option<Bson>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<()> {
    // Each item should be a tuple (key, value)
    // Use extract to get a tuple of (PyObject, PyObject)
    let (key, value): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item.extract()?;

    // Check if key is bytes - this is not allowed
    if key.extract::<Vec<u8>>().is_ok() {
        let py = item.py();
        let key_repr = key.repr()?.to_string();
        return Err(invalid_document_error(py,
            format!("documents must have only string keys, key was {}", key_repr)));
    }

    // Convert key to string
    let key_str: String = if let Ok(s) = key.extract::<String>() {
        s
    } else {
        let py = item.py();
        return Err(invalid_document_error(py,
            format!("Dictionary keys must be strings, got {}",
                key.get_type().name()?)));
    };

    // Check keys if requested
    if check_keys {
        if key_str.starts_with('$') {
            let py = item.py();
            return Err(invalid_document_error(py,
                format!("key '{}' must not start with '$'", key_str)));
        }
        if key_str.contains('.') {
            let py = item.py();
            return Err(invalid_document_error(py,
                format!("key '{}' must not contain '.'", key_str)));
        }
    }

    let bson_value = python_to_bson(value, check_keys, codec_options)?;

    // Always store _id field, but it will be reordered at top level only
    if key_str == "_id" {
        *has_id = true;
        *id_value = Some(bson_value);
    } else {
        doc.insert(key_str, bson_value);
    }

    Ok(())
}

/// Convert a Python mapping (dict, SON, OrderedDict, etc.) to a BSON Document
/// HYBRID APPROACH: Fast path for PyDict, items() method for other mappings

fn extract_dict_item(
    key: &Bound<'_, PyAny>,
    value: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Bson)> {
    let py = key.py();

    // Keys must be strings (not bytes, not other types)
    let key_str: String = if let Ok(s) = key.extract::<String>() {
        s
    } else {
        // Get a string representation of the key for the error message
        let key_repr = if let Ok(b) = key.extract::<Vec<u8>>() {
            format!("b'{}'", String::from_utf8_lossy(&b))
        } else {
            format!("{}", key)
        };
        return Err(invalid_document_error(py, format!(
            "Invalid document: documents must have only string keys, key was {}",
            key_repr
        )));
    };

    // Check for null bytes in key (always invalid)
    if key_str.contains('\0') {
        return Err(invalid_document_error(py, format!(
            "Invalid document: Key names must not contain the NULL byte"
        )));
    }

    // Check keys if requested (but not for _id)
    if check_keys && key_str != "_id" {
        if key_str.starts_with('$') {
            return Err(invalid_document_error(py, format!(
                "Invalid document: key '{}' must not start with '$'",
                key_str
            )));
        }
        if key_str.contains('.') {
            return Err(invalid_document_error(py, format!(
                "Invalid document: key '{}' must not contain '.'",
                key_str
            )));
        }
    }

    let bson_value = python_to_bson(value.clone(), check_keys, codec_options)?;

    Ok((key_str, bson_value))
}


fn extract_mapping_item(
    item: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Bson)> {
    // Each item should be a tuple (key, value)
    let (key, value): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item.extract()?;

    // Keys must be strings (not bytes, not other types)
    let py = item.py();
    let key_str: String = if let Ok(s) = key.extract::<String>() {
        s
    } else {
        // Get a string representation of the key for the error message
        let key_repr = if let Ok(b) = key.extract::<Vec<u8>>() {
            format!("b'{}'", String::from_utf8_lossy(&b))
        } else {
            format!("{}", key)
        };
        return Err(invalid_document_error(py, format!(
            "Invalid document: documents must have only string keys, key was {}",
            key_repr
        )));
    };

    // Check for null bytes in key (always invalid)
    if key_str.contains('\0') {
        return Err(invalid_document_error(py, format!(
            "Invalid document: Key names must not contain the NULL byte"
        )));
    }

    // Check keys if requested (but not for _id)
    if check_keys && key_str != "_id" {
        if key_str.starts_with('$') {
            return Err(invalid_document_error(py, format!(
                "Invalid document: key '{}' must not start with '$'",
                key_str
            )));
        }
        if key_str.contains('.') {
            return Err(invalid_document_error(py, format!(
                "Invalid document: key '{}' must not contain '.'",
                key_str
            )));
        }
    }

    let bson_value = python_to_bson(value, check_keys, codec_options)?;

    Ok((key_str, bson_value))
}


fn handle_bson_type_marker(
    obj: Bound<'_, PyAny>,
    marker: i32,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bson> {
    match marker {
        BINARY_TYPE_MARKER => {
            // Binary object
            let subtype: u8 = obj.getattr("subtype")?.extract()?;
            let bytes: Vec<u8> = obj.extract()?;

            let bson_subtype = match subtype {
                0 => bson::spec::BinarySubtype::Generic,
                1 => bson::spec::BinarySubtype::Function,
                2 => bson::spec::BinarySubtype::BinaryOld,
                3 => bson::spec::BinarySubtype::UuidOld,
                4 => bson::spec::BinarySubtype::Uuid,
                5 => bson::spec::BinarySubtype::Md5,
                6 => bson::spec::BinarySubtype::Encrypted,
                7 => bson::spec::BinarySubtype::Column,
                8 => bson::spec::BinarySubtype::Sensitive,
                9 => bson::spec::BinarySubtype::Vector,
                10..=127 => bson::spec::BinarySubtype::Reserved(subtype),
                128..=255 => bson::spec::BinarySubtype::UserDefined(subtype),
            };

            Ok(Bson::Binary(bson::Binary {
                subtype: bson_subtype,
                bytes,
            }))
        }
        OBJECTID_TYPE_MARKER => {
            // ObjectId object - get the binary representation
            let binary: Vec<u8> = obj.getattr("binary")?.extract()?;
            if binary.len() != 12 {
                return Err(invalid_document_error(obj.py(), "Invalid document: ObjectId must be 12 bytes".to_string()));
            }
            let mut oid_bytes = [0u8; 12];
            oid_bytes.copy_from_slice(&binary);
            Ok(Bson::ObjectId(bson::oid::ObjectId::from_bytes(oid_bytes)))
        }
        DATETIME_TYPE_MARKER => {
            // DateTime/DatetimeMS object - get milliseconds since epoch
            if let Ok(value) = obj.getattr("_value") {
                // Check that __int__() returns an actual integer, not a float
                if let Ok(int_result) = obj.call_method0("__int__") {
                    // Check if the result is a float (which would be invalid)
                    if int_result.is_instance_of::<PyFloat>() {
                        return Err(PyTypeError::new_err(
                            "DatetimeMS.__int__() must return an integer, not float"
                        ));
                    }
                }

                let millis: i64 = value.extract()?;
                Ok(Bson::DateTime(bson::DateTime::from_millis(millis)))
            } else {
                Err(invalid_document_error(obj.py(),
                    "Invalid document: DateTime object must have _value attribute".to_string(),
                ))
            }
        }
        REGEX_TYPE_MARKER => {
            // Regex object - pattern can be str or bytes
            let pattern_obj = obj.getattr("pattern")?;
            let pattern: String = if let Ok(s) = pattern_obj.extract::<String>() {
                s
            } else if let Ok(b) = pattern_obj.extract::<Vec<u8>>() {
                // Pattern is bytes, convert to string (lossy for non-UTF8)
                String::from_utf8_lossy(&b).to_string()
            } else {
                return Err(invalid_document_error(obj.py(),
                    "Invalid document: Regex pattern must be str or bytes".to_string()));
            };

            let flags_obj = obj.getattr("flags")?;

            // Flags can be an int or a string
            let flags_str = if let Ok(flags_int) = flags_obj.extract::<i32>() {
                int_flags_to_str(flags_int)
            } else {
                flags_obj.extract::<String>().unwrap_or_default()
            };

            Ok(Bson::RegularExpression(bson::Regex {
                pattern,
                options: flags_str,
            }))
        }
        CODE_TYPE_MARKER => {
            // Code object - inherits from str
            let code_str: String = obj.extract()?;

            // Check if there's a scope
            if let Ok(scope_obj) = obj.getattr("scope") {
                if !scope_obj.is_none() {
                    // Code with scope
                    let scope_doc = python_mapping_to_bson_doc(&scope_obj, check_keys, codec_options, false)?;
                    return Ok(Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                        code: code_str,
                        scope: scope_doc,
                    }));
                }
            }

            // Code without scope
            Ok(Bson::JavaScriptCode(code_str))
        }
        TIMESTAMP_TYPE_MARKER => {
            // Timestamp object
            let time: u32 = obj.getattr("time")?.extract()?;
            let inc: u32 = obj.getattr("inc")?.extract()?;
            Ok(Bson::Timestamp(bson::Timestamp {
                time,
                increment: inc,
            }))
        }
        INT64_TYPE_MARKER => {
            // Int64 object - extract the value and encode as BSON Int64
            let value: i64 = obj.extract()?;
            Ok(Bson::Int64(value))
        }
        DECIMAL128_TYPE_MARKER => {
            // Decimal128 object
            let bid: Vec<u8> = obj.getattr("bid")?.extract()?;
            if bid.len() != 16 {
                return Err(invalid_document_error(obj.py(), "Invalid document: Decimal128 must be 16 bytes".to_string()));
            }
            let mut bytes = [0u8; 16];
            bytes.copy_from_slice(&bid);
            Ok(Bson::Decimal128(bson::Decimal128::from_bytes(bytes)))
        }
        MAXKEY_TYPE_MARKER => {
            Ok(Bson::MaxKey)
        }
        MINKEY_TYPE_MARKER => {
            Ok(Bson::MinKey)
        }
        DBREF_TYPE_MARKER => {
            // DBRef object - use as_doc() method
            if let Ok(as_doc_method) = obj.getattr("as_doc") {
                if let Ok(doc_obj) = as_doc_method.call0() {
                    let dbref_doc = python_mapping_to_bson_doc(&doc_obj, check_keys, codec_options, false)?;
                    return Ok(Bson::Document(dbref_doc));
                }
            }

            // Fallback: manually construct the document
            let mut dbref_doc = Document::new();
            let collection: String = obj.getattr("collection")?.extract()?;
            dbref_doc.insert("$ref", collection);

            let id_obj = obj.getattr("id")?;
            let id_bson = python_to_bson(id_obj, check_keys, codec_options)?;
            dbref_doc.insert("$id", id_bson);

            if let Ok(database_obj) = obj.getattr("database") {
                if !database_obj.is_none() {
                    let database: String = database_obj.extract()?;
                    dbref_doc.insert("$db", database);
                }
            }

            Ok(Bson::Document(dbref_doc))
        }
        _ => {
            // Unknown type marker, fall through to remaining types
            handle_remaining_python_types(obj, check_keys, codec_options)
        }
    }
}


fn handle_remaining_python_types(
    obj: Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bson> {
    use pyo3::types::PyList;
    use pyo3::types::PyTuple;

    // FAST PATH: Check for PyList first (most common sequence type)
    if let Ok(list) = obj.downcast::<PyList>() {
        let mut arr = Vec::with_capacity(list.len());
        for item in list {
            arr.push(python_to_bson(item, check_keys, codec_options)?);
        }
        return Ok(Bson::Array(arr));
    }

    // FAST PATH: Check for PyTuple
    if let Ok(tuple) = obj.downcast::<PyTuple>() {
        let mut arr = Vec::with_capacity(tuple.len());
        for item in tuple {
            arr.push(python_to_bson(item, check_keys, codec_options)?);
        }
        return Ok(Bson::Array(arr));
    }

    // Check for bytes/bytearray by type (not by extract, which would match tuples)
    // Raw bytes without Binary wrapper -> subtype 0
    if obj.is_instance_of::<PyBytes>() {
        let v: Vec<u8> = obj.extract()?;
        return Ok(Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: v,
        }));
    }

    // Check for dict-like objects (SON, OrderedDict, etc.)
    if obj.hasattr("items")? {
        // Any object with items() method (dict, SON, OrderedDict, etc.)
        let doc = python_mapping_to_bson_doc(&obj, check_keys, codec_options, false)?;
        return Ok(Bson::Document(doc));
    }

    // SLOW PATH: Try generic sequence extraction
    if let Ok(list) = obj.extract::<Vec<Bound<'_, PyAny>>>() {
        // Check for sequences (lists, tuples)
        let mut arr = Vec::new();
        for item in list {
            arr.push(python_to_bson(item, check_keys, codec_options)?);
        }
        return Ok(Bson::Array(arr));
    }

    // Get object repr and type for error message
    let obj_repr = obj.repr().map(|r| r.to_string()).unwrap_or_else(|_| "?".to_string());
    let obj_type = obj.get_type().to_string();
    Err(invalid_document_error(obj.py(), format!(
        "cannot encode object: {}, of type: {}",
        obj_repr, obj_type
    )))
}

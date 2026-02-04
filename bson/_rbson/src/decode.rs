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

//! BSON decoding functions
//!
//! This module contains all functions for decoding BSON bytes to Python objects.

use bson::{doc, Bson, Document};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyAny, PyBytes, PyDict, PyList, PyString};
use std::io::Cursor;

use crate::errors::{invalid_bson_error, invalid_document_error};
use crate::types::{TYPE_CACHE};
use crate::utils::{str_flags_to_int};

#[pyfunction]
#[pyo3(signature = (data, _codec_options))]
pub fn _bson_to_dict(
    py: Python,
    data: &Bound<'_, PyAny>,
    _codec_options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let codec_options = Some(_codec_options);
    // Accept bytes, bytearray, memoryview, and other buffer protocol objects
    // Try to get bytes using the buffer protocol
    let bytes: Vec<u8> = if let Ok(b) = data.extract::<Vec<u8>>() {
        b
    } else if let Ok(bytes_obj) = data.downcast::<PyBytes>() {
        bytes_obj.as_bytes().to_vec()
    } else {
        // Try to use buffer protocol for memoryview, array, mmap, etc.
        match data.call_method0("__bytes__") {
            Ok(bytes_result) => {
                if let Ok(bytes_obj) = bytes_result.downcast::<PyBytes>() {
                    bytes_obj.as_bytes().to_vec()
                } else {
                    return Err(PyTypeError::new_err("data must be bytes, bytearray, memoryview, or buffer protocol object"));
                }
            }
            Err(_) => {
                // Try tobytes() method (for array.array)
                match data.call_method0("tobytes") {
                    Ok(bytes_result) => {
                        if let Ok(bytes_obj) = bytes_result.downcast::<PyBytes>() {
                            bytes_obj.as_bytes().to_vec()
                        } else {
                            return Err(PyTypeError::new_err("data must be bytes, bytearray, memoryview, or buffer protocol object"));
                        }
                    }
                    Err(_) => {
                        // Try read() method (for mmap)
                        match data.call_method0("read") {
                            Ok(bytes_result) => {
                                if let Ok(bytes_obj) = bytes_result.downcast::<PyBytes>() {
                                    bytes_obj.as_bytes().to_vec()
                                } else {
                                    return Err(PyTypeError::new_err("data must be bytes, bytearray, memoryview, or buffer protocol object"));
                                }
                            }
                            Err(_) => {
                                return Err(PyTypeError::new_err("data must be bytes, bytearray, memoryview, or buffer protocol object"));
                            }
                        }
                    }
                }
            }
        }
    };

    // Validate BSON document structure
    // Minimum size is 5 bytes (4 bytes for size + 1 byte for null terminator)
    if bytes.len() < 5 {
        return Err(invalid_bson_error(py, "not enough data for a BSON document".to_string()));
    }

    // Check that the size field matches the actual data length
    let size = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if size != bytes.len() {
        if size < bytes.len() {
            return Err(invalid_bson_error(py, "bad eoo".to_string()));
        } else {
            return Err(invalid_bson_error(py, "invalid message size".to_string()));
        }
    }

    // Check that the document ends with a null terminator
    if bytes[bytes.len() - 1] != 0 {
        return Err(invalid_bson_error(py, "bad eoo".to_string()));
    }

    // Check minimum size
    if size < 5 {
        return Err(invalid_bson_error(py, "invalid message size".to_string()));
    }

    // Extract unicode_decode_error_handler from codec_options
    let unicode_error_handler = if let Some(opts) = codec_options {
        opts.getattr("unicode_decode_error_handler")
            .ok()
            .and_then(|h| h.extract::<String>().ok())
            .unwrap_or_else(|| "strict".to_string())
    } else {
        "strict".to_string()
    };

    // Try direct byte reading for better performance
    // If we encounter an unsupported type, fall back to Document-based approach
    match read_document_from_bytes(py, &bytes, 0, codec_options) {
        Ok(dict) => return Ok(dict),
        Err(e) => {
            let error_msg = format!("{}", e);

            // If we got a UTF-8 error and have a non-strict error handler, use Python fallback
            if error_msg.contains("utf-8") && unicode_error_handler != "strict" {
                let decode_func = TYPE_CACHE.get_bson_to_dict_python(py)?;
                let py_data = PyBytes::new_bound(py, &bytes);
                let py_opts = if let Some(opts) = codec_options {
                    opts.clone().into_py(py).into_bound(py)
                } else {
                    py.None().into_bound(py)
                };
                return Ok(decode_func.bind(py).call1((py_data, py_opts))?.into());
            }

            // If we got an unsupported type error, fall back to Document-based approach
            if error_msg.contains("Unsupported BSON type") || error_msg.contains("Detected unknown BSON type") {
                // Fall through to old implementation below
            } else {
                // For other errors, propagate them
                return Err(e);
            }
        }
    }

    // Fallback: Use Document-based approach for documents with unsupported types
    let cursor = Cursor::new(&bytes);
    let doc_result = Document::from_reader(cursor);

    if let Err(ref e) = doc_result {
        let error_msg = format!("{}", e);
        if error_msg.contains("utf-8") && unicode_error_handler != "strict" {
            let decode_func = TYPE_CACHE.get_bson_to_dict_python(py)?;
            let py_data = PyBytes::new_bound(py, &bytes);
            let py_opts = if let Some(opts) = codec_options {
                opts.clone().into_py(py).into_bound(py)
            } else {
                py.None().into_bound(py)
            };
            return Ok(decode_func.bind(py).call1((py_data, py_opts))?.into());
        }
    }

    let doc = doc_result.map_err(|e| {
        let error_msg = format!("{}", e);

        // Try to match C extension error format for unknown BSON types
        // C extension: "type b'\\x14' for fieldname 'foo'"
        // Rust bson: "error at key \"foo\": malformed value: \"invalid tag: 20\""
        if error_msg.contains("invalid tag:") {
            // Extract the tag number and field name
            if let Some(tag_start) = error_msg.find("invalid tag: ") {
                let tag_str = &error_msg[tag_start + 13..];
                if let Some(tag_end) = tag_str.find('"') {
                    if let Ok(tag_num) = tag_str[..tag_end].parse::<u8>() {
                        if let Some(key_start) = error_msg.find("error at key \"") {
                            let key_str = &error_msg[key_start + 14..];
                            if let Some(key_end) = key_str.find('"') {
                                let field_name = &key_str[..key_end];

                                // If the field name is numeric (array index), try to find the parent field name
                                let actual_field_name = if field_name.chars().all(|c| c.is_ascii_digit()) {
                                    // Try to find the parent field name by parsing the BSON
                                    find_parent_field_for_unknown_type(&bytes, tag_num).unwrap_or(field_name)
                                } else {
                                    field_name
                                };

                                let formatted_msg = format!("type b'\\x{:02x}' for fieldname '{}'", tag_num, actual_field_name);
                                return invalid_bson_error(py, formatted_msg);
                            }
                        }
                    }
                }
            }
        }

        invalid_bson_error(py, format!("invalid bson: {}", error_msg))
    })?;
    bson_doc_to_python_dict(py, &doc, codec_options)

    // Old path using Document::from_reader (kept as fallback, but not used)
    /*
    let cursor = Cursor::new(&bytes);
    let doc_result = Document::from_reader(cursor);

    // If we got a UTF-8 error and have a non-strict error handler, use Python fallback
    if let Err(ref e) = doc_result {
        let error_msg = format!("{}", e);
        if error_msg.contains("utf-8") && unicode_error_handler != "strict" {
            // Use Python's fallback implementation which handles unicode_decode_error_handler
            let bson_module = py.import("bson")?;
            let decode_func = bson_module.getattr("_bson_to_dict_python")?;
            let py_data = PyBytes::new(py, &bytes);
            let py_opts = if let Some(opts) = codec_options {
                opts.clone().into_py(py).into_bound(py)
            } else {
                py.None().into_bound(py)
            };
            return Ok(decode_func.call1((py_data, py_opts))?.into());
        }
    }
    */
}

/// Process a single item from a mapping's items() iterator

fn read_document_from_bytes(
    py: Python,
    bytes: &[u8],
    offset: usize,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    read_document_from_bytes_with_parent(py, bytes, offset, codec_options, None)
}


fn read_document_from_bytes_with_parent(
    py: Python,
    bytes: &[u8],
    offset: usize,
    codec_options: Option<&Bound<'_, PyAny>>,
    parent_field_name: Option<&str>,
) -> PyResult<Py<PyAny>> {
    // Read document size
    if bytes.len() < offset + 4 {
        return Err(invalid_bson_error(py, "not enough data for a BSON document".to_string()));
    }

    let size = i32::from_le_bytes([
        bytes[offset],
        bytes[offset + 1],
        bytes[offset + 2],
        bytes[offset + 3],
    ]) as usize;

    if offset + size > bytes.len() {
        return Err(invalid_bson_error(py, "invalid message size".to_string()));
    }

    // Get document_class from codec_options, default to dict
    let dict: Bound<'_, PyAny> = if let Some(opts) = codec_options {
        let document_class = opts.getattr("document_class")?;
        document_class.call0()?
    } else {
        PyDict::new(py).into_any()
    };

    // Read elements
    let mut pos = offset + 4; // Skip size field
    let end = offset + size - 1; // -1 for null terminator

    // Track if this might be a DBRef (has $ref and $id fields)
    let mut has_ref = false;
    let mut has_id = false;

    while pos < end {
        // Read type byte
        let type_byte = bytes[pos];
        pos += 1;

        if type_byte == 0 {
            break; // End of document
        }

        // Read key (null-terminated string)
        let key_start = pos;
        while pos < bytes.len() && bytes[pos] != 0 {
            pos += 1;
        }

        if pos >= bytes.len() {
            return Err(invalid_bson_error(py, "invalid bson: unexpected end of data".to_string()));
        }

        let key = std::str::from_utf8(&bytes[key_start..pos])
            .map_err(|e| invalid_bson_error(py, format!("invalid bson: invalid UTF-8 in key: {}", e)))?;

        pos += 1; // Skip null terminator

        // Track DBRef fields
        if key == "$ref" {
            has_ref = true;
        } else if key == "$id" {
            has_id = true;
        }

        // Determine the field name to use for error reporting
        // If the key is numeric (array index) and we have a parent field name, use the parent
        let error_field_name = if let Some(parent) = parent_field_name {
            if key.chars().all(|c| c.is_ascii_digit()) {
                parent
            } else {
                key
            }
        } else {
            key
        };

        // Read value based on type
        let (value, new_pos) = read_bson_value(py, bytes, pos, type_byte, codec_options, error_field_name)?;
        pos = new_pos;

        dict.set_item(key, value)?;
    }

    // Validate that we consumed exactly the right number of bytes
    // pos should be at end (which is offset + size - 1)
    // and the next byte should be the null terminator
    if pos != end {
        return Err(invalid_bson_error(py, "invalid length or type code".to_string()));
    }

    // Verify null terminator
    if bytes[pos] != 0 {
        return Err(invalid_bson_error(py, "invalid length or type code".to_string()));
    }

    // If this looks like a DBRef, convert it to a DBRef object
    if has_ref && has_id {
        return convert_dict_to_dbref(py, &dict, codec_options);
    }

    Ok(dict.into())
}

/// Read a single BSON value from bytes

fn read_bson_value(
    py: Python,
    bytes: &[u8],
    pos: usize,
    type_byte: u8,
    codec_options: Option<&Bound<'_, PyAny>>,
    field_name: &str,
) -> PyResult<(Py<PyAny>, usize)> {
    match type_byte {
        0x01 => {
            // Double
            if pos + 8 > bytes.len() {
                return Err(invalid_bson_error(py, "invalid bson: not enough data for double".to_string()));
            }
            let value = f64::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7],
            ]);
            Ok((value.into_py(py), pos + 8))
        }
        0x02 => {
            // String
            if pos + 4 > bytes.len() {
                return Err(invalid_bson_error(py, "invalid bson: not enough data for string length".to_string()));
            }
            let str_len = i32::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
            ]) as isize;

            // String length must be at least 1 (for null terminator)
            if str_len < 1 {
                return Err(invalid_bson_error(py, "invalid bson: bad string length".to_string()));
            }

            let str_start = pos + 4;
            let str_end = str_start + (str_len as usize) - 1; // -1 for null terminator

            if str_end >= bytes.len() {
                return Err(invalid_bson_error(py, "invalid bson: bad string length".to_string()));
            }

            // Validate that the null terminator is actually present
            if bytes[str_end] != 0 {
                return Err(invalid_bson_error(py, "invalid bson: bad string length".to_string()));
            }

            let s = std::str::from_utf8(&bytes[str_start..str_end])
                .map_err(|e| invalid_bson_error(py, format!("invalid bson: invalid UTF-8 in string: {}", e)))?;

            Ok((s.into_py(py), str_end + 1)) // +1 to skip null terminator
        }
        0x03 => {
            // Embedded document
            let doc = read_document_from_bytes(py, bytes, pos, codec_options)?;
            let size = i32::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
            ]) as usize;
            Ok((doc, pos + size))
        }
        0x04 => {
            // Array
            let arr = read_array_from_bytes(py, bytes, pos, codec_options, field_name)?;
            let size = i32::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
            ]) as usize;
            Ok((arr, pos + size))
        }
        0x08 => {
            // Boolean
            if pos >= bytes.len() {
                return Err(invalid_bson_error(py, "invalid bson: not enough data for boolean".to_string()));
            }
            let value = bytes[pos] != 0;
            Ok((value.into_py(py), pos + 1))
        }
        0x0A => {
            // Null
            Ok((py.None(), pos))
        }
        0x10 => {
            // Int32
            if pos + 4 > bytes.len() {
                return Err(invalid_bson_error(py, "invalid bson: not enough data for int32".to_string()));
            }
            let value = i32::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
            ]);
            Ok((value.into_py(py), pos + 4))
        }
        0x12 => {
            // Int64 - return as Int64 type to preserve type information
            if pos + 8 > bytes.len() {
                return Err(invalid_bson_error(py, "invalid bson: not enough data for int64".to_string()));
            }
            let value = i64::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7],
            ]);

            // Use cached Int64 class
            let int64_class = TYPE_CACHE.get_int64_class(py)?;
            let int64_obj = int64_class.bind(py).call1((value,))?;

            Ok((int64_obj.into(), pos + 8))
        }
        _ => {
            // For unknown BSON types, raise an error with the correct field name
            // Match C extension error format: "Detected unknown BSON type b'\xNN' for fieldname 'foo'"
            let error_msg = format!(
                "Detected unknown BSON type b'\\x{:02x}' for fieldname '{}'. Are you using the latest driver version?",
                type_byte, field_name
            );
            Err(invalid_bson_error(py, error_msg))
        }
    }
}


fn read_array_from_bytes(
    py: Python,
    bytes: &[u8],
    offset: usize,
    codec_options: Option<&Bound<'_, PyAny>>,
    parent_field_name: &str,
) -> PyResult<Py<PyAny>> {
    // Arrays are encoded as documents with numeric keys
    // We need to read it as a document and convert to a list
    // Pass the parent field name so that errors in array elements report the array field name
    let doc_dict = read_document_from_bytes_with_parent(py, bytes, offset, codec_options, Some(parent_field_name))?;

    // Convert dict to list (keys should be "0", "1", "2", ...)
    let dict = doc_dict.bind(py);
    let items = dict.call_method0("items")?;
    let mut pairs: Vec<(usize, Py<PyAny>)> = Vec::new();

    for item in items.iter()? {
        let item = item?;
        let tuple = item.downcast::<pyo3::types::PyTuple>()?;
        let key: String = tuple.get_item(0)?.extract()?;
        let value = tuple.get_item(1)?;
        let index: usize = key.parse()
            .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid array index"
            ))?;
        pairs.push((index, value.into_py(py)));
    }

    // Sort by index and extract values
    pairs.sort_by_key(|(idx, _)| *idx);
    let values: Vec<Py<PyAny>> = pairs.into_iter().map(|(_, v)| v).collect();

    Ok(pyo3::types::PyList::new(py, values)?.into_py(py))
}

/// Find the parent field name for an unknown type in an array

fn find_parent_field_for_unknown_type(bytes: &[u8], unknown_type: u8) -> Option<&str> {
    // Parse the BSON to find the field that contains the unknown type
    // We're looking for an array field that contains an element with the unknown type

    if bytes.len() < 5 {
        return None;
    }

    let size = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if size > bytes.len() {
        return None;
    }

    let mut pos = 4; // Skip size field
    let end = size - 1; // -1 for null terminator

    while pos < end && pos < bytes.len() {
        let type_byte = bytes[pos];
        pos += 1;

        if type_byte == 0 {
            break;
        }

        // Read field name
        let key_start = pos;
        while pos < bytes.len() && bytes[pos] != 0 {
            pos += 1;
        }

        if pos >= bytes.len() {
            return None;
        }

        let key = match std::str::from_utf8(&bytes[key_start..pos]) {
            Ok(k) => k,
            Err(_) => return None,
        };

        pos += 1; // Skip null terminator

        // Check if this is an array (type 0x04)
        if type_byte == 0x04 {
            // Read array size
            if pos + 4 > bytes.len() {
                return None;
            }
            let array_size = i32::from_le_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
            ]) as usize;

            // Check if the array contains the unknown type
            let array_start = pos;
            let array_end = pos + array_size;
            if array_end > bytes.len() {
                return None;
            }

            // Scan the array for the unknown type
            let mut array_pos = array_start + 4; // Skip array size
            while array_pos < array_end - 1 {
                let elem_type = bytes[array_pos];
                if elem_type == 0 {
                    break;
                }

                if elem_type == unknown_type {
                    // Found it! Return the array field name
                    return Some(key);
                }

                array_pos += 1;

                // Skip element name
                while array_pos < bytes.len() && bytes[array_pos] != 0 {
                    array_pos += 1;
                }
                if array_pos >= bytes.len() {
                    return None;
                }
                array_pos += 1;

                // We can't easily skip the value without parsing it fully,
                // so just break here and return the key if we found the type
                break;
            }

            pos += array_size;
        } else {
            // Skip other types - we need to know their sizes
            match type_byte {
                0x01 => pos += 8,  // Double
                0x02 => {  // String
                    if pos + 4 > bytes.len() {
                        return None;
                    }
                    let str_len = i32::from_le_bytes([
                        bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                    ]) as usize;
                    pos += 4 + str_len;
                }
                0x03 | 0x04 => {  // Document or Array
                    if pos + 4 > bytes.len() {
                        return None;
                    }
                    let doc_size = i32::from_le_bytes([
                        bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                    ]) as usize;
                    pos += doc_size;
                }
                0x08 => pos += 1,  // Boolean
                0x0A => {},  // Null
                0x10 => pos += 4,  // Int32
                0x12 => pos += 8,  // Int64
                _ => return None,  // Unknown type, can't continue
            }
        }
    }

    None
}

/// Decode BSON bytes to a Python dictionary
/// This is the main entry point matching the C extension API

fn bson_to_python(
    py: Python,
    bson: &Bson,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    match bson {
        Bson::Null => Ok(py.None()),
        Bson::Boolean(v) => Ok((*v).into_py(py)),
        Bson::Int32(v) => Ok((*v as i64).into_py(py)),
        Bson::Int64(v) => {
            // Return bson.int64.Int64 object instead of plain Python int
            let int64_class = TYPE_CACHE.get_int64_class(py)?;
            let int64_obj = int64_class.bind(py).call1((*v,))?;
            Ok(int64_obj.into())
        }
        Bson::Double(v) => Ok((*v).into_py(py)),
        Bson::String(v) => Ok(v.into_py(py)),
        Bson::Binary(v) => decode_binary(py, v, codec_options),
        Bson::Document(v) => bson_doc_to_python_dict(py, v, codec_options),
        Bson::Array(v) => {
            let list = pyo3::types::PyList::empty(py);
            for item in v {
                list.append(bson_to_python(py, item, codec_options)?)?;
            }
            Ok(list.into())
        }
        Bson::ObjectId(v) => {
            // Use cached ObjectId class
            let objectid_class = TYPE_CACHE.get_objectid_class(py)?;

            // Create ObjectId from bytes
            let bytes = PyBytes::new_bound(py, &v.bytes());
            let objectid = objectid_class.bind(py).call1((bytes,))?;
            Ok(objectid.into())
        }
        Bson::DateTime(v) => decode_datetime(py, v, codec_options),
        Bson::RegularExpression(v) => {
            // Use cached Regex class
            let regex_class = TYPE_CACHE.get_regex_class(py)?;

            // Convert BSON regex options to Python flags
            let flags = str_flags_to_int(&v.options);

            // Create Regex(pattern, flags)
            let regex = regex_class.bind(py).call1((v.pattern.clone(), flags))?;
            Ok(regex.into())
        }
        Bson::JavaScriptCode(v) => {
            // Use cached Code class
            let code_class = TYPE_CACHE.get_code_class(py)?;

            // Create Code(code)
            let code = code_class.bind(py).call1((v,))?;
            Ok(code.into())
        }
        Bson::JavaScriptCodeWithScope(v) => {
            // Use cached Code class
            let code_class = TYPE_CACHE.get_code_class(py)?;

            // Convert scope to Python dict
            let scope_dict = bson_doc_to_python_dict(py, &v.scope, codec_options)?;

            // Create Code(code, scope)
            let code = code_class.bind(py).call1((v.code.clone(), scope_dict))?;
            Ok(code.into())
        }
        Bson::Timestamp(v) => {
            // Use cached Timestamp class
            let timestamp_class = TYPE_CACHE.get_timestamp_class(py)?;

            // Create Timestamp(time, inc)
            let timestamp = timestamp_class.bind(py).call1((v.time, v.increment))?;
            Ok(timestamp.into())
        }
        Bson::Decimal128(v) => {
            // Use cached Decimal128 class
            let decimal128_class = TYPE_CACHE.get_decimal128_class(py)?;

            // Create Decimal128 from bytes
            let bytes = PyBytes::new_bound(py, &v.bytes());

            // Use from_bid class method
            let decimal128 = decimal128_class.bind(py).call_method1("from_bid", (bytes,))?;
            Ok(decimal128.into())
        }
        Bson::MaxKey => {
            // Use cached MaxKey class
            let maxkey_class = TYPE_CACHE.get_maxkey_class(py)?;

            // Create MaxKey instance
            let maxkey = maxkey_class.bind(py).call0()?;
            Ok(maxkey.into())
        }
        Bson::MinKey => {
            // Use cached MinKey class
            let minkey_class = TYPE_CACHE.get_minkey_class(py)?;

            // Create MinKey instance
            let minkey = minkey_class.bind(py).call0()?;
            Ok(minkey.into())
        }
        Bson::Symbol(v) => {
            // Symbol is deprecated but we need to support decoding it
            Ok(PyString::new(py, v).into())
        }
        Bson::Undefined => {
            // Undefined is deprecated, return None
            Ok(py.None())
        }
        Bson::DbPointer(v) => {
            // DBPointer is deprecated, decode to DBRef
            // The DbPointer struct has private fields, so we need to use Debug to extract them
            let debug_str = format!("{:?}", v);

            // Parse the debug string: DbPointer { namespace: "...", id: ObjectId("...") }
            // Extract namespace and ObjectId hex string
            let namespace_start = debug_str.find("namespace: \"").map(|i| i + 12);
            let namespace_end = debug_str.find("\", id:");
            let oid_start = debug_str.find("ObjectId(\"").map(|i| i + 10);
            let oid_end = debug_str.rfind("\")");

            if let (Some(ns_start), Some(ns_end), Some(oid_start), Some(oid_end)) =
                (namespace_start, namespace_end, oid_start, oid_end) {
                let namespace = &debug_str[ns_start..ns_end];
                let oid_hex = &debug_str[oid_start..oid_end];

                // Use cached DBRef and ObjectId classes
                let dbref_class = TYPE_CACHE.get_dbref_class(py)?;
                let objectid_class = TYPE_CACHE.get_objectid_class(py)?;

                // Create ObjectId from hex string
                let objectid = objectid_class.bind(py).call1((oid_hex,))?;

                // Create DBRef(collection, id)
                let dbref = dbref_class.bind(py).call1((namespace, objectid))?;
                Ok(dbref.into())
            } else {
                Err(invalid_document_error(py, format!(
                    "invalid bson: Failed to parse DBPointer: {:?}",
                    v
                )))
            }
        }
        _ => Err(invalid_document_error(py, format!(
            "invalid bson: Unsupported BSON type for Python conversion: {:?}",
            bson
        ))),
    }
}


fn bson_doc_to_python_dict(
    py: Python,
    doc: &Document,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Check if this document is a DBRef (has $ref and $id fields)
    if doc.contains_key("$ref") && doc.contains_key("$id") {
        return decode_dbref(py, doc, codec_options);
    }

    // Get document_class from codec_options, default to dict
    let dict: Bound<'_, PyAny> = if let Some(opts) = codec_options {
        let document_class = opts.getattr("document_class")?;
        document_class.call0()?
    } else {
        PyDict::new(py).into_any()
    };

    for (key, value) in doc {
        let py_value = bson_to_python(py, value, codec_options)?;
        dict.set_item(key, py_value)?;
    }

    Ok(dict.into())
}

/// Convert a Python dict that looks like a DBRef to a DBRef object

fn convert_dict_to_dbref(
    py: Python,
    dict: &Bound<'_, PyAny>,
    _codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Check if $ref field exists
    if !dict.call_method1("__contains__", ("$ref",))?.extract::<bool>()? {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("DBRef missing $ref field"));
    }
    let collection = dict.call_method1("get", ("$ref",))?;
    let collection_str: String = collection.extract()?;

    // Check if $id field exists (value can be None)
    if !dict.call_method1("__contains__", ("$id",))?.extract::<bool>()? {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("DBRef missing $id field"));
    }
    let id_obj = dict.call_method1("get", ("$id",))?;

    // Use cached DBRef class
    let dbref_class = TYPE_CACHE.get_dbref_class(py)?;

    // Get optional $db field
    let database_opt = dict.call_method1("get", ("$db",))?;

    // Build kwargs for extra fields (anything other than $ref, $id, $db)
    let kwargs = PyDict::new(py);
    let items = dict.call_method0("items")?;
    for item in items.try_iter()? {
        let item = item?;
        let tuple = item.downcast::<pyo3::types::PyTuple>()?;
        let key: String = tuple.get_item(0)?.extract()?;
        if key != "$ref" && key != "$id" && key != "$db" {
            let value = tuple.get_item(1)?;
            kwargs.set_item(key, value)?;
        }
    }

    // Create DBRef with positional args and kwargs
    if !database_opt.is_none() {
        let database_str: String = database_opt.extract()?;
        let dbref = dbref_class.bind(py).call((collection_str, id_obj, database_str), Some(&kwargs))?;
        return Ok(dbref.into());
    }

    let dbref = dbref_class.bind(py).call((collection_str, id_obj), Some(&kwargs))?;
    Ok(dbref.into())
}


fn decode_dbref(
    py: Python,
    doc: &Document,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let collection = if let Some(Bson::String(s)) = doc.get("$ref") {
        s.clone()
    } else {
        return Err(invalid_document_error(py, "Invalid document: DBRef $ref field must be a string".to_string()));
    };

    let id_bson = doc.get("$id").ok_or_else(|| invalid_document_error(py, "Invalid document: DBRef missing $id field".to_string()))?;
    let id_py = bson_to_python(py, id_bson, codec_options)?;

    // Use cached DBRef class
    let dbref_class = TYPE_CACHE.get_dbref_class(py)?;

    // Get optional $db field
    let database_arg = if let Some(db_bson) = doc.get("$db") {
        if let Bson::String(database) = db_bson {
            Some(database.clone())
        } else {
            None
        }
    } else {
        None
    };

    // Collect any extra fields (not $ref, $id, or $db) as kwargs
    let kwargs = PyDict::new(py);
    for (key, value) in doc {
        if key != "$ref" && key != "$id" && key != "$db" {
            let py_value = bson_to_python(py, value, codec_options)?;
            kwargs.set_item(key, py_value)?;
        }
    }

    // Create DBRef with positional args and kwargs
    if let Some(database) = database_arg {
        let dbref = dbref_class.bind(py).call((collection, id_py, database), Some(&kwargs))?;
        Ok(dbref.into())
    } else {
        let dbref = dbref_class.bind(py).call((collection, id_py), Some(&kwargs))?;
        Ok(dbref.into())
    }
}


fn decode_binary(
    py: Python,
    v: &bson::Binary,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let subtype = match &v.subtype {
        bson::spec::BinarySubtype::Generic => 0u8,
        bson::spec::BinarySubtype::Function => 1u8,
        bson::spec::BinarySubtype::BinaryOld => 2u8,
        bson::spec::BinarySubtype::UuidOld => 3u8,
        bson::spec::BinarySubtype::Uuid => 4u8,
        bson::spec::BinarySubtype::Md5 => 5u8,
        bson::spec::BinarySubtype::Encrypted => 6u8,
        bson::spec::BinarySubtype::Column => 7u8,
        bson::spec::BinarySubtype::Sensitive => 8u8,
        bson::spec::BinarySubtype::Vector => 9u8,
        bson::spec::BinarySubtype::Reserved(s) => *s,
        bson::spec::BinarySubtype::UserDefined(s) => *s,
        _ => {
            return Err(invalid_document_error(py,
                "invalid bson: Encountered unknown binary subtype that cannot be converted".to_string(),
            ));
        }
    };

    // Check for UUID subtypes (3 and 4)
    if subtype == 3 || subtype == 4 {
        let should_decode_as_uuid = if let Some(opts) = codec_options {
            if let Ok(uuid_rep) = opts.getattr("uuid_representation") {
                if let Ok(rep_value) = uuid_rep.extract::<i32>() {
                    // Decode as UUID if representation is not UNSPECIFIED (0)
                    rep_value != 0
                } else {
                    true
                }
            } else {
                true
            }
        } else {
            true
        };

        if should_decode_as_uuid {
            // Decode as UUID using cached class
            let uuid_class = TYPE_CACHE.get_uuid_class(py)?;
            let bytes_obj = PyBytes::new_bound(py, &v.bytes);
            let kwargs = [("bytes", bytes_obj)].into_py_dict_bound(py);
            let uuid_obj = uuid_class.bind(py).call((), Some(&kwargs))?;
            return Ok(uuid_obj.into());
        }
    }

    if subtype == 0 {
        Ok(PyBytes::new_bound(py, &v.bytes).into())
    } else {
        // Use cached Binary class
        let binary_class = TYPE_CACHE.get_binary_class(py)?;

        // Create Binary(data, subtype)
        let bytes = PyBytes::new_bound(py, &v.bytes);
        let binary = binary_class.bind(py).call1((bytes, subtype))?;
        Ok(binary.into())
    }
}


fn decode_datetime(
    py: Python,
    v: &bson::DateTime,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Check datetime_conversion from codec_options
    // DATETIME_CLAMP = 2, DATETIME_MS = 3, DATETIME_AUTO = 4
    let datetime_conversion = if let Some(opts) = codec_options {
        if let Ok(dt_conv) = opts.getattr("datetime_conversion") {
            // Extract the enum value as an integer
            if let Ok(conv_int) = dt_conv.call_method0("__int__") {
                conv_int.extract::<i32>().unwrap_or(4)
            } else {
                4
            }
        } else {
            4
        }
    } else {
        4
    };

    // Python datetime range: datetime.min to datetime.max
    // Min: -62135596800000 ms (year 1)
    // Max: 253402300799999 ms (year 9999)
    const DATETIME_MIN_MS: i64 = -62135596800000;
    const DATETIME_MAX_MS: i64 = 253402300799999;

    // Extremely out of range values (beyond what can be represented)
    // These should raise InvalidBSON with a helpful error message
    const EXTREME_MIN_MS: i64 = -2i64.pow(52);  // -4503599627370496
    const EXTREME_MAX_MS: i64 = 2i64.pow(52);   // 4503599627370496

    let mut millis = v.timestamp_millis();
    let is_out_of_range = millis < DATETIME_MIN_MS || millis > DATETIME_MAX_MS;
    let is_extremely_out_of_range = millis <= EXTREME_MIN_MS || millis >= EXTREME_MAX_MS;

    // If extremely out of range, raise InvalidBSON with suggestion
    if is_extremely_out_of_range {
        let error_msg = format!(
            "Value {} is too large or too small to be a valid BSON datetime. \
            (Consider Using CodecOptions(datetime_conversion=DATETIME_AUTO) or \
            MongoClient(datetime_conversion='DATETIME_AUTO')). See: \
            https://www.mongodb.com/docs/languages/python/pymongo-driver/current/data-formats/dates-and-times/#handling-out-of-range-datetimes",
            millis
        );
        return Err(invalid_bson_error(py, error_msg));
    }

    // If DATETIME_MS (3), always return DatetimeMS object
    if datetime_conversion == 3 {
        let datetime_ms_class = TYPE_CACHE.get_datetime_ms_class(py)?;
        let datetime_ms = datetime_ms_class.bind(py).call1((millis,))?;
        return Ok(datetime_ms.into());
    }

    // If DATETIME_AUTO (4) and out of range, return DatetimeMS
    if datetime_conversion == 4 && is_out_of_range {
        let datetime_ms_class = TYPE_CACHE.get_datetime_ms_class(py)?;
        let datetime_ms = datetime_ms_class.bind(py).call1((millis,))?;
        return Ok(datetime_ms.into());
    }

    // Track the original millis value before clamping for timezone conversion
    let original_millis = millis;

    // If DATETIME_CLAMP (2), clamp to valid datetime range
    if datetime_conversion == 2 {
        if millis < DATETIME_MIN_MS {
            millis = DATETIME_MIN_MS;
        } else if millis > DATETIME_MAX_MS {
            millis = DATETIME_MAX_MS;
        }
    } else if is_out_of_range {
        // For other modes, raise error if out of range
        return Err(PyErr::new::<pyo3::exceptions::PyOverflowError, _>(
            "date value out of range"
        ));
    }

    // Check if tz_aware is False in codec_options
    let tz_aware = if let Some(opts) = codec_options {
        if let Ok(tz_aware_val) = opts.getattr("tz_aware") {
            tz_aware_val.extract::<bool>().unwrap_or(true)
        } else {
            true
        }
    } else {
        true
    };

    // Convert to Python datetime using cached class
    let datetime_class = TYPE_CACHE.get_datetime_class(py)?;

    // Convert milliseconds to seconds and microseconds
    let seconds = millis / 1000;
    let microseconds = (millis % 1000) * 1000;

    if tz_aware {
        // Return timezone-aware datetime with UTC timezone using cached utc
        let utc = TYPE_CACHE.get_utc(py)?;

        // Construct datetime from epoch using timedelta to avoid platform-specific limitations
        // This works on all platforms including Windows for dates outside fromtimestamp() range
        let epoch = datetime_class.bind(py).call1((1970, 1, 1, 0, 0, 0, 0, utc.bind(py)))?;
        let datetime_module = py.import_bound("datetime")?;
        let timedelta_class = datetime_module.getattr("timedelta")?;

        // Create timedelta for seconds and microseconds
        let kwargs = [("seconds", seconds), ("microseconds", microseconds)].into_py_dict_bound(py);
        let delta = timedelta_class.call((), Some(&kwargs))?;
        let dt_final = epoch.call_method1("__add__", (delta,))?;

        // Convert to local timezone if tzinfo is provided in codec_options
        if let Some(opts) = codec_options {
            if let Ok(tzinfo) = opts.getattr("tzinfo") {
                if !tzinfo.is_none() {
                    // Call astimezone(tzinfo) to convert to the specified timezone
                    // This might fail with OverflowError if the datetime is at the boundary
                    match dt_final.call_method1("astimezone", (&tzinfo,)) {
                        Ok(local_dt) => return Ok(local_dt.into()),
                        Err(e) => {
                            // If OverflowError during clamping, return datetime.min or datetime.max with the target tzinfo
                            if e.is_instance_of::<pyo3::exceptions::PyOverflowError>(py) && datetime_conversion == 2 {
                                // Check if dt_final is at datetime.min or datetime.max
                                let datetime_min = datetime_class.bind(py).getattr("min")?;
                                let datetime_max = datetime_class.bind(py).getattr("max")?;

                                // Compare year to determine if we're at min or max
                                let year = dt_final.getattr("year")?.extract::<i32>()?;

                                if year == 1 {
                                    // At datetime.min, return datetime.min.replace(tzinfo=tzinfo)
                                    let kwargs = [("tzinfo", &tzinfo)].into_py_dict_bound(py);
                                    let dt_with_tz = datetime_min.call_method("replace", (), Some(&kwargs))?;
                                    return Ok(dt_with_tz.into());
                                } else {
                                    // At datetime.max, return datetime.max.replace(tzinfo=tzinfo, microsecond=999000)
                                    let microsecond = 999000i32.into_py(py).into_bound(py);
                                    let kwargs = [("tzinfo", &tzinfo), ("microsecond", &microsecond)].into_py_dict_bound(py);
                                    let dt_with_tz = datetime_max.call_method("replace", (), Some(&kwargs))?;
                                    return Ok(dt_with_tz.into());
                                }
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        Ok(dt_final.into())
    } else {
        // Return naive datetime (no timezone)
        // Construct datetime from epoch using timedelta to avoid platform-specific limitations
        let epoch = datetime_class.bind(py).call1((1970, 1, 1, 0, 0, 0, 0))?;
        let datetime_module = py.import_bound("datetime")?;
        let timedelta_class = datetime_module.getattr("timedelta")?;

        // Create timedelta for seconds and microseconds
        let kwargs = [("seconds", seconds), ("microseconds", microseconds)].into_py_dict_bound(py);
        let delta = timedelta_class.call((), Some(&kwargs))?;
        let naive_dt = epoch.call_method1("__add__", (delta,))?;
        Ok(naive_dt.into())
    }
}

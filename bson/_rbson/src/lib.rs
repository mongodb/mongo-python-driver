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

//! Rust implementation of BSON encoding/decoding functions
//!
//! This module provides the same interface as the C extension (bson._cbson)
//! but implemented in Rust using PyO3 and the bson library.

#![allow(clippy::useless_conversion)]

use bson::{doc, Bson, Document};
use once_cell::sync::OnceCell;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyAny, PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString};
use std::io::Cursor;

/// Cache for Python type objects to avoid repeated imports
struct TypeCache {
    uuid_class: OnceCell<PyObject>,
    datetime_class: OnceCell<PyObject>,
    pattern_class: OnceCell<PyObject>,
}

static TYPE_CACHE: TypeCache = TypeCache {
    uuid_class: OnceCell::new(),
    datetime_class: OnceCell::new(),
    pattern_class: OnceCell::new(),
};

impl TypeCache {
    /// Get or initialize the UUID class
    fn get_uuid_class(&self, py: Python) -> Option<PyObject> {
        self.uuid_class.get_or_init(|| {
            py.import("uuid")
                .and_then(|m| m.getattr("UUID"))
                .map(|c| c.unbind())
                .ok()
                .unwrap_or_else(|| py.None())
        }).clone_ref(py).into()
    }

    /// Get or initialize the datetime class
    fn get_datetime_class(&self, py: Python) -> Option<PyObject> {
        self.datetime_class.get_or_init(|| {
            py.import("datetime")
                .and_then(|m| m.getattr("datetime"))
                .map(|c| c.unbind())
                .ok()
                .unwrap_or_else(|| py.None())
        }).clone_ref(py).into()
    }

    /// Get or initialize the regex Pattern class
    fn get_pattern_class(&self, py: Python) -> Option<PyObject> {
        self.pattern_class.get_or_init(|| {
            py.import("re")
                .and_then(|m| m.getattr("Pattern"))
                .map(|c| c.unbind())
                .ok()
                .unwrap_or_else(|| py.None())
        }).clone_ref(py).into()
    }
}

/// Helper to create InvalidDocument exception
fn invalid_document_error(py: Python, msg: String) -> PyErr {
    let bson_errors = py.import("bson.errors").expect("Failed to import bson.errors");
    let invalid_document = bson_errors.getattr("InvalidDocument").expect("Failed to get InvalidDocument");
    PyErr::from_value(invalid_document.call1((msg,)).expect("Failed to create InvalidDocument").into())
}

/// Helper to create InvalidDocument exception with document property
fn invalid_document_error_with_doc(py: Python, msg: String, doc: &Bound<'_, PyAny>) -> PyErr {
    let bson_errors = py.import("bson.errors").expect("Failed to import bson.errors");
    let invalid_document = bson_errors.getattr("InvalidDocument").expect("Failed to get InvalidDocument");
    // Call with positional arguments: InvalidDocument(message, document)
    use pyo3::types::PyTuple;
    let args = PyTuple::new(py, &[msg.into_py(py), doc.clone().into_py(py)]).expect("Failed to create tuple");
    PyErr::from_value(invalid_document.call1(args).expect("Failed to create InvalidDocument").into())
}

/// Helper to create InvalidBSON exception
fn invalid_bson_error(py: Python, msg: String) -> PyErr {
    let bson_errors = py.import("bson.errors").expect("Failed to import bson.errors");
    let invalid_bson = bson_errors.getattr("InvalidBSON").expect("Failed to get InvalidBSON");
    PyErr::from_value(invalid_bson.call1((msg,)).expect("Failed to create InvalidBSON").into())
}

// Type markers for BSON objects
const BINARY_TYPE_MARKER: i32 = 5;
const OBJECTID_TYPE_MARKER: i32 = 7;
const DATETIME_TYPE_MARKER: i32 = 9;
const REGEX_TYPE_MARKER: i32 = 11;
const CODE_TYPE_MARKER: i32 = 13;
const SYMBOL_TYPE_MARKER: i32 = 14;
const DBPOINTER_TYPE_MARKER: i32 = 15;
const TIMESTAMP_TYPE_MARKER: i32 = 17;
const INT64_TYPE_MARKER: i32 = 18;
const DECIMAL128_TYPE_MARKER: i32 = 19;
const DBREF_TYPE_MARKER: i32 = 100;
const MAXKEY_TYPE_MARKER: i32 = 127;
const MINKEY_TYPE_MARKER: i32 = 255;

/// Convert Python datetime to milliseconds since epoch UTC
/// This is equivalent to Python's bson.datetime_ms._datetime_to_millis()
fn datetime_to_millis(py: Python, dtm: &Bound<'_, PyAny>) -> PyResult<i64> {
    // Get datetime components
    let year: i32 = dtm.getattr("year")?.extract()?;
    let month: i32 = dtm.getattr("month")?.extract()?;
    let day: i32 = dtm.getattr("day")?.extract()?;
    let hour: i32 = dtm.getattr("hour")?.extract()?;
    let minute: i32 = dtm.getattr("minute")?.extract()?;
    let second: i32 = dtm.getattr("second")?.extract()?;
    let microsecond: i32 = dtm.getattr("microsecond")?.extract()?;

    // Check if datetime has timezone offset
    let utcoffset = dtm.call_method0("utcoffset")?;
    let offset_seconds: i64 = if !utcoffset.is_none() {
        // Get total_seconds() from timedelta
        let total_seconds: f64 = utcoffset.call_method0("total_seconds")?.extract()?;
        total_seconds as i64
    } else {
        0
    };

    // Calculate seconds since epoch using the same algorithm as Python's calendar.timegm
    // This is: (year - 1970) * 365.25 days + month/day adjustments + time
    // We'll use Python's calendar.timegm for accuracy
    let calendar = py.import("calendar")?;
    let timegm = calendar.getattr("timegm")?;

    // Create a time tuple (year, month, day, hour, minute, second, weekday, yearday, isdst)
    // We need timetuple() method
    let timetuple = dtm.call_method0("timetuple")?;
    let seconds_since_epoch: i64 = timegm.call1((timetuple,))?.extract()?;

    // Adjust for timezone offset (subtract to get UTC)
    let utc_seconds = seconds_since_epoch - offset_seconds;

    // Convert to milliseconds and add microseconds
    let millis = utc_seconds * 1000 + (microsecond / 1000) as i64;

    Ok(millis)
}

/// Convert Python regex flags (int) to BSON regex options (string)
fn int_flags_to_str(flags: i32) -> String {
    let mut options = String::new();

    // Python re module flags to BSON regex options:
    // re.IGNORECASE = 2 -> 'i'
    // re.MULTILINE = 8 -> 'm'
    // re.DOTALL = 16 -> 's'
    // re.VERBOSE = 64 -> 'x'
    // Note: re.LOCALE and re.UNICODE are Python-specific

    if flags & 2 != 0 {
        options.push('i');
    }
    if flags & 4 != 0 {
        options.push('l'); // Preserved for round-trip compatibility
    }
    if flags & 8 != 0 {
        options.push('m');
    }
    if flags & 16 != 0 {
        options.push('s');
    }
    if flags & 32 != 0 {
        options.push('u'); // Preserved for round-trip compatibility
    }
    if flags & 64 != 0 {
        options.push('x');
    }

    options
}

/// Convert BSON regex options (string) to Python regex flags (int)
fn str_flags_to_int(options: &str) -> i32 {
    let mut flags = 0;

    for ch in options.chars() {
        match ch {
            'i' => flags |= 2,  // re.IGNORECASE
            'l' => flags |= 4,  // re.LOCALE
            'm' => flags |= 8,  // re.MULTILINE
            's' => flags |= 16, // re.DOTALL
            'u' => flags |= 32, // re.UNICODE
            'x' => flags |= 64, // re.VERBOSE
            _ => {}             // Ignore unknown flags
        }
    }

    flags
}

/// Test function for POC validation
#[pyfunction]
fn _test_rust_extension(py: Python) -> PyResult<PyObject> {
    let result = PyDict::new(py);
    result.set_item("implementation", "rust")?;
    result.set_item("version", "0.1.0")?;
    result.set_item("status", "production-ready")?;
    result.set_item("pyo3_version", env!("CARGO_PKG_VERSION"))?;
    Ok(result.into())
}

/// Write a BSON document directly to bytes without intermediate Document structure
/// This is much faster than building a Document and then serializing it
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

/// Validate a document key
fn validate_key(key: &str, check_keys: bool) -> PyResult<()> {
    // Check for null bytes (always invalid)
    if key.contains('\0') {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Key names must not contain the NULL byte"
        ));
    }

    // Check keys if requested (but not for _id)
    if check_keys && key != "_id" {
        if key.starts_with('$') {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("key '{}' must not start with '$'", key)
            ));
        }
        if key.contains('.') {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("key '{}' must not contain '.'", key)
            ));
        }
    }

    Ok(())
}

/// Write a single BSON element directly to bytes
/// BSON element format: type (1 byte) + key (cstring) + value (type-specific)
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

/// Write a C-style null-terminated string
fn write_cstring(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// Write a BSON string (int32 length + string + null terminator)
fn write_string(buf: &mut Vec<u8>, s: &str) {
    let len = (s.len() + 1) as i32; // +1 for null terminator
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf.push(0);
}

/// Write a Python list as a BSON array directly to bytes
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

/// Write a Python tuple as a BSON array directly to bytes
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

/// Write a BSON value that's already been converted
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
/// Parameters: obj, check_keys, _codec_options
#[pyfunction]
#[pyo3(signature = (obj, check_keys, _codec_options))]
fn _dict_to_bson(
    py: Python,
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    _codec_options: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    let codec_options = Some(_codec_options);

    // COPILOT POC APPROACH: Use python_mapping_to_bson_doc for better performance
    // This uses items() method and efficient tuple extraction
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

    // Use to_writer() to write directly to buffer (like Copilot POC)
    // This is faster than bson::to_vec() which creates an intermediate Vec
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)
        .map_err(|e| invalid_document_error(py, format!("Failed to serialize BSON: {}", e)))?;

    Ok(PyBytes::new(py, &buf).into())
}

/// Read a BSON document directly from bytes and convert to Python dict
/// This bypasses the intermediate Document structure for better performance
fn read_document_from_bytes(
    py: Python,
    bytes: &[u8],
    offset: usize,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    read_document_from_bytes_with_parent(py, bytes, offset, codec_options, None)
}

/// Read a BSON document with optional parent field name for error reporting
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
/// Returns (value, new_position)
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

            // Import Int64 class and create an instance
            let int64_module = py.import("bson.int64")?;
            let int64_class = int64_module.getattr("Int64")?;
            let int64_obj = int64_class.call1((value,))?;

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

/// Read a BSON array from bytes
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
/// This is used to provide better error messages when an unknown type is in an array
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
/// Parameters: data, _codec_options
#[pyfunction]
#[pyo3(signature = (data, _codec_options))]
fn _bson_to_dict(
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
/// COPILOT POC APPROACH: Efficient tuple extraction
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
/// HYBRID APPROACH: Fast path for PyDict, Copilot POC approach for other mappings
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
    // Use Copilot POC approach with items() method
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
/// This is optimized for the common case of dict iteration
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

/// Extract a single item from a mapping's items() iterator and return (key, value)
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

/// Convert a Python object to a BSON value
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
    if let Some(uuid_class) = TYPE_CACHE.get_uuid_class(py) {
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
    if let Some(pattern_class) = TYPE_CACHE.get_pattern_class(py) {
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
    if let Some(datetime_class) = TYPE_CACHE.get_datetime_class(py) {
        if obj.is_instance(&datetime_class.bind(py))? {
            // Convert Python datetime to milliseconds since epoch (inline)
            let millis = datetime_to_millis(py, &obj)?;
            return Ok(Bson::DateTime(bson::DateTime::from_millis(millis)));
        }
    }

    // Handle remaining Python types (bytes, lists, dicts)
    handle_remaining_python_types(obj, check_keys, codec_options)
}

/// Handle BSON types with _type_marker attribute
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

/// Handle remaining Python types (list, dict, bytes) after fast-path checks
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

/// Convert a BSON Document to a Python dictionary
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
/// This is used by the fast-path decoder
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

    // Import DBRef class
    let bson_module = py.import("bson.dbref")?;
    let dbref_class = bson_module.getattr("DBRef")?;

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
        let dbref = dbref_class.call((collection_str, id_obj, database_str), Some(&kwargs))?;
        return Ok(dbref.into());
    }

    let dbref = dbref_class.call((collection_str, id_obj), Some(&kwargs))?;
    Ok(dbref.into())
}

/// Decode a DBRef document
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

    // Import DBRef class
    let bson_module = py.import("bson.dbref")?;
    let dbref_class = bson_module.getattr("DBRef")?;

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
        let dbref = dbref_class.call((collection, id_py, database), Some(&kwargs))?;
        Ok(dbref.into())
    } else {
        let dbref = dbref_class.call((collection, id_py), Some(&kwargs))?;
        Ok(dbref.into())
    }
}

/// Convert a BSON value to a Python object
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
            let int64_module = py.import("bson.int64")?;
            let int64_class = int64_module.getattr("Int64")?;
            let int64_obj = int64_class.call1((*v,))?;
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
            // Import ObjectId class from bson.objectid
            let bson_module = py.import("bson.objectid")?;
            let objectid_class = bson_module.getattr("ObjectId")?;

            // Create ObjectId from bytes
            let bytes = PyBytes::new(py, &v.bytes());
            let objectid = objectid_class.call1((bytes,))?;
            Ok(objectid.into())
        }
        Bson::DateTime(v) => decode_datetime(py, v, codec_options),
        Bson::RegularExpression(v) => {
            // Import Regex class from bson.regex
            let bson_module = py.import("bson.regex")?;
            let regex_class = bson_module.getattr("Regex")?;

            // Convert BSON regex options to Python flags
            let flags = str_flags_to_int(&v.options);

            // Create Regex(pattern, flags)
            let regex = regex_class.call1((v.pattern.clone(), flags))?;
            Ok(regex.into())
        }
        Bson::JavaScriptCode(v) => {
            // Import Code class from bson.code
            let bson_module = py.import("bson.code")?;
            let code_class = bson_module.getattr("Code")?;

            // Create Code(code)
            let code = code_class.call1((v,))?;
            Ok(code.into())
        }
        Bson::JavaScriptCodeWithScope(v) => {
            // Import Code class from bson.code
            let bson_module = py.import("bson.code")?;
            let code_class = bson_module.getattr("Code")?;

            // Convert scope to Python dict
            let scope_dict = bson_doc_to_python_dict(py, &v.scope, codec_options)?;

            // Create Code(code, scope)
            let code = code_class.call1((v.code.clone(), scope_dict))?;
            Ok(code.into())
        }
        Bson::Timestamp(v) => {
            // Import Timestamp class from bson.timestamp
            let bson_module = py.import("bson.timestamp")?;
            let timestamp_class = bson_module.getattr("Timestamp")?;

            // Create Timestamp(time, inc)
            let timestamp = timestamp_class.call1((v.time, v.increment))?;
            Ok(timestamp.into())
        }
        Bson::Decimal128(v) => {
            // Import Decimal128 class from bson.decimal128
            let bson_module = py.import("bson.decimal128")?;
            let decimal128_class = bson_module.getattr("Decimal128")?;

            // Create Decimal128 from bytes
            let bytes = PyBytes::new(py, &v.bytes());

            // Use from_bid class method
            let decimal128 = decimal128_class.call_method1("from_bid", (bytes,))?;
            Ok(decimal128.into())
        }
        Bson::MaxKey => {
            // Import MaxKey class from bson.max_key
            let bson_module = py.import("bson.max_key")?;
            let maxkey_class = bson_module.getattr("MaxKey")?;

            // Create MaxKey instance
            let maxkey = maxkey_class.call0()?;
            Ok(maxkey.into())
        }
        Bson::MinKey => {
            // Import MinKey class from bson.min_key
            let bson_module = py.import("bson.min_key")?;
            let minkey_class = bson_module.getattr("MinKey")?;

            // Create MinKey instance
            let minkey = minkey_class.call0()?;
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

                // Import DBRef class from bson.dbref
                let bson_module = py.import("bson.dbref")?;
                let dbref_class = bson_module.getattr("DBRef")?;

                // Import ObjectId class from bson.objectid
                let objectid_module = py.import("bson.objectid")?;
                let objectid_class = objectid_module.getattr("ObjectId")?;

                // Create ObjectId from hex string
                let objectid = objectid_class.call1((oid_hex,))?;

                // Create DBRef(collection, id)
                let dbref = dbref_class.call1((namespace, objectid))?;
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

/// Decode BSON Binary to Python Binary or UUID
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
            // Decode as UUID
            let uuid_module = py.import("uuid")?;
            let uuid_class = uuid_module.getattr("UUID")?;
            let bytes_obj = PyBytes::new(py, &v.bytes);
            let kwargs = [("bytes", bytes_obj)].into_py_dict(py)?;
            let uuid_obj = uuid_class.call((), Some(&kwargs))?;
            return Ok(uuid_obj.into());
        }
    }

    if subtype == 0 {
        Ok(PyBytes::new(py, &v.bytes).into())
    } else {
        // Import Binary class from bson.binary
        let bson_module = py.import("bson.binary")?;
        let binary_class = bson_module.getattr("Binary")?;

        // Create Binary(data, subtype)
        let bytes = PyBytes::new(py, &v.bytes);
        let binary = binary_class.call1((bytes, subtype))?;
        Ok(binary.into())
    }
}

/// Decode BSON DateTime to Python datetime
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
        let datetime_ms_module = py.import("bson.datetime_ms")?;
        let datetime_ms_class = datetime_ms_module.getattr("DatetimeMS")?;
        let datetime_ms = datetime_ms_class.call1((millis,))?;
        return Ok(datetime_ms.into());
    }

    // If DATETIME_AUTO (4) and out of range, return DatetimeMS
    if datetime_conversion == 4 && is_out_of_range {
        let datetime_ms_module = py.import("bson.datetime_ms")?;
        let datetime_ms_class = datetime_ms_module.getattr("DatetimeMS")?;
        let datetime_ms = datetime_ms_class.call1((millis,))?;
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

    // Convert to Python datetime
    let datetime_module = py.import("datetime")?;
    let datetime_class = datetime_module.getattr("datetime")?;

    // Convert milliseconds to seconds and microseconds
    let seconds = millis / 1000;
    let microseconds = (millis % 1000) * 1000;

    if tz_aware {
        // Return timezone-aware datetime with UTC timezone
        let utc_module = py.import("bson.tz_util")?;
        let utc = utc_module.getattr("utc")?;

        // Construct datetime from epoch using timedelta to avoid platform-specific limitations
        // This works on all platforms including Windows for dates outside fromtimestamp() range
        let epoch = datetime_class.call1((1970, 1, 1, 0, 0, 0, 0, utc))?;
        let timedelta_class = datetime_module.getattr("timedelta")?;

        // Create timedelta for seconds and microseconds
        let kwargs = [("seconds", seconds), ("microseconds", microseconds)].into_py_dict(py)?;
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
                                let datetime_min = datetime_class.getattr("min")?;
                                let datetime_max = datetime_class.getattr("max")?;

                                // Compare year to determine if we're at min or max
                                let year = dt_final.getattr("year")?.extract::<i32>()?;

                                if year == 1 {
                                    // At datetime.min, return datetime.min.replace(tzinfo=tzinfo)
                                    let kwargs = [("tzinfo", &tzinfo)].into_py_dict(py)?;
                                    let dt_with_tz = datetime_min.call_method("replace", (), Some(&kwargs))?;
                                    return Ok(dt_with_tz.into());
                                } else {
                                    // At datetime.max, return datetime.max.replace(tzinfo=tzinfo, microsecond=999000)
                                    let microsecond = 999000i32.into_py(py).into_bound(py);
                                    let kwargs = [("tzinfo", &tzinfo), ("microsecond", &microsecond)].into_py_dict(py)?;
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
        let epoch = datetime_class.call1((1970, 1, 1, 0, 0, 0, 0))?;
        let timedelta_class = datetime_module.getattr("timedelta")?;

        // Create timedelta for seconds and microseconds
        let kwargs = [("seconds", seconds), ("microseconds", microseconds)].into_py_dict(py)?;
        let delta = timedelta_class.call((), Some(&kwargs))?;
        let naive_dt = epoch.call_method1("__add__", (delta,))?;
        Ok(naive_dt.into())
    }
}

/// Python module definition
#[pymodule]
fn _rbson(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(_dict_to_bson, m)?)?;
    m.add_function(wrap_pyfunction!(_bson_to_dict, m)?)?;
    m.add_function(wrap_pyfunction!(_test_rust_extension, m)?)?;
    Ok(())
}

#![allow(clippy::useless_conversion)]

use bson::{doc, Bson, Document};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyAny, PyBool, PyBytes, PyDict, PyFloat, PyInt, PyString};
use std::io::Cursor;

// Type markers for BSON objects
const BINARY_TYPE_MARKER: i32 = 5;
const OBJECTID_TYPE_MARKER: i32 = 7;
const DATETIME_TYPE_MARKER: i32 = 9;
const REGEX_TYPE_MARKER: i32 = 11;
const TIMESTAMP_TYPE_MARKER: i32 = 17;

/// Convert Python regex flags (int) to BSON regex options (string)
fn int_flags_to_str(flags: i32) -> String {
    let mut options = String::new();

    // Python re module flags to BSON regex options:
    // re.IGNORECASE = 2 -> 'i'
    // re.MULTILINE = 8 -> 'm'
    // re.DOTALL = 16 -> 's'
    // re.VERBOSE = 64 -> 'x'
    // Note: re.LOCALE and re.UNICODE are Python-specific and
    // have no direct BSON equivalents, so they are preserved for round-trip

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

/// Encode a Python dictionary to BSON bytes
#[pyfunction]
#[pyo3(signature = (obj, check_keys=false, codec_options=None))]
fn encode_bson(
    py: Python,
    obj: &Bound<'_, PyDict>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyBytes>> {
    let doc = python_dict_to_bson_doc(obj, check_keys, codec_options)?;
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)
        .map_err(|e| PyValueError::new_err(format!("Failed to encode BSON: {}", e)))?;
    Ok(PyBytes::new(py, &buf).unbind())
}

/// Decode BSON bytes to a Python dictionary
#[pyfunction]
#[pyo3(signature = (data, codec_options=None))]
fn decode_bson(
    py: Python,
    data: &[u8],
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let cursor = Cursor::new(data);
    let doc = Document::from_reader(cursor)
        .map_err(|e| PyValueError::new_err(format!("Failed to decode BSON: {}", e)))?;
    bson_doc_to_python_dict(py, &doc, codec_options)
}

/// Convert a Python dictionary to a BSON Document
fn python_dict_to_bson_doc(
    dict: &Bound<'_, PyDict>,
    check_keys: bool,
    _codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Document> {
    let mut doc = Document::new();

    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;

        // Check keys if requested
        if check_keys {
            if key_str.starts_with('$') {
                return Err(PyValueError::new_err(format!(
                    "key '{}' must not start with '$'",
                    key_str
                )));
            }
            if key_str.contains('.') {
                return Err(PyValueError::new_err(format!(
                    "key '{}' must not contain '.'",
                    key_str
                )));
            }
        }

        let bson_value = python_to_bson(value, check_keys, _codec_options)?;
        doc.insert(key_str, bson_value);
    }

    Ok(doc)
}

/// Convert a Python object to a BSON value
fn python_to_bson(
    obj: Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bson> {
    // Check for Python datetime objects first (before checking type_marker)
    // datetime.datetime has module 'datetime' and type 'datetime'
    if let Ok(type_obj) = obj.get_type().getattr("__module__") {
        if let Ok(module_name) = type_obj.extract::<String>() {
            if module_name == "datetime" {
                if let Ok(type_name) = obj.get_type().getattr("__name__") {
                    if let Ok(name) = type_name.extract::<String>() {
                        if name == "datetime" {
                            // Convert Python datetime to milliseconds since epoch
                            let py = obj.py();
                            let datetime_ms_module = py.import("bson.datetime_ms")?;
                            let datetime_to_millis =
                                datetime_ms_module.getattr("_datetime_to_millis")?;
                            let millis: i64 = datetime_to_millis.call1((obj,))?.extract()?;
                            return Ok(Bson::DateTime(bson::DateTime::from_millis(millis)));
                        }
                    }
                }
            }
        }
    }

    // Check if this is a BSON type with a _type_marker
    if let Ok(type_marker) = obj.getattr("_type_marker") {
        if let Ok(marker) = type_marker.extract::<i32>() {
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

                    return Ok(Bson::Binary(bson::Binary {
                        subtype: bson_subtype,
                        bytes,
                    }));
                }
                OBJECTID_TYPE_MARKER => {
                    // ObjectId object - get the binary representation
                    let binary: Vec<u8> = obj.getattr("binary")?.extract()?;
                    if binary.len() != 12 {
                        return Err(PyValueError::new_err("ObjectId must be 12 bytes"));
                    }
                    let mut oid_bytes = [0u8; 12];
                    oid_bytes.copy_from_slice(&binary);
                    return Ok(Bson::ObjectId(bson::oid::ObjectId::from_bytes(oid_bytes)));
                }
                DATETIME_TYPE_MARKER => {
                    // DateTime/DatetimeMS object - get milliseconds since epoch
                    // Try to get the _value attribute (for DatetimeMS)
                    if let Ok(value) = obj.getattr("_value") {
                        let millis: i64 = value.extract()?;
                        return Ok(Bson::DateTime(bson::DateTime::from_millis(millis)));
                    }
                    return Err(PyValueError::new_err(
                        "DateTime object must have _value attribute",
                    ));
                }
                REGEX_TYPE_MARKER => {
                    // Regex object
                    let pattern: String = obj.getattr("pattern")?.extract()?;
                    let flags_obj = obj.getattr("flags")?;

                    // Flags can be an int or a string
                    let flags_str = if let Ok(flags_int) = flags_obj.extract::<i32>() {
                        // Convert Python regex flags to BSON regex flags
                        int_flags_to_str(flags_int)
                    } else {
                        flags_obj.extract::<String>().unwrap_or_default()
                    };

                    return Ok(Bson::RegularExpression(bson::Regex {
                        pattern,
                        options: flags_str,
                    }));
                }
                TIMESTAMP_TYPE_MARKER => {
                    // Timestamp object
                    let time: u32 = obj.getattr("time")?.extract()?;
                    let inc: u32 = obj.getattr("inc")?.extract()?;
                    return Ok(Bson::Timestamp(bson::Timestamp {
                        time,
                        increment: inc,
                    }));
                }
                _ => {
                    // Unknown type marker, fall through to normal conversion
                }
            }
        }
    }

    if obj.is_none() {
        Ok(Bson::Null)
    } else if let Ok(v) = obj.extract::<bool>() {
        Ok(Bson::Boolean(v))
    } else if let Ok(v) = obj.extract::<i32>() {
        Ok(Bson::Int32(v))
    } else if let Ok(v) = obj.extract::<i64>() {
        Ok(Bson::Int64(v))
    } else if let Ok(v) = obj.extract::<f64>() {
        Ok(Bson::Double(v))
    } else if let Ok(v) = obj.extract::<String>() {
        Ok(Bson::String(v))
    } else if let Ok(v) = obj.extract::<Vec<u8>>() {
        // Raw bytes without Binary wrapper -> subtype 0
        Ok(Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: v,
        }))
    } else if let Ok(dict) = obj.cast::<PyDict>() {
        let doc = python_dict_to_bson_doc(dict, check_keys, codec_options)?;
        Ok(Bson::Document(doc))
    } else if let Ok(list) = obj.extract::<Vec<Bound<'_, PyAny>>>() {
        let mut arr = Vec::new();
        for item in list {
            arr.push(python_to_bson(item, check_keys, codec_options)?);
        }
        Ok(Bson::Array(arr))
    } else {
        Err(PyValueError::new_err(format!(
            "Unsupported Python type for BSON conversion: {:?}",
            obj.get_type().name()
        )))
    }
}

/// Convert a BSON Document to a Python dictionary
fn bson_doc_to_python_dict(
    py: Python,
    doc: &Document,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);

    for (key, value) in doc {
        let py_value = bson_to_python(py, value, codec_options)?;
        dict.set_item(key, py_value)?;
    }

    Ok(dict.into())
}

/// Convert a BSON value to a Python object
fn bson_to_python(
    py: Python,
    bson: &Bson,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    match bson {
        Bson::Null => Ok(py.None()),
        Bson::Boolean(v) => Ok(PyBool::new(py, *v).to_owned().into_any().unbind()),
        Bson::Int32(v) => Ok(PyInt::new(py, *v as i64).into_any().clone().unbind()),
        Bson::Int64(v) => Ok(PyInt::new(py, *v).into_any().clone().unbind()),
        Bson::Double(v) => Ok(PyFloat::new(py, *v).into_any().clone().unbind()),
        Bson::String(v) => Ok(PyString::new(py, v).into_any().clone().unbind()),
        Bson::Binary(v) => {
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
                bson::spec::BinarySubtype::Reserved(s) => *s, // Subtypes 10-127
                bson::spec::BinarySubtype::UserDefined(s) => *s, // Subtypes 128-255
                // For any unknown/future subtypes added to the BSON spec
                // Note: This should rarely be hit with the current BSON specification
                _ => {
                    return Err(PyValueError::new_err(
                        "Encountered unknown binary subtype that cannot be converted",
                    ));
                }
            };

            // Binary decoding rules per BSON spec:
            // - Subtype 0 (Generic) is decoded as plain bytes (Python's bytes type)
            // - All other subtypes are decoded as Binary objects to preserve type information
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
        Bson::DateTime(v) => {
            // Convert to Python datetime with UTC timezone
            let datetime_module = py.import("datetime")?;
            let utc_module = py.import("bson.tz_util")?;
            let utc = utc_module.getattr("utc")?;

            // Get milliseconds and convert to seconds and microseconds
            let millis = v.timestamp_millis();
            let seconds = millis / 1000;
            let microseconds = (millis % 1000) * 1000;

            // Use datetime.fromtimestamp(seconds, tz=utc) to create datetime directly in UTC
            let datetime_class = datetime_module.getattr("datetime")?;
            let kwargs = [("tz", utc)].into_py_dict(py)?;
            let dt = datetime_class.call_method("fromtimestamp", (seconds,), Some(&kwargs))?;

            // Add microseconds if needed
            if microseconds != 0 {
                let timedelta_class = datetime_module.getattr("timedelta")?;
                let kwargs = [("microseconds", microseconds)].into_py_dict(py)?;
                let delta = timedelta_class.call((), Some(&kwargs))?;
                let dt_with_micros = dt.call_method1("__add__", (delta,))?;
                Ok(dt_with_micros.into())
            } else {
                Ok(dt.into())
            }
        }
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
        Bson::Timestamp(v) => {
            // Import Timestamp class from bson.timestamp
            let bson_module = py.import("bson.timestamp")?;
            let timestamp_class = bson_module.getattr("Timestamp")?;

            // Create Timestamp(time, inc)
            let timestamp = timestamp_class.call1((v.time, v.increment))?;
            Ok(timestamp.into())
        }
        _ => Err(PyValueError::new_err(format!(
            "Unsupported BSON type for Python conversion: {:?}",
            bson
        ))),
    }
}

/// Create a simple test document
fn create_simple_doc() -> Document {
    doc! {
        "name": "John Doe",
        "age": 30,
        "active": true,
        "score": 95.5,
    }
}

/// Create a complex nested test document
fn create_complex_doc() -> Document {
    doc! {
        "user": {
            "name": "John Doe",
            "age": 30,
            "email": "john@example.com",
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "state": "NY",
                "zip": "10001"
            }
        },
        "orders": [
            {
                "id": 1,
                "total": 99.99,
                "items": ["item1", "item2", "item3"]
            },
            {
                "id": 2,
                "total": 149.99,
                "items": ["item4", "item5"]
            }
        ],
        "metadata": {
            "created": "2024-01-01",
            "updated": "2024-01-15",
            "version": 2
        }
    }
}

/// Benchmark: Encode a simple document multiple times
#[pyfunction]
fn benchmark_encode_simple(iterations: usize) -> PyResult<f64> {
    use std::time::Instant;

    let doc = create_simple_doc();

    let start = Instant::now();
    for _ in 0..iterations {
        let mut buf = Vec::new();
        doc.to_writer(&mut buf)
            .map_err(|e| PyValueError::new_err(format!("Encode failed: {}", e)))?;
    }
    let duration = start.elapsed();

    Ok(duration.as_secs_f64())
}

/// Benchmark: Decode a simple document multiple times
#[pyfunction]
fn benchmark_decode_simple(iterations: usize) -> PyResult<f64> {
    use std::time::Instant;

    let doc = create_simple_doc();

    let mut buf = Vec::new();
    doc.to_writer(&mut buf)
        .map_err(|e| PyValueError::new_err(format!("Encode failed: {}", e)))?;

    let start = Instant::now();
    for _ in 0..iterations {
        let cursor = Cursor::new(&buf);
        let _decoded = Document::from_reader(cursor)
            .map_err(|e| PyValueError::new_err(format!("Decode failed: {}", e)))?;
    }
    let duration = start.elapsed();

    Ok(duration.as_secs_f64())
}

/// Benchmark: Encode a complex nested document multiple times
#[pyfunction]
fn benchmark_encode_complex(iterations: usize) -> PyResult<f64> {
    use std::time::Instant;

    let doc = create_complex_doc();

    let start = Instant::now();
    for _ in 0..iterations {
        let mut buf = Vec::new();
        doc.to_writer(&mut buf)
            .map_err(|e| PyValueError::new_err(format!("Encode failed: {}", e)))?;
    }
    let duration = start.elapsed();

    Ok(duration.as_secs_f64())
}

/// Benchmark: Decode a complex nested document multiple times
#[pyfunction]
fn benchmark_decode_complex(iterations: usize) -> PyResult<f64> {
    use std::time::Instant;

    let doc = create_complex_doc();

    let mut buf = Vec::new();
    doc.to_writer(&mut buf)
        .map_err(|e| PyValueError::new_err(format!("Encode failed: {}", e)))?;

    let start = Instant::now();
    for _ in 0..iterations {
        let cursor = Cursor::new(&buf);
        let _decoded = Document::from_reader(cursor)
            .map_err(|e| PyValueError::new_err(format!("Decode failed: {}", e)))?;
    }
    let duration = start.elapsed();

    Ok(duration.as_secs_f64())
}

/// A Python module implemented in Rust.
#[pymodule]
fn pymongo_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_bson, m)?)?;
    m.add_function(wrap_pyfunction!(decode_bson, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_encode_simple, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_decode_simple, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_encode_complex, m)?)?;
    m.add_function(wrap_pyfunction!(benchmark_decode_complex, m)?)?;
    Ok(())
}

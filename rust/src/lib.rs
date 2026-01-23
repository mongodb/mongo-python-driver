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
const CODE_TYPE_MARKER: i32 = 13;
#[allow(dead_code)]
const SYMBOL_TYPE_MARKER: i32 = 14;
#[allow(dead_code)]
const DBPOINTER_TYPE_MARKER: i32 = 15;
const TIMESTAMP_TYPE_MARKER: i32 = 17;
const DECIMAL128_TYPE_MARKER: i32 = 19;
const MAXKEY_TYPE_MARKER: i32 = 127;
const MINKEY_TYPE_MARKER: i32 = 255;

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
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyBytes>> {
    let doc = python_mapping_to_bson_doc(obj, check_keys, codec_options)?;
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
    data: &Bound<'_, PyAny>,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // Accept both bytes and bytearray
    let bytes = if let Ok(b) = data.extract::<&[u8]>() {
        b
    } else {
        return Err(PyValueError::new_err("data must be bytes or bytearray"));
    };
    
    let cursor = Cursor::new(bytes);
    let doc = Document::from_reader(cursor)
        .map_err(|e| PyValueError::new_err(format!("Failed to decode BSON: {}", e)))?;
    bson_doc_to_python_dict(py, &doc, codec_options)
}

/// Convert a Python mapping (dict, SON, OrderedDict, etc.) to a BSON Document
fn python_mapping_to_bson_doc(
    obj: &Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Document> {
    let mut doc = Document::new();
    let mut has_id = false;
    let mut id_value: Option<Bson> = None;

    // Try to get items() method for mapping protocol
    if let Ok(items_method) = obj.getattr("items") {
        if let Ok(items_result) = items_method.call0() {
            // Try to cast to PyList or PyTuple first for efficient iteration
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
            
            // Insert _id first if present
            if has_id {
                if let Some(id_val) = id_value {
                    let mut new_doc = Document::new();
                    new_doc.insert("_id", id_val);
                    for (k, v) in doc {
                        new_doc.insert(k, v);
                    }
                    return Ok(new_doc);
                }
            }
            
            return Ok(doc);
        }
    }

    Err(PyValueError::new_err(
        "Object must be a dict or have an items() method",
    ))
}

/// Process a single item from a mapping's items() iterator
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

    // Convert key to string (support bytes keys)
    let key_str: String = if let Ok(s) = key.extract::<String>() {
        s
    } else if let Ok(b) = key.extract::<Vec<u8>>() {
        String::from_utf8(b)
            .map_err(|e| PyValueError::new_err(format!("Invalid UTF-8 in bytes key: {}", e)))?
    } else {
        return Err(PyValueError::new_err(format!(
            "Dictionary keys must be strings or bytes, got {}",
            key.get_type().name()?
        )));
    };

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

    let bson_value = python_to_bson(value, check_keys, codec_options)?;

    // Store _id field separately to insert first
    if key_str == "_id" {
        *has_id = true;
        *id_value = Some(bson_value);
    } else {
        doc.insert(key_str, bson_value);
    }

    Ok(())
}

/// Convert a Python object to a BSON value
fn python_to_bson(
    obj: Bound<'_, PyAny>,
    check_keys: bool,
    codec_options: Option<&Bound<'_, PyAny>>,
) -> PyResult<Bson> {
    let py = obj.py();
    
    // Check for Python UUID objects (uuid.UUID)
    if let Ok(type_obj) = obj.get_type().getattr("__module__") {
        if let Ok(module_name) = type_obj.extract::<String>() {
            if module_name == "uuid" {
                if let Ok(type_name) = obj.get_type().getattr("__name__") {
                    if let Ok(name) = type_name.extract::<String>() {
                        if name == "UUID" {
                            // Convert UUID to Binary with subtype 4 (or 3 based on codec_options)
                            let uuid_bytes: Vec<u8> = obj.getattr("bytes")?.extract()?;
                            return Ok(Bson::Binary(bson::Binary {
                                subtype: bson::spec::BinarySubtype::Uuid,
                                bytes: uuid_bytes,
                            }));
                        }
                    }
                }
            }
            
            // Check for compiled regex Pattern objects
            // Pattern type name can be 'Pattern', 'Pattern[str]', 'Pattern[bytes]' depending on Python version
            if module_name == "re" || module_name == "re._parser" {
                if let Ok(type_name) = obj.get_type().getattr("__name__") {
                    if let Ok(name) = type_name.extract::<String>() {
                        if name.starts_with("Pattern") {
                            // Extract pattern and flags from re.Pattern
                            // Use hasattr to be extra safe
                            if obj.hasattr("pattern")? && obj.hasattr("flags")? {
                                let pattern: String = obj.getattr("pattern")?.extract()?;
                                let flags: i32 = obj.getattr("flags")?.extract()?;
                                let flags_str = int_flags_to_str(flags);
                                return Ok(Bson::RegularExpression(bson::Regex {
                                    pattern,
                                    options: flags_str,
                                }));
                            }
                        }
                    }
                }
            }
            
            // Check for Python datetime objects (before checking type_marker)
            // datetime.datetime has module 'datetime' and type 'datetime'
            if module_name == "datetime" {
                if let Ok(type_name) = obj.get_type().getattr("__name__") {
                    if let Ok(name) = type_name.extract::<String>() {
                        if name == "datetime" {
                            // Convert Python datetime to milliseconds since epoch
                            let datetime_ms_module = py.import("bson.datetime_ms")?;
                            let datetime_to_millis =
                                datetime_ms_module.getattr("_datetime_to_millis")?;
                            let millis: i64 = datetime_to_millis.call1((obj.clone(),))?.extract()?;
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
                CODE_TYPE_MARKER => {
                    // Code object - inherits from str
                    // Get the string value (which is the code itself)
                    let code_str: String = obj.extract()?;
                    
                    // Check if there's a scope
                    if let Ok(scope_obj) = obj.getattr("scope") {
                        if !scope_obj.is_none() {
                            // Code with scope
                            let scope_doc = python_mapping_to_bson_doc(&scope_obj, check_keys, codec_options)?;
                            return Ok(Bson::JavaScriptCodeWithScope(bson::JavaScriptCodeWithScope {
                                code: code_str,
                                scope: scope_doc,
                            }));
                        }
                    }
                    
                    // Code without scope
                    return Ok(Bson::JavaScriptCode(code_str));
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
                DECIMAL128_TYPE_MARKER => {
                    // Decimal128 object
                    // Get the bytes representation
                    let bid: Vec<u8> = obj.getattr("bid")?.extract()?;
                    if bid.len() != 16 {
                        return Err(PyValueError::new_err("Decimal128 must be 16 bytes"));
                    }
                    let mut bytes = [0u8; 16];
                    bytes.copy_from_slice(&bid);
                    return Ok(Bson::Decimal128(bson::Decimal128::from_bytes(bytes)));
                }
                MAXKEY_TYPE_MARKER => {
                    // MaxKey object
                    return Ok(Bson::MaxKey);
                }
                MINKEY_TYPE_MARKER => {
                    // MinKey object
                    return Ok(Bson::MinKey);
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
    } else if obj.hasattr("items")? {
        // Any object with items() method (dict, SON, OrderedDict, etc.)
        let doc = python_mapping_to_bson_doc(&obj, check_keys, codec_options)?;
        Ok(Bson::Document(doc))
    } else if let Ok(list) = obj.extract::<Vec<Bound<'_, PyAny>>>() {
        // Check for sequences (lists, tuples) before bytes
        // This must come before Vec<u8> check because tuples of ints can be extracted as Vec<u8>
        let mut arr = Vec::new();
        for item in list {
            arr.push(python_to_bson(item, check_keys, codec_options)?);
        }
        Ok(Bson::Array(arr))
    } else if let Ok(v) = obj.extract::<Vec<u8>>() {
        // Raw bytes without Binary wrapper -> subtype 0
        // This check must come AFTER sequence check to avoid treating tuples as bytes
        Ok(Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: v,
        }))
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
        Bson::Int32(v) => Ok(PyInt::new(py, *v as i64).into_any().unbind()),
        Bson::Int64(v) => {
            // Return bson.int64.Int64 object instead of plain Python int
            let int64_module = py.import("bson.int64")?;
            let int64_class = int64_module.getattr("Int64")?;
            let int64_obj = int64_class.call1((*v,))?;
            Ok(int64_obj.into())
        }
        Bson::Double(v) => Ok(PyFloat::new(py, *v).into_any().unbind()),
        Bson::String(v) => Ok(PyString::new(py, v).into_any().unbind()),
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
            // - Subtypes 3 and 4 (UUID) should be decoded as UUID objects by default
            // - All other subtypes are decoded as Binary objects to preserve type information
            
            // Check for UUID subtypes (3 and 4)
            if subtype == 3 || subtype == 4 {
                // Check codec_options for UUID representation setting
                // PyMongo's UuidRepresentation enum values:
                // UNSPECIFIED = 0, PYTHON_LEGACY = 1, JAVA_LEGACY = 2, CSHARP_LEGACY = 3, STANDARD = 4
                // When uuid_representation is UNSPECIFIED (0), we should decode as Binary
                // For other values, decode as UUID
                let should_decode_as_uuid = if let Some(opts) = codec_options {
                    if let Ok(uuid_rep) = opts.getattr("uuid_representation") {
                        if let Ok(rep_value) = uuid_rep.extract::<i32>() {
                            // Decode as UUID if representation is not UNSPECIFIED (0)
                            rep_value != 0
                        } else {
                            // If we can't extract as int, default to UUID
                            true
                        }
                    } else {
                        // No uuid_representation attribute, default to UUID
                        true
                    }
                } else {
                    // No codec_options, default to UUID for subtypes 3 and 4
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
            
            // Get milliseconds and convert to seconds and microseconds
            let millis = v.timestamp_millis();
            let seconds = millis / 1000;
            let microseconds = (millis % 1000) * 1000;
            
            if tz_aware {
                // Return timezone-aware datetime with UTC timezone
                let utc_module = py.import("bson.tz_util")?;
                let utc = utc_module.getattr("utc")?;
                
                // Use datetime.fromtimestamp(seconds, tz=utc) to create datetime directly in UTC
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
            } else {
                // Return naive datetime (no timezone)
                // Note: utcfromtimestamp is deprecated in Python 3.12+
                // Use fromtimestamp with UTC then remove tzinfo for compatibility
                let timezone_module = py.import("datetime")?;
                let timezone_class = timezone_module.getattr("timezone")?;
                let utc = timezone_class.getattr("utc")?;
                
                let kwargs = [("tz", utc)].into_py_dict(py)?;
                let dt = datetime_class.call_method("fromtimestamp", (seconds,), Some(&kwargs))?;
                
                // Remove timezone to make it naive
                let kwargs = [("tzinfo", py.None())].into_py_dict(py)?;
                let naive_dt = dt.call_method("replace", (), Some(&kwargs))?;
                
                // Add microseconds if needed
                if microseconds != 0 {
                    let timedelta_class = datetime_module.getattr("timedelta")?;
                    let kwargs = [("microseconds", microseconds)].into_py_dict(py)?;
                    let delta = timedelta_class.call((), Some(&kwargs))?;
                    let dt_with_micros = naive_dt.call_method1("__add__", (delta,))?;
                    Ok(dt_with_micros.into())
                } else {
                    Ok(naive_dt.into())
                }
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
            // Return it as a string for now (PyMongo also does this in some cases)
            // Or we could import bson.son.SON and create a proper Symbol
            Ok(PyString::new(py, v).into_any().unbind())
        }
        Bson::Undefined => {
            // Import Undefined class from bson (if it exists)
            // For now, return None as undefined is deprecated
            Ok(py.None())
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

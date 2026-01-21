use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyAny};
use pyo3::exceptions::PyValueError;
use bson::{doc, Document, Bson};
use std::io::Cursor;

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
    Ok(PyBytes::new_bound(py, &buf).unbind())
}

/// Decode BSON bytes to a Python dictionary
#[pyfunction]
#[pyo3(signature = (data, codec_options=None))]
fn decode_bson(py: Python, data: &[u8], codec_options: Option<&Bound<'_, PyAny>>) -> PyResult<PyObject> {
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
    // Check if this is a Binary object (has _type_marker == 5)
    if let Ok(type_marker) = obj.getattr("_type_marker") {
        if let Ok(marker) = type_marker.extract::<i32>() {
            if marker == 5 {
                // This is a Binary object
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
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
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
) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    
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
) -> PyResult<PyObject> {
    match bson {
        Bson::Null => Ok(py.None()),
        Bson::Boolean(v) => Ok(v.to_object(py)),
        Bson::Int32(v) => Ok(v.to_object(py)),
        Bson::Int64(v) => Ok(v.to_object(py)),
        Bson::Double(v) => Ok(v.to_object(py)),
        Bson::String(v) => Ok(v.to_object(py)),
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
                bson::spec::BinarySubtype::Reserved(s) => *s,  // Subtypes 10-127
                bson::spec::BinarySubtype::UserDefined(s) => *s,  // Subtypes 128-255
                // For any unknown/future subtypes, try to convert to u8
                _ => {
                    // This should not happen with current BSON spec, but handle it gracefully
                    eprintln!("Warning: Unknown binary subtype encountered");
                    0u8
                }
            };
            
            // Subtype 0 is decoded as bytes, others as Binary objects
            if subtype == 0 {
                Ok(PyBytes::new_bound(py, &v.bytes).into())
            } else {
                // Import Binary class from bson.binary
                let bson_module = py.import_bound("bson.binary")?;
                let binary_class = bson_module.getattr("Binary")?;
                
                // Create Binary(data, subtype)
                let bytes = PyBytes::new_bound(py, &v.bytes);
                let binary = binary_class.call1((bytes, subtype))?;
                Ok(binary.into())
            }
        }
        Bson::Document(v) => bson_doc_to_python_dict(py, v, codec_options),
        Bson::Array(v) => {
            let list = pyo3::types::PyList::empty_bound(py);
            for item in v {
                list.append(bson_to_python(py, item, codec_options)?)?;
            }
            Ok(list.into())
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

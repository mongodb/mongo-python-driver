use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3::exceptions::PyValueError;
use bson::{doc, Document, Bson};
use std::io::Cursor;

/// Encode a Python dictionary to BSON bytes
#[pyfunction]
fn encode_bson(py: Python, obj: &Bound<'_, PyDict>) -> PyResult<Py<PyBytes>> {
    let doc = python_dict_to_bson_doc(obj)?;
    let mut buf = Vec::new();
    doc.to_writer(&mut buf)
        .map_err(|e| PyValueError::new_err(format!("Failed to encode BSON: {}", e)))?;
    Ok(PyBytes::new_bound(py, &buf).unbind())
}

/// Decode BSON bytes to a Python dictionary
#[pyfunction]
fn decode_bson(py: Python, data: &[u8]) -> PyResult<PyObject> {
    let cursor = Cursor::new(data);
    let doc = Document::from_reader(cursor)
        .map_err(|e| PyValueError::new_err(format!("Failed to decode BSON: {}", e)))?;
    bson_doc_to_python_dict(py, &doc)
}

/// Convert a Python dictionary to a BSON Document
fn python_dict_to_bson_doc(dict: &Bound<'_, PyDict>) -> PyResult<Document> {
    let mut doc = Document::new();
    
    for (key, value) in dict.iter() {
        let key_str: String = key.extract()?;
        let bson_value = python_to_bson(value)?;
        doc.insert(key_str, bson_value);
    }
    
    Ok(doc)
}

/// Convert a Python object to a BSON value
fn python_to_bson(obj: Bound<'_, PyAny>) -> PyResult<Bson> {
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
        Ok(Bson::Binary(bson::Binary {
            subtype: bson::spec::BinarySubtype::Generic,
            bytes: v,
        }))
    } else if let Ok(dict) = obj.downcast::<PyDict>() {
        let doc = python_dict_to_bson_doc(dict)?;
        Ok(Bson::Document(doc))
    } else if let Ok(list) = obj.extract::<Vec<Bound<'_, PyAny>>>() {
        let mut arr = Vec::new();
        for item in list {
            arr.push(python_to_bson(item)?);
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
fn bson_doc_to_python_dict(py: Python, doc: &Document) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    
    for (key, value) in doc {
        let py_value = bson_to_python(py, value)?;
        dict.set_item(key, py_value)?;
    }
    
    Ok(dict.into())
}

/// Convert a BSON value to a Python object
fn bson_to_python(py: Python, bson: &Bson) -> PyResult<PyObject> {
    match bson {
        Bson::Null => Ok(py.None()),
        Bson::Boolean(v) => Ok(v.to_object(py)),
        Bson::Int32(v) => Ok(v.to_object(py)),
        Bson::Int64(v) => Ok(v.to_object(py)),
        Bson::Double(v) => Ok(v.to_object(py)),
        Bson::String(v) => Ok(v.to_object(py)),
        Bson::Binary(v) => Ok(PyBytes::new_bound(py, &v.bytes).into()),
        Bson::Document(v) => bson_doc_to_python_dict(py, v),
        Bson::Array(v) => {
            let list = pyo3::types::PyList::empty_bound(py);
            for item in v {
                list.append(bson_to_python(py, item)?)?;
            }
            Ok(list.into())
        }
        _ => Err(PyValueError::new_err(format!(
            "Unsupported BSON type for Python conversion: {:?}",
            bson
        ))),
    }
}

/// Benchmark: Encode a simple document multiple times
#[pyfunction]
fn benchmark_encode_simple(iterations: usize) -> PyResult<f64> {
    use std::time::Instant;
    
    let doc = doc! {
        "name": "John Doe",
        "age": 30,
        "active": true,
        "score": 95.5,
    };
    
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
    
    let doc = doc! {
        "name": "John Doe",
        "age": 30,
        "active": true,
        "score": 95.5,
    };
    
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
    
    let doc = doc! {
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
    };
    
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
    
    let doc = doc! {
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
    };
    
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

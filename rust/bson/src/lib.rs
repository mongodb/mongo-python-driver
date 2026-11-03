//! `_rbson` — a Rust-backed BSON codec for PyMongo.
//!
//! Drop-in alternative to `bson._cbson` exposing the same six private
//! entry points with identical signatures and semantics (the C extension is
//! the behavioral reference):
//!
//! - `_dict_to_bson(document, check_keys, codec_options, top_level=True)`
//! - `_bson_to_dict(bson, codec_options)`
//! - `_decode_all(bson, codec_options)`
//! - `_element_to_dict(bson, position, obj_end, codec_options, raw_array)`
//! - `_raw_to_dict(bson, position, obj_end, codec_options, result, raw_array)`
//! - `_array_of_documents_to_buffer(array)`
//!
//! The GIL is held for the duration of every call (parity with the C
//! extension): the module declares `Py_mod_gil = Py_MOD_GIL_USED`
//! (`gil_used = true`), so free-threaded CPython builds re-enable the GIL
//! when importing it. Note that pyo3 does not declare
//! `Py_mod_multiple_interpreters`, and the import machinery defaults a
//! missing slot to SUPPORTED — so this module *can* be imported into a
//! shared-GIL subinterpreter even though its process-wide statics (`Types`,
//! the field-name cache) would then be shared across interpreters.
//! Importing the module into more than one interpreter is not supported.

mod buffer;
mod datetime_conv;
mod decode;
mod encode;
mod errors;
mod options;
mod types;

use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyList, PyModule, PyTuple};

use buffer::Buffer;
use decode::BSON_MIN_SIZE;
use errors::invalid_bson;
use options::CodecOptionsData;
use types::Types;

/// True when any document in the stream is at least `threshold` bytes
/// (mirrors `_contains_view_eligible_doc`).
fn contains_view_eligible_doc(data: &[u8], threshold: usize) -> bool {
    let mut position = 0;
    while data.len() - position >= 4 {
        let size = u32::from_le_bytes(data[position..position + 4].try_into().unwrap()) as usize;
        if size < BSON_MIN_SIZE || size > data.len() - position {
            return false;
        }
        if size >= threshold {
            return true;
        }
        position += size;
    }
    false
}

/// Prepares the input buffer owner for decoding, mirroring
/// `_prepare_input_buffer`: raw-document decoding from mutable/foreign
/// buffers copies them to bytes when the stream contains a
/// view-eligible (>= threshold) document.
///
/// The scan and copy read the exporter's memory through a transient borrow
/// that spans no Python code: `Types::get` is resolved *before* the buffer
/// is acquired (its first use imports modules and compiles a regex, and
/// that initialization could run Python that mutates the exporter), and
/// the eligibility scan itself is pure Rust. At most one copy is made,
/// straight into the returned bytes object.
fn prepare_input_buffer<'py>(
    py: Python<'py>,
    object: &Bound<'py, PyAny>,
    options: &CodecOptionsData<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    if !options.is_raw_bson
        || object.is_instance_of::<PyBytes>()
        || object.is_instance_of::<pyo3::types::PyByteArray>()
    {
        return Ok(object.clone());
    }
    let threshold = Types::get(py)?.raw_bson_view_threshold;
    let buffer = PyBuffer::<u8>::get(object)?;
    with_get_buffer_checks(&buffer)?;
    let data =
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const u8, buffer.len_bytes()) };
    if contains_view_eligible_doc(data, threshold) {
        // Copy to bytes so memoryview-backed views outlive the input
        // (mirrors PyBytes_FromObject).
        let copy = PyBytes::new(py, data);
        drop(buffer);
        Ok(copy.into_any())
    } else {
        drop(buffer);
        Ok(object.clone())
    }
}

/// Calls `f` with a `&[u8]` view of any buffer-protocol object (bytes,
/// bytearray, memoryview, mmap, ...), mirroring `_get_buffer` in the C
/// extension. Immutable bytes are borrowed zero-copy; every other exporter
/// is snapshotted to an owned copy first — its memory is writable through
/// other paths (a buffer export prevents resizing, not writes), and `f`
/// invokes decoder callbacks that can run Python code, so an immutable
/// Rust slice over that memory would be unsound.
fn with_buffer_view<R>(
    object: &Bound<'_, PyAny>,
    f: impl FnOnce(&[u8]) -> PyResult<R>,
) -> PyResult<R> {
    if let Ok(bytes) = object.cast::<PyBytes>() {
        return f(bytes.as_bytes());
    }
    let buffer = PyBuffer::<u8>::get(object)?;
    with_get_buffer_checks(&buffer)?;
    // Copy while the GIL is held and before any callback runs: nothing can
    // write through the exporter during this window, and the view is
    // dropped before `f` executes.
    let data =
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const u8, buffer.len_bytes()) }
            .to_vec();
    drop(buffer);
    f(&data)
}

/// The defensive checks of the C's `_get_buffer`: pyo3 acquires buffers
/// with PyBUF_FULL_RO (not PyBUF_SIMPLE), so a misbehaving exporter can
/// hand back non-contiguous or null buffers; mirroring the C, each is
/// rejected with a ValueError.
fn with_get_buffer_checks(buffer: &PyBuffer<u8>) -> PyResult<()> {
    // `!view->buf || view->len < 0`.
    if buffer.buf_ptr().is_null() {
        return Err(pyo3::exceptions::PyValueError::new_err("invalid buffer"));
    }
    // `!PyBuffer_IsContiguous(view, 'C')`.
    if !buffer.is_c_contiguous() {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "must be a contiguous buffer",
        ));
    }
    // `view->itemsize != 1`.
    if buffer.item_size() != 1 {
        return Err(pyo3::exceptions::PyValueError::new_err(
            "buffer data must be ascii or utf8",
        ));
    }
    Ok(())
}

/// Encodes a Python mapping to BSON bytes.
#[pyfunction]
#[pyo3(
    signature = (document, check_keys, codec_options, top_level = true),
    text_signature = "(document, check_keys, codec_options, top_level=True)"
)]
fn _dict_to_bson<'py>(
    py: Python<'py>,
    document: &Bound<'py, PyAny>,
    check_keys: bool,
    codec_options: &Bound<'py, PyAny>,
    top_level: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let options = CodecOptionsData::parse(codec_options)?;

    // A RawBSONDocument encodes to its raw bytes unconditionally (mirrors
    // the C `_cbson_dict_to_bson`, which checks the marker regardless of
    // top_level): bytes are returned as-is, anything else is converted
    // with PyBytes_FromObject.
    if let Some(marker) = options::read_type_marker(document)? {
        if marker == options::RAW_BSON_DOCUMENT_MARKER {
            let raw = document.getattr(pyo3::intern!(py, "raw"))?;
            return match raw.cast::<PyBytes>() {
                Ok(bytes) => Ok(bytes.clone()),
                Err(_) => {
                    // Copy via the buffer protocol (mirrors
                    // PyBytes_FromObject).
                    let data = with_buffer_view(&raw, |data| Ok(data.to_vec()))?;
                    Ok(PyBytes::new(py, &data))
                }
            };
        }
    }

    let bytes = encode::dict_to_bson(py, document, check_keys, &options, top_level)?;
    Ok(PyBytes::new(py, &bytes))
}

/// Decodes a single BSON document into `codec_options.document_class`.
#[pyfunction]
fn _bson_to_dict<'py>(
    py: Python<'py>,
    bson: &Bound<'py, PyAny>,
    codec_options: &Bound<'py, PyAny>,
) -> PyResult<Py<PyAny>> {
    let options = CodecOptionsData::parse(codec_options)?;
    let owner = prepare_input_buffer(py, bson, &options)?;
    with_buffer_view(&owner, |data| {
        decode::bson_to_dict(
            py,
            &decode::DecodeInput {
                data,
                owner: Some(&owner),
            },
            &options,
        )
    })
}

/// Decodes a buffer containing one or more BSON documents into a list.
#[pyfunction]
fn _decode_all<'py>(
    py: Python<'py>,
    bson: &Bound<'py, PyAny>,
    codec_options: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let options = CodecOptionsData::parse(codec_options)?;
    let owner = prepare_input_buffer(py, bson, &options)?;
    with_buffer_view(&owner, |data| {
        let input = decode::DecodeInput {
            data,
            owner: Some(&owner),
        };
        let mut position = 0;
        let results = PyList::empty(py);
        while position < data.len() {
            let slice = &data[position..];
            let size = read_size_prefix(py, slice)?;
            if size > slice.len() {
                return Err(invalid_bson(py, "objsize too large"));
            }
            // The C's _cbson_decode_all propagates elements_to_dict
            // failures untouched — the InvalidBSON wrapping happened
            // deeper, in get_value.
            let value = decode::elements_to_dict(py, &input, &slice[..size], &options)
                .map(|(value, _)| value)?;
            results.append(value)?;
            position += size;
        }
        Ok(results.into_any())
    })
}

/// Reads the 4-byte size prefix of the next document and validates it.
fn read_size_prefix(py: Python<'_>, data: &[u8]) -> PyResult<usize> {
    if data.len() < BSON_MIN_SIZE {
        return Err(invalid_bson(py, "not enough data for a BSON document"));
    }
    // The C reads the size prefix as a signed int32; negative values
    // (and zero) are "invalid message size".
    let size = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
    if size < BSON_MIN_SIZE as i32 {
        return Err(invalid_bson(py, "invalid message size"));
    }
    let size = size as usize;
    if size > data.len() {
        return Err(invalid_bson(py, "objsize too large"));
    }
    if data[size - 1] != 0 {
        return Err(invalid_bson(py, "bad eoo"));
    }
    Ok(size)
}

fn rewrap(py: Python<'_>, err: PyErr) -> PyErr {
    decode::rewrap_as_invalid_bson(py, err)
}

/// Decodes a single key/value pair at `position`, returning
/// `(name, value, new_position)`. The input must be a bytes object
/// (mirrors `_cbson_element_to_dict`).
#[pyfunction]
#[pyo3(signature = (bson, position, obj_end, codec_options, raw_array=false))]
fn _element_to_dict<'py>(
    py: Python<'py>,
    bson: &Bound<'py, PyAny>,
    position: u32,
    obj_end: u32,
    codec_options: &Bound<'py, PyAny>,
    raw_array: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let bytes = bson.cast::<PyBytes>().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err(
            "argument to _element_to_dict must be a bytes object",
        )
    })?;
    let options = CodecOptionsData::parse(codec_options)?;
    let data = bytes.as_bytes();
    let input = decode::DecodeInput {
        data,
        owner: Some(bson),
    };
    let max = obj_end as usize;
    let mut pos = position as usize;
    if pos > max || max > data.len() {
        return Err(invalid_bson(py, "bad object or element length"));
    }
    if pos >= max {
        return Err(invalid_bson(py, "field name too large"));
    }
    let type_byte = data[pos];
    pos += 1;
    let name_start = pos;
    while pos < max && data[pos] != 0 {
        pos += 1;
    }
    if pos >= max {
        return Err(invalid_bson(py, "field name too large"));
    }
    let name_bytes = &data[name_start..pos];
    pos += 1; // skip the NUL

    // The name is decoded before the value (mirrors the C's
    // _element_to_dict); a UnicodeDecodeError is wrapped in InvalidBSON.
    let name = decode::decode_key(py, name_bytes, &options).map_err(|err| rewrap(py, err))?;
    let (value, new_position) = decode::get_value_with_raw_array(
        py, &input, name_bytes, type_byte, pos, max, &options, raw_array,
    )
    .map_err(|err| rewrap(py, err))?;
    let result = PyTuple::new(
        py,
        [
            name.unbind(),
            value,
            new_position.into_pyobject(py)?.into_any().unbind(),
        ],
    )?;
    Ok(result.into_any())
}

/// Decodes all elements of a raw BSON document into an existing mapping
/// (`result`), used by RawBSONDocument's lazy inflation. Returns `result`.
#[pyfunction]
#[pyo3(signature = (bson, position, obj_end, codec_options, result_mapping, raw_array=false))]
fn _raw_to_dict<'py>(
    py: Python<'py>,
    bson: &Bound<'py, PyAny>,
    position: u32,
    obj_end: u32,
    codec_options: &Bound<'py, PyAny>,
    result_mapping: &Bound<'py, PyAny>,
    raw_array: bool,
) -> PyResult<Py<PyAny>> {
    let options = CodecOptionsData::parse(codec_options)?;
    let owner = prepare_input_buffer(py, bson, &options)?;
    with_buffer_view(&owner, |data| {
        let input = decode::DecodeInput {
            data,
            owner: Some(&owner),
        };
        // obj_end must be the index of the document's eoo byte.
        if obj_end as usize == 0
            || obj_end as usize >= data.len()
            || position > obj_end
            || data[obj_end as usize] != 0
        {
            return Err(invalid_bson(py, "bad object or element length"));
        }
        let end = obj_end as usize - 1;
        let mut pos = position as usize;
        while pos < end {
            let type_byte = data[pos];
            pos += 1;
            // The name scan is bounded by obj_end (the eoo index), not by
            // end: a no-payload final value (None, MinKey, MaxKey) puts the
            // name's NUL at obj_end - 1, the last byte before the
            // terminator (the C's _element_to_dict scans with max =
            // obj_end). The eoo check above guarantees the scan cannot run
            // past the buffer.
            let name_start = pos;
            while pos < obj_end as usize && data[pos] != 0 {
                pos += 1;
            }
            if pos >= obj_end as usize {
                return Err(invalid_bson(py, "field name too large"));
            }
            let name_bytes = &data[name_start..pos];
            pos += 1; // skip the NUL

            // The name is decoded before the value (mirrors the C); a
            // UnicodeDecodeError is wrapped in InvalidBSON.
            let name =
                decode::decode_key(py, name_bytes, &options).map_err(|err| rewrap(py, err))?;
            let (value, new_pos) = decode::get_value_with_raw_array(
                py,
                &input,
                name_bytes,
                type_byte,
                pos,
                // Per-element values are bounded by the eoo index
                // (the C passes obj_end as max to _element_to_dict).
                obj_end as usize,
                &options,
                raw_array,
            )
            .map_err(|err| rewrap(py, err))?;
            pos = new_pos;

            result_mapping.set_item(name, value)?;
        }
        if pos != obj_end as usize {
            return Err(invalid_bson(py, "bad object or element length"));
        }
        Ok(result_mapping.clone().unbind())
    })
}

/// Converts a BSON array-of-documents into a stream of concatenated BSON
/// documents (used for batched write commands).
#[pyfunction]
fn _array_of_documents_to_buffer<'py>(
    py: Python<'py>,
    array: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    with_buffer_view(array, |data| {
        if data.len() < BSON_MIN_SIZE {
            return Err(invalid_bson(py, "not enough data for a BSON document"));
        }
        let size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if size != data.len() || size < BSON_MIN_SIZE {
            return Err(invalid_bson(py, "objsize too large"));
        }
        if data[size - 1] != 0 {
            return Err(invalid_bson(py, "bad eoo"));
        }

        let mut buffer = Buffer::new();
        let mut position = 4;
        while position < size - 1 {
            // Verify the value is an embedded document.
            let type_byte = data[position];
            if type_byte != 3 {
                return Err(invalid_bson(py, "array element was not an object"));
            }
            position += 1;
            // Just skip the key.
            while position < size && data[position] != 0 {
                position += 1;
            }
            if position >= size {
                return Err(invalid_bson(py, "invalid array content"));
            }
            position += 1;
            if position >= size || size - position < BSON_MIN_SIZE {
                return Err(invalid_bson(py, "invalid array content"));
            }
            let value_length =
                u32::from_le_bytes(data[position..position + 4].try_into().unwrap()) as usize;
            if value_length < BSON_MIN_SIZE {
                return Err(invalid_bson(py, "invalid message size"));
            }
            if value_length >= size - position {
                return Err(invalid_bson(py, "invalid array content"));
            }
            buffer.write_bytes(&data[position..position + value_length])?;
            position += value_length;
        }
        if position != size - 1 {
            return Err(invalid_bson(py, "bad object or element length"));
        }
        Ok(PyBytes::new(py, &buffer.into_vec()).into_any())
    })
}

#[pymodule(gil_used = true)]
fn _rbson(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(_dict_to_bson, module)?)?;
    module.add_function(wrap_pyfunction!(_bson_to_dict, module)?)?;
    module.add_function(wrap_pyfunction!(_decode_all, module)?)?;
    module.add_function(wrap_pyfunction!(_element_to_dict, module)?)?;
    module.add_function(wrap_pyfunction!(_raw_to_dict, module)?)?;
    module.add_function(wrap_pyfunction!(_array_of_documents_to_buffer, module)?)?;
    Ok(())
}

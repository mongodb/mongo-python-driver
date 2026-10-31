//! Encoding Python objects to BSON bytes.
//!
//! A direct port of the C extension's encoder (`_cbsonmodule.c`:
//! `write_dict` / `write_element_to_buffer`), including the `_type_marker`
//! fast-dispatch protocol. The C extension is the behavioral reference.

use std::ffi::CStr;

use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyDateAccess, PyDateTime, PyDict, PyFloat, PyInt, PyList, PyString,
    PyTimeAccess, PyTuple,
};
use pyo3::PyTypeInfo;

use crate::buffer::Buffer;
use crate::datetime_conv::millis_from_fields;
use crate::errors::{invalid_document, invalid_string_data};
use crate::options::{read_type_marker, CodecOptionsData};
use crate::types::Types;

// BSON type bytes.
const BSONNUM: u8 = 0x01;
const BSONSTR: u8 = 0x02;
const BSONOBJ: u8 = 0x03;
const BSONARR: u8 = 0x04;
const BSONBIN: u8 = 0x05;
const BSONOID: u8 = 0x07;
const BSONBOO: u8 = 0x08;
const BSONDAT: u8 = 0x09;
const BSONNUL: u8 = 0x0A;
const BSONRGX: u8 = 0x0B;
const BSONCOD: u8 = 0x0D;
const BSONCWS: u8 = 0x0F;
const BSONINT: u8 = 0x10;
const BSONTIM: u8 = 0x11;
const BSONLON: u8 = 0x12;
const BSONDEC: u8 = 0x13;
const BSONMAX: u8 = 0x7F;
const BSONMIN: u8 = 0xFF;

// _type_marker value of RawBSONDocument (see bson/raw_bson.py).
const MARKER_RAW_BSON: i64 = 101;

const I32_MIN: i64 = -2147483648;
const I32_MAX: i64 = 2147483647;
/// BSON_MAX_SIZE from _cbsonmodule.c.
const BSON_MAX_SIZE_I64: i64 = I32_MAX;

/// True when the value's type is exactly `T` (mirrors `Py*_CheckExact`).
fn is_exact<'py, T: PyTypeInfo>(value: &Bound<'py, PyAny>) -> bool {
    value.is_exact_instance_of::<T>()
}

/// Encodes a mapping to BSON bytes (`_dict_to_bson`).
pub fn dict_to_bson<'py>(
    py: Python<'py>,
    document: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
    top_level: bool,
) -> PyResult<Vec<u8>> {
    let mut buf = Buffer::new();
    // The C's handle_invalid_doc_error wraps only failures of the element
    // iteration loops (not the _id pre-write), so the wrapping lives in
    // write_dict.
    write_dict(py, &mut buf, document, check_keys, options, top_level)?;
    Ok(buf.into_vec())
}

/// Wraps `InvalidDocument` errors with the "Invalid document: " prefix and
/// the document as second arg (mirrors the C `_copy_and_set_exception`).
fn wrap_invalid_document<'py>(py: Python<'py>, document: &Bound<'py, PyAny>, err: PyErr) -> PyErr {
    let types = match Types::get(py) {
        Ok(types) => types,
        Err(_) => return err,
    };
    let is_invalid_document = err
        .value(py)
        .is_instance(types.invalid_document.bind(py))
        .unwrap_or(false);
    if !is_invalid_document {
        return err;
    }
    let message = match err.value(py).str() {
        Ok(m) => match m.to_cow() {
            Ok(c) => c.into_owned(),
            Err(_) => return err,
        },
        Err(_) => return err,
    };
    let class = types.invalid_document.bind(py);
    match class.call1((format!("Invalid document: {message}"), document)) {
        Ok(new_exc) => {
            // Transfer the original traceback so the error points at the
            // encode site (mirrors the C's _transfer_traceback).
            let old_tb = err.value(py).getattr("__traceback__").ok();
            if let Some(tb) = old_tb {
                let _ = new_exc.setattr("__traceback__", tb);
            }
            PyErr::from_value(new_exc)
        }
        Err(_) => err,
    }
}

/// True when `key` is a string equal to `expected`.
fn is_str_eq(key: &Bound<'_, PyAny>, expected: &str) -> bool {
    match key.cast::<PyString>() {
        Ok(s) => s.to_str().map(|s| s == expected).unwrap_or(false),
        Err(_) => false,
    }
}

/// Writes `length + elements + terminator` for a mapping, with the `_id`
/// element first when `top_level` — mirroring the C's `write_dict`, which
/// owns the framing. A raw document (marker 101) writes only its raw
/// bytes: the raw document's own length prefix frames it, so no extra
/// length or terminator is written.
pub fn write_dict<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    mapping: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
    top_level: bool,
) -> PyResult<()> {
    // Fast path: dict or dict subclass (e.g. SON). Non-dict mappings first
    // check for a raw document passthrough, then for the Mapping protocol.
    if mapping.cast::<PyDict>().is_err() {
        if let Some(marker) = read_type_marker(mapping)? {
            if marker == MARKER_RAW_BSON {
                let raw = mapping.getattr(pyo3::intern!(py, "raw"))?;
                let data = raw_bytes_of(py, &raw)?;
                buf.write_bytes(&data);
                return Ok(());
            }
        }
        let types = Types::get(py)?;
        if !mapping.is_instance(types.mapping.bind(py))? {
            return Err(pyo3::exceptions::PyTypeError::new_err(format!(
                "encoder expected a mapping type but got: {}",
                py_repr(mapping)
            )));
        }
    }

    let start = buf.position();
    buf.write_u32_le(0); // length placeholder
    let _guard = recursion_guard(py, GUARD_DOCUMENT)?;

    let mut skip_id = false;
    if top_level {
        // _id is encoded first, regardless of its position in the mapping.
        // The lookup must not trigger mapping hooks (e.g. defaultdict's
        // __missing__): dict lookups use the raw storage API, other
        // mappings use a has-key check first.
        let id_value = if let Ok(dict) = mapping.cast::<PyDict>() {
            dict.get_item(pyo3::intern!(py, "_id"))?
        } else {
            match mapping.get_item(pyo3::intern!(py, "_id")) {
                Ok(value) => Some(value),
                Err(err) if err.is_instance_of::<pyo3::exceptions::PyKeyError>(py) => None,
                Err(err) => return Err(err),
            }
        };
        if let Some(value) = id_value {
            write_value(
                py, buf, b"_id\x00", &value, check_keys, options, false, false,
            )?;
            skip_id = true;
        }
    }
    // Fast path: dict or dict subclass (e.g. SON). Other Mappings are
    // iterated via their `items()` method.
    if let Ok(dict) = mapping.cast::<PyDict>() {
        for (key, value) in dict.iter() {
            if skip_id && is_str_eq(&key, "_id") {
                continue;
            }
            if let Err(err) = write_pair(py, buf, &key, &value, check_keys, options) {
                return Err(fail_element(py, top_level, mapping, err));
            }
        }
    } else {
        let items = mapping.getattr(pyo3::intern!(py, "items"))?.call0()?;
        for item in items.try_iter()? {
            let item = item?;
            let pair = item.cast::<PyTuple>().map_err(|_| {
                pyo3::exceptions::PyTypeError::new_err("encoder expected mapping items")
            })?;
            let key = pair.get_item(0)?;
            let value = pair.get_item(1)?;
            if skip_id && is_str_eq(&key, "_id") {
                continue;
            }
            if let Err(err) = write_pair(py, buf, &key, &value, check_keys, options) {
                return Err(fail_element(py, top_level, mapping, err));
            }
        }
    }
    buf.write_u8(0);
    let length = (buf.len() - start) as u32;
    buf.backpatch_u32(start, length);
    Ok(())
}

/// The element-iteration failure path: at the top level, InvalidDocument
/// errors are wrapped with the document (mirrors the C's
/// `handle_invalid_doc_error` call sites, which gate on `top_level` and
/// exclude the `_id` pre-write).
fn fail_element<'py>(
    py: Python<'py>,
    top_level: bool,
    mapping: &Bound<'py, PyAny>,
    err: PyErr,
) -> PyErr {
    if !top_level {
        return err;
    }
    wrap_invalid_document(py, mapping, err)
}

/// Validates a key and writes its element (`decode_and_write_pair`'s
/// encode-side counterpart: `write_pair` in the C extension).
fn write_pair<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    key: &Bound<'py, PyAny>,
    value: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
) -> PyResult<()> {
    // Keys must be strings.
    let key_str = match key.cast::<PyString>() {
        Ok(s) => s,
        Err(_) => {
            return Err(invalid_document(
                py,
                &format!(
                    "documents must have only string keys, key was {}",
                    key.repr()?.to_cow()?
                ),
            ))
        }
    };
    // UnicodeEncodeError propagates, matching C.
    let key_utf8 = key_str.to_cow()?;

    // Keys must not contain NUL bytes (detected via strlen in C).
    if key_utf8.as_bytes().contains(&0) {
        return Err(invalid_document(
            py,
            "Key names must not contain the NULL byte",
        ));
    }

    if check_keys {
        if key_utf8.starts_with('$') {
            return Err(invalid_document(
                py,
                &format!("key '{}' must not start with '$'", key_utf8),
            ));
        }
        if key_utf8.contains('.') {
            return Err(invalid_document(
                py,
                &format!("key '{}' must not contain '.'", key_utf8),
            ));
        }
    }

    // The element name (key bytes + NUL) goes in a stack buffer for the
    // common short-key case; the C builds it on the stack the same way.
    let key_bytes = key_utf8.as_bytes();
    const STACK_NAME_LEN: usize = 96;
    let mut stack_name = [0u8; STACK_NAME_LEN];
    if key_bytes.len() < STACK_NAME_LEN {
        stack_name[..key_bytes.len()].copy_from_slice(key_bytes);
        return write_value(
            py,
            buf,
            &stack_name[..key_bytes.len() + 1],
            value,
            check_keys,
            options,
            false,
            false,
        );
    }
    let mut name = Vec::with_capacity(key_utf8.len() + 1);
    name.extend_from_slice(key_utf8.as_bytes());
    name.push(0);
    write_value(py, buf, &name, value, check_keys, options, false, false)
}

/// Encodes a name/value pair, dispatching on the value's type.
///
/// Dispatch order mirrors `write_element_to_buffer` in the C extension:
/// exact builtins (no attribute lookups), then `_type_marker`, then the
/// isinstance waterfall, then the type registry, then the fallback encoder.
#[allow(clippy::too_many_arguments)]
fn write_value<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
    in_custom_call: bool,
    in_fallback_call: bool,
) -> PyResult<()> {
    // ---- Exact builtin fast path (no attribute lookups). ----
    // bool must be checked before int (bool is an int subclass).
    if is_exact::<PyBool>(value) {
        let v: bool = value.extract()?;
        buf.write_u8(BSONBOO);
        buf.write_bytes(name);
        buf.write_u8(v as u8);
        return Ok(());
    }
    if is_exact::<PyInt>(value) {
        return write_int_dispatch(
            py,
            buf,
            name,
            value,
            check_keys,
            options,
            in_custom_call,
            in_fallback_call,
        );
    }
    if is_exact::<PyFloat>(value) {
        let v: f64 = value.extract()?;
        buf.write_u8(BSONNUM);
        buf.write_bytes(name);
        buf.write_f64_le(v);
        return Ok(());
    }
    if value.is_none() {
        buf.write_u8(BSONNUL);
        buf.write_bytes(name);
        return Ok(());
    }
    if is_exact::<PyDict>(value) {
        return write_document_value(py, buf, name, value, check_keys, options);
    }
    if is_exact::<PyList>(value) || is_exact::<PyTuple>(value) {
        return write_array_value(py, buf, name, value, check_keys, options);
    }
    if is_exact::<PyBytes>(value) {
        let bytes = value.cast::<PyBytes>().map_err(PyErr::from)?;
        return write_binary_subtype_zero(py, buf, name, bytes);
    }
    if is_exact::<PyString>(value) {
        let string = value.cast::<PyString>().map_err(PyErr::from)?;
        return write_string_value(py, buf, name, string);
    }

    // ---- _type_marker dispatch (exact ints only). ----
    let marker = read_type_marker(value)?;
    if let Some(marker) = marker {
        if write_marker_value(
            py,
            buf,
            name,
            value,
            marker,
            check_keys,
            options,
            in_custom_call,
            in_fallback_call,
        )? {
            return Ok(());
        }
        // Unknown marker: fall through to the isinstance chain.
    }

    // ---- isinstance waterfall for builtins and stdlib types. ----
    if value.is_instance_of::<PyBool>() {
        let v: bool = value.extract()?;
        buf.write_u8(BSONBOO);
        buf.write_bytes(name);
        buf.write_u8(v as u8);
        return Ok(());
    }
    if value.is_instance_of::<PyInt>() {
        return write_int_dispatch(
            py,
            buf,
            name,
            value,
            check_keys,
            options,
            in_custom_call,
            in_fallback_call,
        );
    }
    if value.is_instance_of::<PyFloat>() {
        let v: f64 = value.extract()?;
        buf.write_u8(BSONNUM);
        buf.write_bytes(name);
        buf.write_f64_le(v);
        return Ok(());
    }
    if value.is_none() {
        buf.write_u8(BSONNUL);
        buf.write_bytes(name);
        return Ok(());
    }
    if value.is_instance_of::<PyDict>() {
        return write_document_value(py, buf, name, value, check_keys, options);
    }
    if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        return write_array_value(py, buf, name, value, check_keys, options);
    }
    if value.is_instance_of::<PyBytes>() {
        let bytes = value.cast::<PyBytes>().map_err(PyErr::from)?;
        return write_binary_subtype_zero(py, buf, name, bytes);
    }
    if value.is_instance_of::<PyString>() {
        let string = value.cast::<PyString>().map_err(PyErr::from)?;
        return write_string_value(py, buf, name, string);
    }
    if value.cast::<PyDateTime>().is_ok() {
        return write_datetime_value(py, buf, name, value);
    }
    let types = Types::get(py)?;
    if value.is_instance(types.re_type.bind(py))? {
        return write_regex_value(py, buf, name, value);
    }
    if value.is_instance(types.mapping.bind(py))? {
        return write_document_value(py, buf, name, value, check_keys, options);
    }
    if value.is_instance(types.uuid.bind(py))? {
        // Encoded as Binary.from_uuid(value, uuid_representation).
        let binary = types.binary.bind(py).call_method1(
            pyo3::intern!(py, "from_uuid"),
            (value, options.uuid_representation),
        )?;
        return write_value(
            py,
            buf,
            name,
            &binary,
            check_keys,
            options,
            in_custom_call,
            in_fallback_call,
        );
    }

    // ---- Custom type encoder (exact type match), then fallback. ----
    // The re-dispatch is bounded by CPython's recursion counter (the C
    // relies on the Python frames of the callbacks; guarding explicitly
    // keeps the Rust stack bounded the same way).
    if let Some(converted) = custom_encoder_value(py, options, value, in_custom_call)? {
        let _guard = recursion_guard(py, GUARD_EMPTY)?;
        return write_value(py, buf, name, &converted, check_keys, options, true, false);
    }
    if let Some(converted) = fallback_encoder_value(py, options, value, in_fallback_call)? {
        let _guard = recursion_guard(py, GUARD_EMPTY)?;
        return write_value(py, buf, name, &converted, check_keys, options, false, true);
    }

    // ---- Cannot encode. ----
    buf.write_bytes(name); // Keep the buffer consistent for error reporting.
    Err(cannot_encode(py, value, in_fallback_call))
}

/// RAII guard around CPython's recursion limit, mirroring the C's
/// `Py_EnterRecursiveCall`/`Py_LeaveRecursiveCall` pairs. Cyclic structures
/// raise RecursionError instead of overflowing the stack.
pub struct RecursionGuard {
    // Not Send: Py_EnterRecursiveCall/LeaveRecursiveCall touch the
    // current thread's recursion counter.
    _not_send: std::marker::PhantomData<*const ()>,
}

/// Enters a CPython recursion check; returns an `Err` with the pending
/// error (typically RecursionError) when the limit is hit.
///
/// The message is a static C string (CPython reads it when formatting the
/// RecursionError): no per-call allocation, so guards are cheap enough
/// for every document, array, and converted value.
pub fn recursion_guard(py: Python<'_>, message: &'static CStr) -> PyResult<RecursionGuard> {
    let ok = unsafe { pyo3::ffi::Py_EnterRecursiveCall(message.as_ptr()) };
    if ok != 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(RecursionGuard {
        _not_send: std::marker::PhantomData,
    })
}

/// The `Py_EnterRecursiveCall` messages, matching the C extension's.
pub const GUARD_EMPTY: &CStr = c"";
pub const GUARD_DOCUMENT: &CStr = c" while encoding a BSON document";
pub const GUARD_ARRAY: &CStr = c" while encoding a BSON array";
pub const GUARD_DECODE_DOCUMENT: &CStr = c" while decoding a BSON document";
pub const GUARD_DECODE_LIST: &CStr = c" while decoding a list value";

impl Drop for RecursionGuard {
    fn drop(&mut self) {
        unsafe { pyo3::ffi::Py_LeaveRecursiveCall() };
    }
}

/// Handles `_type_marker` values (Binary=5, ObjectId=7, DatetimeMS=9,
/// Regex=11, Code=13, Timestamp=17, Int64=18, Decimal128=19, DBRef=100,
/// RawBSONDocument=101, MaxKey=127, MinKey=255). Returns `false` for
/// unrecognized markers, which fall through to the isinstance chain
/// (mirrors the C switch without a default case).
#[allow(clippy::too_many_arguments)]
fn write_marker_value<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
    marker: i64,
    _check_keys: bool,
    options: &CodecOptionsData<'py>,
    _in_custom_call: bool,
    _in_fallback_call: bool,
) -> PyResult<bool> {
    match marker {
        5 => {
            // Binary: read .subtype; Binary is a bytes subclass.
            let subtype: i64 = value
                .getattr(pyo3::intern!(py, "subtype"))?
                .extract()
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("invalid subtype"))?;
            let data = value.cast::<PyBytes>().map_err(PyErr::from)?.as_bytes();
            // Mirrors the C's _downcast_and_check(size, extra): the length
            // plus its framing (subtype byte, plus the extra length prefix
            // for subtype 2) must fit in an int32.
            let extra: i64 = if subtype == 2 { 4 + 1 } else { 1 };
            if data.len() as i64 > BSON_MAX_SIZE_I64 - extra {
                return Err(invalid_string_data(
                    py,
                    "String length must be <= 2147483647",
                ));
            }
            let size = data.len() as i32;
            buf.write_u8(BSONBIN);
            buf.write_bytes(name);
            if subtype == 2 {
                // The payload carries an extra length prefix.
                buf.write_i32_le(size + 4);
                buf.write_u8(2);
            }
            buf.write_i32_le(size);
            if subtype != 2 {
                buf.write_u8(subtype as u8);
            }
            buf.write_bytes(data);
            Ok(true)
        }
        7 => {
            // ObjectId: .binary is the 12-byte representation.
            let binary = value.getattr(pyo3::intern!(py, "binary"))?;
            let bytes = binary.cast::<PyBytes>().map_err(PyErr::from)?;
            buf.write_u8(BSONOID);
            buf.write_bytes(name);
            buf.write_bytes(bytes.as_bytes());
            Ok(true)
        }
        9 => {
            // DatetimeMS: int(value) is the millis value (the C uses
            // PyNumber_Long, which routes through __int__ and requires the
            // result to be an int; a float __int__ raises TypeError).
            let as_int = value.call_method0(pyo3::intern!(py, "__int__"))?;
            if !as_int.is_instance_of::<PyInt>() {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "int() argument must be an int, not a float",
                ));
            }
            let millis: i64 = as_int.extract().map_err(|_| {
                pyo3::exceptions::PyOverflowError::new_err(
                    "MongoDB datetimes can only handle up to 8-byte ints",
                )
            })?;
            buf.write_u8(BSONDAT);
            buf.write_bytes(name);
            buf.write_i64_le(millis);
            Ok(true)
        }
        11 => {
            write_regex_value(py, buf, name, value)?;
            Ok(true)
        }
        13 => {
            // Code: the value is the code string; .scope decides between
            // plain code (0x0D) and code w/ scope (0x0F).
            let scope = value.getattr(pyo3::intern!(py, "scope"))?;
            let code = value.cast::<PyString>().map_err(PyErr::from)?;
            if scope.is_none() {
                buf.write_u8(BSONCOD);
                buf.write_bytes(name);
                write_string_payload(buf, code)?;
                return Ok(true);
            }
            buf.write_u8(BSONCWS);
            buf.write_bytes(name);
            let start = buf.position();
            buf.write_u32_le(0); // total length placeholder
            write_string_payload(buf, code)?;
            // The scope is a full document (length + elements + term),
            // written without key checking (mirrors the C).
            write_dict(py, buf, &scope, false, options, false)?;
            let length = (buf.len() - start) as u32;
            buf.backpatch_u32(start, length);
            Ok(true)
        }
        17 => {
            // Timestamp: i32 inc, then i32 time. Mirrors the C's
            // PyLong_AsUnsignedLong: negative or >unsigned-long values
            // raise OverflowError; [2³¹, 2³²) truncates to i32.
            let inc: u64 = value
                .getattr(pyo3::intern!(py, "inc"))?
                .extract()
                .map_err(|_| {
                    pyo3::exceptions::PyOverflowError::new_err(
                        "can't convert negative value to unsigned long",
                    )
                })?;
            let time: u64 = value
                .getattr(pyo3::intern!(py, "time"))?
                .extract()
                .map_err(|_| {
                    pyo3::exceptions::PyOverflowError::new_err(
                        "can't convert negative value to unsigned long",
                    )
                })?;
            buf.write_u8(BSONTIM);
            buf.write_bytes(name);
            buf.write_i32_le(inc as i32);
            buf.write_i32_le(time as i32);
            Ok(true)
        }
        18 => {
            // Int64.
            let v: i64 = value.extract().map_err(|_| {
                pyo3::exceptions::PyOverflowError::new_err(
                    "MongoDB can only handle up to 8-byte ints",
                )
            })?;
            buf.write_u8(BSONLON);
            buf.write_bytes(name);
            buf.write_i64_le(v);
            Ok(true)
        }
        19 => {
            // Decimal128: .bid is the 16-byte representation.
            let bid = value.getattr(pyo3::intern!(py, "bid"))?;
            let bytes = bid.cast::<PyBytes>().map_err(PyErr::from)?;
            buf.write_u8(BSONDEC);
            buf.write_bytes(name);
            buf.write_bytes(bytes.as_bytes());
            Ok(true)
        }
        100 => {
            // DBRef: encoded as its as_doc() document.
            let as_doc = value.call_method0(pyo3::intern!(py, "as_doc"))?;
            buf.write_u8(BSONOBJ);
            buf.write_bytes(name);
            write_dict(py, buf, &as_doc, false, options, false)?;
            Ok(true)
        }
        101 => {
            // RawBSONDocument: .raw (bytes or memoryview) written as-is.
            let raw = value.getattr(pyo3::intern!(py, "raw"))?;
            let data = raw_bytes_of(py, &raw)?;
            buf.write_u8(BSONOBJ);
            buf.write_bytes(name);
            buf.write_bytes(&data);
            Ok(true)
        }
        255 => {
            buf.write_u8(BSONMIN);
            buf.write_bytes(name);
            Ok(true)
        }
        127 => {
            buf.write_u8(BSONMAX);
            buf.write_bytes(name);
            Ok(true)
        }
        _ => Ok(false), // unknown marker: fall through to the chain
    }
}

/// Extracts bytes from a bytes or memoryview object (`write_raw_doc`).
fn raw_bytes_of<'py>(_py: Python<'py>, raw: &Bound<'py, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = raw.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }
    if !raw.is_instance_of::<pyo3::types::PyMemoryView>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(format!(
            "RawBSONDocument.raw must be bytes or memoryview, not {}",
            raw.get_type().name()?.to_cow()?
        )));
    }
    let buffer = PyBuffer::<u8>::get(raw)?;
    // Defensive checks mirroring the C's write_raw_doc/_get_buffer: a
    // misbehaving exporter may hand back non-contiguous or null buffers.
    if buffer.buf_ptr().is_null() || !buffer.is_c_contiguous() || buffer.item_size() != 1 {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "RawBSONDocument.raw must be bytes or memoryview",
        ));
    }
    let slice =
        unsafe { std::slice::from_raw_parts(buffer.buf_ptr() as *const u8, buffer.len_bytes()) };
    Ok(slice.to_vec())
}

/// Writes the regex element shared by the `Regex` class and compiled
/// patterns: `0x0B + name + pattern cstr + flags cstr`.
fn write_regex_value<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let int_flags: i64 = value
        .getattr(pyo3::intern!(py, "flags"))?
        .extract()
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("invalid regex flags"))?;
    let pattern_obj = value.getattr(pyo3::intern!(py, "pattern"))?;

    let pattern_bytes: Vec<u8> = if let Ok(p) = pattern_obj.cast::<PyString>() {
        // UnicodeEncodeError propagates for invalid patterns, matching C.
        p.to_str()?.as_bytes().to_vec()
    } else if let Ok(p) = pattern_obj.cast::<PyBytes>() {
        // Bytes patterns are revalidated as UTF-8 (mirrors the C).
        let data = p.as_bytes();
        match std::str::from_utf8(data) {
            Ok(_) => {}
            Err(_) => {
                return Err(invalid_string_data(
                    py,
                    "regex patterns must be valid UTF-8",
                ))
            }
        }
        data.to_vec()
    } else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "a bytes-like object is required for the regex pattern",
        ));
    };
    if pattern_bytes.contains(&0) {
        return Err(invalid_document(
            py,
            "regex patterns must not contain the NULL byte",
        ));
    }

    buf.write_u8(BSONRGX);
    buf.write_bytes(name);
    buf.write_bytes(&pattern_bytes);
    buf.write_u8(0);
    // Flag chars are written in bit order: i l m s u x.
    let mut flags = Vec::new();
    if int_flags & 2 != 0 {
        flags.push(b'i');
    }
    if int_flags & 4 != 0 {
        flags.push(b'l');
    }
    if int_flags & 8 != 0 {
        flags.push(b'm');
    }
    if int_flags & 16 != 0 {
        flags.push(b's');
    }
    if int_flags & 32 != 0 {
        flags.push(b'u');
    }
    if int_flags & 64 != 0 {
        flags.push(b'x');
    }
    buf.write_bytes(&flags);
    buf.write_u8(0);
    Ok(())
}

/// Formats a value's repr as an owned String (error paths only).
fn py_repr(value: &Bound<'_, PyAny>) -> String {
    value
        .repr()
        .and_then(|r| r.to_cow().map(|c| c.into_owned()))
        .unwrap_or_default()
}

/// The `PyLong` encode path shared by exact ints and int subclasses:
/// int32 when it fits, else int64. On overflow the error is cleared and
/// the custom/fallback encoders get a chance (mirrors the C behavior);
/// the final OverflowError comes from `cannot_encode`.
#[allow(clippy::too_many_arguments)]
fn write_int_dispatch<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
    in_custom_call: bool,
    in_fallback_call: bool,
) -> PyResult<()> {
    match value.extract::<i64>() {
        Ok(v) => {
            if (I32_MIN..=I32_MAX).contains(&v) {
                buf.write_u8(BSONINT);
                buf.write_bytes(name);
                buf.write_i32_le(v as i32);
            } else {
                buf.write_u8(BSONLON);
                buf.write_bytes(name);
                buf.write_i64_le(v);
            }
            Ok(())
        }
        Err(_) => {
            // Overflow: give the custom/fallback encoders a chance, then
            // fail in cannot_encode (which raises OverflowError for ints).
            if let Some(converted) = custom_encoder_value(py, options, value, in_custom_call)? {
                return write_value(
                    py,
                    buf,
                    name,
                    &converted,
                    check_keys,
                    options,
                    true,
                    in_fallback_call,
                );
            }
            if let Some(converted) = fallback_encoder_value(py, options, value, in_fallback_call)? {
                return write_value(py, buf, name, &converted, check_keys, options, false, true);
            }
            Err(cannot_encode(py, value, in_fallback_call))
        }
    }
}

/// Consults `options.type_registry._encoder_map` for an exact-type encoder.
fn custom_encoder_value<'py>(
    py: Python<'py>,
    options: &CodecOptionsData<'py>,
    value: &Bound<'py, PyAny>,
    in_custom_call: bool,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if in_custom_call {
        return Ok(None);
    }
    let Some(encoder_map) = options.encoder_map(py) else {
        return Ok(None);
    };
    let converter = encoder_map.get_item(value.get_type())?;
    let Some(converter) = converter else {
        return Ok(None);
    };
    Ok(Some(converter.call1((value,))?))
}

/// Consults `options.type_registry._fallback_encoder`.
fn fallback_encoder_value<'py>(
    py: Python<'py>,
    options: &CodecOptionsData<'py>,
    value: &Bound<'py, PyAny>,
    in_fallback_call: bool,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    if in_fallback_call {
        return Ok(None);
    }
    let Some(fallback) = options.fallback_encoder(py) else {
        return Ok(None);
    };
    Ok(Some(fallback.call1((value,))?))
}

/// The final failure path, mirroring `_set_cannot_encode`: OverflowError
/// for ints that do not fit in 8 bytes, InvalidDocument otherwise.
fn cannot_encode(py: Python<'_>, value: &Bound<'_, PyAny>, _was_fallback: bool) -> PyErr {
    if value.is_instance_of::<PyInt>() && value.extract::<i64>().is_err() {
        return pyo3::exceptions::PyOverflowError::new_err(
            "MongoDB can only handle up to 8-byte ints",
        );
    }
    invalid_document(
        py,
        &format!(
            "cannot encode object: {}, of type: {}",
            py_repr(value),
            py_repr(&value.get_type().into_any()),
        ),
    )
}

/// Writes `0x03 + name + document` for dict-like values.
fn write_document_value<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
) -> PyResult<()> {
    buf.write_u8(BSONOBJ);
    buf.write_bytes(name);
    write_dict(py, buf, value, check_keys, options, false)
}

/// Writes `0x04 + name + array` (arrays are documents with numeric keys).
fn write_array_value<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
    check_keys: bool,
    options: &CodecOptionsData<'py>,
) -> PyResult<()> {
    buf.write_u8(BSONARR);
    buf.write_bytes(name);
    let start = buf.position();
    buf.write_u32_le(0); // length placeholder
    let _guard = recursion_guard(py, GUARD_ARRAY)?;
    let len = value.len()?;
    // List/tuple values (including subclasses — the cast is
    // isinstance-based, matching the C's subclass-inclusive PyList_Check /
    // PyTuple_Check) are read through the concrete sequence API
    // (PyList_GET_ITEM / PyTuple_GET_ITEM in the C), which bypasses
    // __getitem__ overrides. The mapping-protocol fallback below only
    // stays for defensive symmetry with the dispatch above it.
    let list = value.cast::<PyList>().ok();
    let tuple = if list.is_none() {
        value.cast::<PyTuple>().ok()
    } else {
        None
    };
    // The numeric element key ("0", "1", ... + NUL) is built in a stack
    // buffer (snprintf into a local in the C): no per-element allocation.
    let mut index_key = [0u8; 12];
    for i in 0..len {
        let item = if let Some(list) = &list {
            list.get_item(i)
        } else if let Some(tuple) = &tuple {
            tuple.get_item(i)
        } else {
            value.get_item(i)
        }?;
        // Array lengths are int32, so i < 2^31 has at most 10 digits: the
        // write below always fits 11 bytes including the NUL.
        let mut pos = index_key.len() - 1;
        index_key[pos] = 0;
        let mut n = i;
        loop {
            pos -= 1;
            index_key[pos] = b'0' + (n % 10) as u8;
            n /= 10;
            if n == 0 {
                break;
            }
        }
        write_value(
            py,
            buf,
            &index_key[pos..],
            &item,
            check_keys,
            options,
            false,
            false,
        )?;
    }
    buf.write_u8(0);
    let length = (buf.len() - start) as u32;
    buf.backpatch_u32(start, length);
    Ok(())
}

/// Writes `bytes` values as BSON binary subtype 0.
fn write_binary_subtype_zero(
    _py: Python<'_>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'_, PyBytes>,
) -> PyResult<()> {
    let data = value.as_bytes();
    if data.len() > i32::MAX as usize {
        return Err(invalid_string_data(
            _py,
            "String length must be <= 2147483647",
        ));
    }
    buf.write_u8(BSONBIN);
    buf.write_bytes(name);
    buf.write_i32_le(data.len() as i32);
    buf.write_u8(0); // subtype
    buf.write_bytes(data);
    Ok(())
}

/// Writes `0x02 + name + i32(len+1) + utf8 + NUL`.
fn write_string_value<'py>(
    _py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyString>,
) -> PyResult<()> {
    buf.write_u8(BSONSTR);
    buf.write_bytes(name);
    write_string_payload(buf, value)
}

/// Writes the string payload: `i32(len+1) + utf8 + NUL`.
fn write_string_payload(buf: &mut Buffer, value: &Bound<'_, PyString>) -> PyResult<()> {
    // UnicodeEncodeError propagates, matching C.
    let utf8 = value.to_cow()?;
    if utf8.len() >= i32::MAX as usize {
        return Err(invalid_string_data(
            value.py(),
            "String length must be <= 2147483647",
        ));
    }
    buf.write_i32_le(utf8.len() as i32 + 1);
    buf.write_bytes(utf8.as_bytes());
    buf.write_u8(0);
    Ok(())
}

/// Writes datetime values as `0x09 + name + i64 millis`.
///
/// Aware datetimes are shifted by `utcoffset()` first (mirrors the C
/// encoder, which subtracts the offset and then treats the fields as UTC).
fn write_datetime_value<'py>(
    py: Python<'py>,
    buf: &mut Buffer,
    name: &[u8],
    value: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let dt = value.cast::<PyDateTime>().map_err(PyErr::from)?;
    let offset = dt.call_method0(pyo3::intern!(py, "utcoffset"))?;
    let effective: Bound<'py, PyDateTime> = if offset.is_none() {
        dt.clone()
    } else {
        let shifted = value.sub(offset)?;
        shifted.cast::<PyDateTime>().map_err(PyErr::from)?.clone()
    };
    let millis = millis_from_fields(
        effective.get_year(),
        effective.get_month(),
        effective.get_day(),
        effective.get_hour(),
        effective.get_minute(),
        effective.get_second(),
        effective.get_microsecond(),
    );
    buf.write_u8(BSONDAT);
    buf.write_bytes(name);
    buf.write_i64_le(millis);
    Ok(())
}

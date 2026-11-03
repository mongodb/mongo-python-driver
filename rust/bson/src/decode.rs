//! Decoding BSON bytes to Python objects.
//!
//! A direct port of the C extension's decoder (`_cbsonmodule.c`:
//! `get_value` / `elements_to_dict`). The C extension is the behavioral
//! reference.

use std::sync::Mutex;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDateTime, PyDeltaAccess, PyDict, PyList, PyString, PyTzInfo};

use crate::datetime_conv::millis_to_fields;
use crate::errors::{bson_error, invalid_bson};
use crate::options::{CodecOptionsData, DATETIME_AUTO, DATETIME_CLAMP, DATETIME_MS};
use crate::types::Types;

// BSON type bytes.
const BSONNUM: u8 = 0x01;
const BSONSTR: u8 = 0x02;
const BSONOBJ: u8 = 0x03;
const BSONARR: u8 = 0x04;
const BSONBIN: u8 = 0x05;
const BSONUND: u8 = 0x06;
const BSONOID: u8 = 0x07;
const BSONBOO: u8 = 0x08;
const BSONDAT: u8 = 0x09;
const BSONNUL: u8 = 0x0A;
const BSONRGX: u8 = 0x0B;
const BSONREF: u8 = 0x0C;
const BSONCOD: u8 = 0x0D;
const BSONSYM: u8 = 0x0E;
const BSONCWS: u8 = 0x0F;
const BSONINT: u8 = 0x10;
const BSONTIM: u8 = 0x11;
const BSONLON: u8 = 0x12;
const BSONDEC: u8 = 0x13;
const BSONMAX: u8 = 0x7F;
const BSONMIN: u8 = 0xFF;

/// The smallest legal BSON document: 4-byte length + terminator.
pub const BSON_MIN_SIZE: usize = 5;

/// Maximum BSON document size (mirrors BSON_MAX_SIZE in _cbsonmodule.c).
pub const BSON_MAX_SIZE: usize = i32::MAX as usize;

/// Context for a decode pass: the borrowed data plus the owning Python
/// object (needed to build memoryview slices for raw document classes).
pub struct DecodeInput<'a, 'py> {
    pub data: &'a [u8],
    /// The original buffer object (bytes, bytearray, memoryview, ...) or
    /// None when unknown.
    pub owner: Option<&'a Bound<'py, PyAny>>,
}

/// Decodes a single BSON document (`_bson_to_dict`).
///
/// `data` must be exactly one document (the C validates
/// `size == total_size` and a NUL terminator).
pub fn bson_to_dict<'py>(
    py: Python<'py>,
    input: &DecodeInput<'_, 'py>,
    options: &CodecOptionsData<'py>,
) -> PyResult<Py<PyAny>> {
    let data = input.data;
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
    if data.len() < size || data.len() > BSON_MAX_SIZE {
        return Err(invalid_bson(py, "objsize too large"));
    }
    if size != data.len() || data[size - 1] != 0 {
        return Err(invalid_bson(py, "bad eoo"));
    }
    elements_to_dict(py, input, &data[..size], options).map(|(value, _)| value)
}

/// Decodes all elements of a BSON document into a new
/// `options.document_class` instance, mirroring `elements_to_dict`.
///
/// `data` must be exactly one valid document: a 4-byte length prefix equal
/// to `data.len()` and a NUL terminator (checked by `bson_to_dict`,
/// `read_size_at`, `read_size_prefix`, and the Code-with-scope case).
/// Returns the document and whether any field name started with `$` (see
/// `decode_elements_into`).
pub fn elements_to_dict<'py>(
    py: Python<'py>,
    input: &DecodeInput<'_, 'py>,
    data: &[u8],
    options: &CodecOptionsData<'py>,
) -> PyResult<(Py<PyAny>, bool)> {
    let size = data.len();

    if options.is_raw_bson {
        return raw_document(py, input, data, size, options).map(|value| (value, false));
    }

    // PyDict_New() directly for the common case (mirrors the C).
    let document: Bound<'py, PyAny> = if options.is_dict_class {
        PyDict::new(py).into_any()
    } else {
        options.document_class.call0()?
    };
    let has_dollar_key = {
        let _guard = crate::encode::recursion_guard(py, crate::encode::GUARD_DECODE_DOCUMENT)?;
        decode_elements_into(py, &document, &data[4..size - 1], options)?
    };
    Ok((document.unbind(), has_dollar_key))
}

/// Builds a raw document class instance from the raw bytes: the whole
/// buffer is passed through when possible, larger documents become
/// memoryview slices, smaller ones are copied (`elements_to_dict`'s
/// is_raw_bson path in the C).
fn raw_document<'py>(
    py: Python<'py>,
    input: &DecodeInput<'_, 'py>,
    data: &[u8],
    size: usize,
    options: &CodecOptionsData<'py>,
) -> PyResult<Py<PyAny>> {
    let owner = input.owner.and_then(|o| o.cast::<PyBytes>().ok());
    // Only immutable bytes owners may be viewed or sliced; anything else
    // (bytearray, foreign buffer) is always copied.
    let bson_bytes: Bound<'py, PyAny> = if let Some(owner) = owner {
        let owner_data = owner.as_bytes();
        // The doc slice must lie within the owner buffer (mirrors the C's
        // pointer-difference offset check).
        let offset = if data.as_ptr() >= owner_data.as_ptr() {
            data.as_ptr().addr() - owner_data.as_ptr().addr()
        } else {
            return Err(invalid_bson(py, "invalid buffer offset"));
        };
        if offset + size > owner_data.len() {
            return Err(invalid_bson(py, "invalid buffer offset"));
        }
        let threshold = Types::get(py)?.raw_bson_view_threshold;
        if offset == 0 && size == owner_data.len() {
            // The document spans the entire buffer: pass it through.
            owner.clone().into_any()
        } else if size >= threshold {
            // Zero-copy: a read-only slice of the immutable buffer.
            let view = pyo3::types::PyMemoryView::from(owner.clone().as_any())?;
            view.get_item(pyo3::types::PySlice::new(
                py,
                offset as isize,
                (offset + size) as isize,
                1,
            ))?
        } else {
            PyBytes::new(py, &data[..size]).into_any()
        }
    } else {
        PyBytes::new(py, &data[..size]).into_any()
    };
    let document = options
        .document_class
        .call1((bson_bytes, options.options_obj(py)))?;
    Ok(document.unbind())
}

/// Decodes elements from `body` (a document's elements, excluding the
/// length prefix and terminator) into an existing mapping. Returns whether
/// any field name started with `$` — a necessary condition for a DBRef
/// shape, letting `get_value` skip the hook in the common case.
pub fn decode_elements_into<'py>(
    py: Python<'py>,
    document: &Bound<'py, PyAny>,
    body: &[u8],
    options: &CodecOptionsData<'py>,
) -> PyResult<bool> {
    let input = DecodeInput {
        data: body,
        owner: None,
    };
    // PyDict_SetItem() is faster than PyObject_SetItem() because it avoids
    // method lookup overhead (mirrors the C).
    let dict_target: Option<Bound<'py, PyDict>> = if options.is_dict_class {
        document.cast::<PyDict>().ok().cloned()
    } else {
        None
    };
    let mut position = 0;
    let mut has_dollar_key = false;
    while position < body.len() {
        let type_byte = body[position];
        position += 1;
        // Read the cstring name. Mirrors the C's strlen-then-check: a name
        // that is not NUL-terminated within the element's extent raises
        // "field name too large".
        let name_start = position;
        position += memchr::memchr(0, &body[position..]).unwrap_or(body.len() - position);
        if position >= body.len() {
            return Err(invalid_bson(py, "field name too large"));
        }
        let name = &body[name_start..position];
        position += 1; // skip the NUL

        // The name is decoded before the value (mirrors the C's
        // _element_to_dict); a UnicodeDecodeError is wrapped in InvalidBSON.
        let key = decode_key(py, name, options).map_err(|err| rewrap_as_invalid_bson(py, err))?;

        let (value, new_position) =
            get_value(py, &input, name, type_byte, position, body.len(), options)
                .map_err(|err| rewrap_as_invalid_bson(py, err))?;
        position = new_position;
        // Document-level converter failures propagate unwrapped (the C's
        // _element_to_dict does not rewrap get_value failures).
        let value = apply_decoder(py, value, options)?;

        // The raw-byte shortcut needs valid UTF-8 plus an unmodified
        // strict handler: a registered "strict" handler can still turn
        // b'\xffref' into '$ref' (the C's _dbref_hook sees only the
        // decoded mapping). Surrogate keys cannot equal "$ref"/"$id".
        has_dollar_key |= if options.utf8_handler.is_none() && std::str::from_utf8(name).is_ok() {
            name.first() == Some(&b'$')
        } else {
            key.cast::<PyString>()
                .ok()
                .and_then(|key| key.to_str().ok())
                .is_some_and(|key| key.starts_with('$'))
        };

        if let Some(dict) = &dict_target {
            dict.set_item(key, value)?;
        } else {
            document.set_item(key, value)?;
        }
    }
    if position != body.len() {
        return Err(invalid_bson(py, "bad object or element length"));
    }
    Ok(has_dollar_key)
}

/// Decodes the value of one element starting at `position` (just after the
/// name). Returns `(value, new_position)`, mirroring the C `get_value`.
///
/// `end` is the exclusive upper bound of the element's extent: every read
/// for this value must lie below it (the C passes `max` for the same
/// purpose).
#[inline]
pub fn get_value<'py>(
    py: Python<'py>,
    input: &DecodeInput<'_, 'py>,
    name: &[u8],
    type_byte: u8,
    position: usize,
    end: usize,
    options: &CodecOptionsData<'py>,
) -> PyResult<(Py<PyAny>, usize)> {
    get_value_with_raw_array(py, input, name, type_byte, position, end, options, false)
}

/// `get_value` with the `raw_array` flag: when set, array elements (0x04)
/// are returned as raw bytes instead of decoded lists (used by
/// RawBSONDocument's `_RawArrayBSONDocument`).
#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
pub fn get_value_with_raw_array<'py>(
    py: Python<'py>,
    input: &DecodeInput<'_, 'py>,
    name: &[u8],
    type_byte: u8,
    mut position: usize,
    end: usize,
    options: &CodecOptionsData<'py>,
    raw_array: bool,
) -> PyResult<(Py<PyAny>, usize)> {
    let data = input.data;
    // The catch-all for any bounds violation inside get_value mirrors the
    // C's `invalid:` label: "invalid length or type code".
    let invalid = || invalid_bson(py, "invalid length or type code");
    let value: Py<PyAny> = match type_byte {
        BSONNUM => {
            if position + 8 > end {
                return Err(invalid());
            }
            let v = f64::from_le_bytes(data[position..position + 8].try_into().unwrap());
            position += 8;
            v.into_pyobject(py)?.unbind().into_any()
        }
        BSONSTR | BSONSYM => {
            // Strings and deprecated symbols decode to str.
            let length = read_string_length(py, data, position, end)?;
            let string_start = position + 4;
            if string_start + length > end {
                return Err(invalid());
            }
            if data[string_start + length - 1] != 0 {
                return Err(invalid());
            }
            let slice = &data[string_start..string_start + length - 1];
            position = string_start + length;
            let string = decode_utf8(py, slice, options)?;
            string.unbind()
        }
        BSONOBJ => {
            let size = read_size_at(py, data, position, end)?;
            let (value, has_dollar_key) =
                elements_to_dict(py, input, &data[position..position + size], options)
                    .map_err(|err| rewrap_as_invalid_bson(py, err))?;
            position += size;
            // The C runs the DBRef hook unconditionally; the skip is only
            // sound for exact dicts, which store keys as decoded. A custom
            // class may add or rewrite keys, so its result is always
            // checked.
            if options.is_raw_bson || (options.is_dict_class && !has_dollar_key) {
                value
            } else {
                dbref_hook(py, value, options)?
            }
        }
        BSONARR => {
            let size = read_size_at(py, data, position, end)?;
            if raw_array {
                // Treat the whole array element as a binary buffer.
                let raw = PyBytes::new(py, &data[position..position + size]);
                position += size;
                raw.unbind().into_any()
            } else {
                // Arrays decode to lists; the numeric keys are skipped
                // (they are in order).
                let arr_end = position + size - 1;
                if data[arr_end] != 0 {
                    return Err(invalid());
                }
                let list = PyList::empty(py);
                let mut pos = position + 4;
                let _guard = crate::encode::recursion_guard(py, crate::encode::GUARD_DECODE_LIST)?;
                while pos < arr_end {
                    let element_type = data[pos];
                    pos += 1; // past the type byte
                    pos += memchr::memchr(0, &data[pos..arr_end]).ok_or_else(invalid)?;
                    pos += 1; // Skip the NUL: keys are in order.
                    let (item, new_pos) =
                        get_value(py, input, name, element_type, pos, arr_end, options)
                            .map_err(|err| rewrap_as_invalid_bson(py, err))?;
                    // The C's array loop wraps item failures (goto
                    // invalid), converter errors included.
                    let item = apply_decoder(py, item, options)
                        .map_err(|err| rewrap_as_invalid_bson(py, err))?;
                    pos = new_pos;
                    if unsafe { pyo3::ffi::PyList_Append(list.as_ptr(), item.as_ptr()) } < 0 {
                        return Err(PyErr::fetch(py));
                    }
                }
                if pos != arr_end {
                    return Err(invalid());
                }
                position = arr_end + 1; // past the terminator
                list.into_any().unbind()
            }
        }
        BSONBIN => {
            if position + 5 > end {
                return Err(invalid());
            }
            let length = read_u32(py, data, position)? as usize;
            if length > end - position - 5 {
                return Err(invalid());
            }
            let subtype = data[position + 4];
            position += 5;
            let data_start = position;
            let types = Types::get(py)?;
            if subtype == 2 {
                if length < 4 {
                    return Err(invalid());
                }
                let length2 = read_u32(py, data, position)? as usize;
                if length2 != length - 4 {
                    return Err(invalid());
                }
            }
            if subtype == 0 {
                // Plain bytes.
                let slice = &data[data_start..data_start + length];
                position += length;
                PyBytes::new(py, slice).unbind().into_any()
            } else if subtype == 3 || subtype == 4 {
                // UUID handling.
                let raw = if subtype == 2 {
                    &data[data_start + 4..data_start + length]
                } else {
                    &data[data_start..data_start + length]
                };
                let payload = PyBytes::new(py, raw);
                position += length;
                let binary = types.binary.bind(py).call1((payload, subtype as i32))?;
                let uuid_rep = options.uuid_representation;
                if uuid_rep == 0
                    || (subtype == 4 && uuid_rep != 4)
                    || (subtype == 3 && uuid_rep == 4)
                {
                    binary.unbind()
                } else {
                    binary
                        .call_method1(pyo3::intern!(py, "as_uuid"), (uuid_rep,))?
                        .unbind()
                }
            } else {
                let raw = if subtype == 2 {
                    &data[data_start + 4..data_start + length]
                } else {
                    &data[data_start..data_start + length]
                };
                let payload = PyBytes::new(py, raw);
                position += length;
                types
                    .binary
                    .bind(py)
                    .call1((payload, subtype as i32))?
                    .unbind()
            }
        }
        BSONUND | BSONNUL => py.None(),
        BSONOID => {
            if position + 12 > end {
                return Err(invalid());
            }
            let oid_bytes = PyBytes::new(py, &data[position..position + 12]);
            position += 12;
            let types = Types::get(py)?;
            vectorcall(py, &types.object_id, &[oid_bytes.as_ptr()])?.unbind()
        }
        BSONBOO => {
            if position + 1 > end {
                return Err(invalid());
            }
            let byte = data[position];
            if byte != 0 && byte != 1 {
                return Err(invalid_bson(
                    py,
                    &format!("invalid boolean value: {byte:x}"),
                ));
            }
            position += 1;
            (byte == 1)
                .into_pyobject(py)?
                .to_owned()
                .into_any()
                .unbind()
        }
        BSONDAT => {
            if position + 8 > end {
                return Err(invalid());
            }
            let millis = i64::from_le_bytes(data[position..position + 8].try_into().unwrap());
            position += 8;
            decode_datetime(py, millis, options)?
        }
        BSONRGX => {
            // pattern cstr + flags cstr, both bounded by the element
            // extent (PYTHON-6110/6111).
            let pattern_end = find_nul(py, data, position, end)?;
            let pattern = decode_utf8(py, &data[position..pattern_end], options)?;
            let flags_start = pattern_end + 1;
            let flags_end = find_nul(py, data, flags_start, end)?;
            let mut flags: i64 = 0;
            for &byte in &data[flags_start..flags_end] {
                match byte {
                    b'i' => flags |= 2,
                    b'l' => flags |= 4,
                    b'm' => flags |= 8,
                    b's' => flags |= 16,
                    b'u' => flags |= 32,
                    b'x' => flags |= 64,
                    _ => {}
                }
            }
            position = flags_end + 1;
            let types = Types::get(py)?;
            types.regex.bind(py).call1((pattern, flags))?.unbind()
        }
        BSONREF => {
            // Deprecated DBPointer decodes to DBRef(collection, ObjectId).
            let length = read_string_length(py, data, position, end)?;
            let string_start = position + 4;
            // Encoded string length + string + 12 byte ObjectId.
            if string_start + length + 12 > end {
                return Err(invalid());
            }
            if data[string_start + length - 1] != 0 {
                return Err(invalid());
            }
            let collection =
                decode_utf8(py, &data[string_start..string_start + length - 1], options)?;
            let oid_start = string_start + length;
            let oid_bytes = PyBytes::new(py, &data[oid_start..oid_start + 12]);
            position = oid_start + 12;
            let types = Types::get(py)?;
            let oid = types.object_id.bind(py).call1((oid_bytes,))?;
            types.dbref.bind(py).call1((collection, oid))?.unbind()
        }
        BSONCOD => {
            let (code, next) = get_string_payload(py, data, position, end, options)?;
            position = next;
            let types = Types::get(py)?;
            vectorcall(py, &types.code, &[code.as_ptr()])?.unbind()
        }
        BSONCWS => {
            if position + 8 > end {
                return Err(invalid());
            }
            let cws_size = read_u32(py, data, position)? as usize;
            if cws_size > end - position {
                return Err(invalid());
            }
            let code_size = read_u32(py, data, position + 4)? as usize;
            // cws length + code length + code + scope length
            let min_len = 4 + 4 + code_size + 4;
            if code_size == 0
                || code_size > end - position
                || min_len > end - position
                || min_len < code_size
            {
                return Err(invalid());
            }
            let code_start = position + 8;
            if data[code_start + code_size - 1] != 0 {
                return Err(invalid());
            }
            let code = decode_utf8(py, &data[code_start..code_start + code_size - 1], options)?;
            let scope_start = code_start + code_size;
            let scope_size = read_u32(py, data, scope_start)? as usize;
            // code length + code + scope length + scope
            let total = 4 + 4 + code_size + scope_size;
            if scope_size < BSON_MIN_SIZE || total != cws_size || total < scope_size {
                return Err(invalid());
            }
            if data[scope_start + scope_size - 1] != 0 {
                return Err(invalid());
            }
            let scope = elements_to_dict(
                py,
                input,
                &data[scope_start..scope_start + scope_size],
                options,
            )
            .map_err(|err| rewrap_as_invalid_bson(py, err))
            .map(|(value, _)| value)?;
            position = scope_start + scope_size;
            let types = Types::get(py)?;
            types.code.bind(py).call1((code, scope))?.unbind()
        }
        BSONINT => {
            if position + 4 > end {
                return Err(invalid());
            }
            let v = i32::from_le_bytes(data[position..position + 4].try_into().unwrap());
            position += 4;
            v.into_pyobject(py)?.unbind().into_any()
        }
        BSONTIM => {
            if position + 8 > end {
                return Err(invalid());
            }
            let inc = u32::from_le_bytes(data[position..position + 4].try_into().unwrap());
            let time = u32::from_le_bytes(data[position + 4..position + 8].try_into().unwrap());
            position += 8;
            let time_obj = unsafe { pyo3::ffi::PyLong_FromUnsignedLong(time as _) };
            let inc_obj = unsafe { pyo3::ffi::PyLong_FromUnsignedLong(inc as _) };
            let time_obj = unsafe { Bound::from_owned_ptr_or_err(py, time_obj)? };
            let inc_obj = unsafe { Bound::from_owned_ptr_or_err(py, inc_obj)? };
            let types = Types::get(py)?;
            vectorcall(py, &types.timestamp, &[time_obj.as_ptr(), inc_obj.as_ptr()])?.unbind()
        }
        BSONLON => {
            if position + 8 > end {
                return Err(invalid());
            }
            let v = i64::from_le_bytes(data[position..position + 8].try_into().unwrap());
            position += 8;
            // int64 decodes to bson.int64.Int64, not plain int.
            let long = unsafe { pyo3::ffi::PyLong_FromLongLong(v) };
            let long = unsafe { Bound::from_owned_ptr_or_err(py, long)? };
            let types = Types::get(py)?;
            vectorcall(py, &types.int64, &[long.as_ptr()])?.unbind()
        }
        BSONDEC => {
            if position + 16 > end {
                return Err(invalid());
            }
            let bid = PyBytes::new(py, &data[position..position + 16]);
            position += 16;
            let from_bid = pyo3::intern!(py, "from_bid");
            let types = Types::get(py)?;
            vectorcall_method1(py, &types.decimal128, from_bid.as_any(), &bid.into_any())?.unbind()
        }
        BSONMIN => {
            let types = Types::get(py)?;
            vectorcall(py, &types.min_key, &[])?.unbind()
        }
        BSONMAX => {
            let types = Types::get(py)?;
            vectorcall(py, &types.max_key, &[])?.unbind()
        }
        _ => {
            let type_repr = PyBytes::new(py, &[type_byte])
                .repr()?
                .to_cow()?
                .into_owned();
            return Err(invalid_bson(
                py,
                &format!(
                    "Detected unknown BSON type {} for fieldname '{}'. \
                     Are you using the latest driver version?",
                    type_repr,
                    String::from_utf8_lossy(name),
                ),
            ));
        }
    };

    Ok((value, position))
}

/// Applies the type registry's decoder for the value's exact type, if
/// registered. Mirrors the C's decoder-map tail in get_value: its failures
/// are not wrapped here — document-level callers propagate them; the array
/// loop wraps, like the C's `goto invalid`.
pub fn apply_decoder<'py>(
    py: Python<'py>,
    value: Py<PyAny>,
    options: &CodecOptionsData<'py>,
) -> PyResult<Py<PyAny>> {
    if let Some(decoder_map) = &options.decoder_map_dict {
        let value_bound = value.bind(py);
        if let Some(converter) = decoder_map.get_item(value_bound.get_type())? {
            return Ok(converter.call1((value_bound,))?.unbind());
        }
    }
    Ok(value)
}

/// Constructs a `DatetimeMS` for the millis value (vectorcall, no tuple).
fn datetime_ms_from_millis<'py>(
    py: Python<'py>,
    millis: i64,
    types: &Types,
) -> PyResult<Py<PyAny>> {
    let long = unsafe { pyo3::ffi::PyLong_FromLongLong(millis) };
    let long = unsafe { Bound::from_owned_ptr_or_err(py, long)? };
    Ok(vectorcall(py, &types.datetime_ms, &[long.as_ptr()])?.unbind())
}

/// Converts a decoded `$ref`/`$id`-shaped document into a DBRef
/// (`_dbref_hook` in the C). The DBRef keys are removed from the document
/// and the remaining keys become `_extra`.
///
/// Only called for documents whose field names included a `$`-prefixed key
/// (a necessary condition for "$ref" and "$id" to be present).
fn dbref_hook<'py>(
    py: Python<'py>,
    value: Py<PyAny>,
    _options: &CodecOptionsData<'py>,
) -> PyResult<Py<PyAny>> {
    let doc = value.bind(py);
    let lookup = |key: &str| -> PyResult<Option<Bound<'py, PyAny>>> {
        // Exact-dict fast path: one raw-storage lookup serves both the
        // presence check and the value (PyMapping_HasKey + GetItem in the
        // C). Subclasses are excluded — their __getitem__ override must
        // win — and other mappings fall back to get_item with a caught
        // KeyError.
        if doc.is_exact_instance_of::<PyDict>() {
            let dict = doc.cast::<PyDict>().unwrap();
            let key = pyo3::types::PyString::intern(py, key);
            let item = unsafe { pyo3::ffi::PyDict_GetItemWithError(dict.as_ptr(), key.as_ptr()) };
            if item.is_null() {
                return if unsafe { pyo3::ffi::PyErr_Occurred() }.is_null() {
                    Ok(None)
                } else {
                    Err(PyErr::fetch(py))
                };
            }
            return unsafe { Ok(Some(Bound::from_borrowed_ptr(py, item))) };
        }
        match doc.get_item(key) {
            Ok(item) => Ok(Some(item)),
            Err(err) if err.is_instance_of::<pyo3::exceptions::PyKeyError>(py) => Ok(None),
            Err(err) => Err(err),
        }
    };
    let Some(reference) = lookup("$ref")? else {
        return Ok(value);
    };
    let Some(id) = lookup("$id")? else {
        return Ok(value);
    };
    let database = lookup("$db")?;
    // Type checks: $ref must be str; $db must be str, None (the key may be
    // present with a null value), or absent — matching the C's
    // `database == Py_None || PyUnicode_Check(database)`.
    if reference.cast::<PyString>().is_err() {
        return Ok(value);
    }
    if let Some(db) = &database {
        if !db.is_none() && db.cast::<PyString>().is_err() {
            return Ok(value);
        }
    }
    let types = Types::get(py)?;
    // Remove the DBRef keys from the document (the remainder becomes
    // DBRef's _extra), matching the C.
    doc.del_item(pyo3::intern!(py, "$ref"))?;
    doc.del_item(pyo3::intern!(py, "$id"))?;
    let database = match database {
        Some(db) => {
            doc.del_item(pyo3::intern!(py, "$db"))?;
            db
        }
        None => py.None().bind(py).clone(),
    };
    let dbref = types.dbref.bind(py).call1((reference, id, database, doc))?;
    Ok(dbref.unbind())
}

/// Finds the next NUL byte at or after `position`, bounded by `end`
/// (bounded scans, mirroring PYTHON-6110/6111).
#[inline(always)]
fn find_nul(py: Python<'_>, data: &[u8], position: usize, end: usize) -> PyResult<usize> {
    let stop = end.min(data.len());
    memchr::memchr(0, &data[position..stop])
        .map(|i| position + i)
        .ok_or_else(|| invalid_bson(py, "invalid length or type code"))
}

/// Reads a length-prefixed string payload, returning `(str, new_position)`.
#[inline(always)]
fn get_string_payload<'py>(
    py: Python<'py>,
    data: &[u8],
    position: usize,
    end: usize,
    options: &CodecOptionsData<'py>,
) -> PyResult<(Bound<'py, PyAny>, usize)> {
    let length = read_string_length(py, data, position, end)?;
    let string_start = position + 4;
    if string_start + length > end {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    if data[string_start + length - 1] != 0 {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    let slice = &data[string_start..string_start + length - 1];
    let string = decode_utf8(py, slice, options)?;
    Ok((string, string_start + length))
}

/// Datetime decoding with `datetime_conversion` mode support
/// (`decode_datetime` in the C).
fn decode_datetime<'py>(
    py: Python<'py>,
    mut millis: i64,
    options: &CodecOptionsData<'py>,
) -> PyResult<Py<PyAny>> {
    let types = Types::get(py)?;

    if options.datetime_conversion == DATETIME_MS {
        return datetime_ms_from_millis(py, millis, types);
    }

    if options.datetime_conversion == DATETIME_CLAMP || options.datetime_conversion == DATETIME_AUTO
    {
        // The representable range shrinks/grows with the target timezone.
        let (mut min_millis, mut max_millis) = (types.min_millis, types.max_millis);
        if options.tz_aware {
            if let Some(tzinfo) = &options.tzinfo {
                let min_offset = offset_millis(py, tzinfo, types.min_datetime.bind(py))?;
                let max_offset = offset_millis(py, tzinfo, types.max_datetime.bind(py))?;
                if min_offset < 0 {
                    min_millis -= min_offset;
                }
                if max_offset > 0 {
                    max_millis -= max_offset;
                }
            }
        }
        if options.datetime_conversion == DATETIME_CLAMP {
            millis = millis.clamp(min_millis, max_millis);
        } else if !(min_millis..=max_millis).contains(&millis) {
            return datetime_ms_from_millis(py, millis, types);
        }
    }

    let (year, month, day, hour, minute, second, microsecond) = millis_to_fields(millis);
    let tzinfo_arg = if options.tz_aware {
        Some(types.utc.bind(py).cast::<PyTzInfo>()?.clone())
    } else {
        None
    };
    let naive = match PyDateTime::new(
        py,
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        tzinfo_arg.as_ref(),
    ) {
        Ok(dt) => dt,
        // Out-of-range ValueError gets the DATETIME_AUTO suggestion
        // appended.
        Err(err) => return Err(append_datetime_suggestion(py, err)),
    };
    let value: Bound<'py, PyAny> = naive.into_any();
    if options.tz_aware {
        if let Some(target) = &options.tzinfo {
            return Ok(value
                .call_method1(pyo3::intern!(py, "astimezone"), (target,))?
                .unbind());
        }
    }
    Ok(value.unbind())
}

/// Appends the DATETIME_AUTO suggestion to out-of-range datetime
/// ValueErrors (mirrors `datetime_from_millis` in the C).
fn append_datetime_suggestion(py: Python<'_>, err: PyErr) -> PyErr {
    if !err.is_instance_of::<pyo3::exceptions::PyValueError>(py) {
        return err;
    }
    let message = err
        .value(py)
        .str()
        .and_then(|m| m.to_cow().map(|c| c.into_owned()))
        .unwrap_or_default();
    pyo3::exceptions::PyValueError::new_err(format!(
        "{message} (Consider Using CodecOptions(datetime_conversion=DATETIME_AUTO) \
         or MongoClient(datetime_conversion='DATETIME_AUTO')). \
         See: https://www.mongodb.com/docs/languages/python/pymongo-driver/current/data-formats/dates-and-times/#handling-out-of-range-datetimes"
    ))
}

/// Returns the utc offset of `at_datetime` under `tzinfo` in millis
/// (`tzinfo.utcoffset(at_datetime)`).
fn offset_millis<'py>(
    py: Python<'py>,
    tzinfo: &Bound<'py, PyAny>,
    at_datetime: &Bound<'py, PyAny>,
) -> PyResult<i64> {
    let offset = tzinfo.call_method1(pyo3::intern!(py, "utcoffset"), (at_datetime,))?;
    if offset.is_none() {
        return Ok(0);
    }
    let delta = offset
        .cast::<pyo3::types::PyDelta>()
        .map_err(|_| {
            bson_error(
                py,
                "BSONError",
                "tzinfo.utcoffset() did not return a datetime.timedelta",
            )
        })?
        .to_owned();
    let days = delta.get_days() as i64;
    let seconds = delta.get_seconds() as i64;
    let micros = delta.get_microseconds() as i64;
    Ok((days * 86_400 + seconds) * 1000 + micros / 1000)
}

/// Reads and validates a nested document size at `position`, bounded by
/// `end` (mirrors the C get_value's document case).
#[inline(always)]
fn read_size_at(py: Python<'_>, data: &[u8], position: usize, end: usize) -> PyResult<usize> {
    if position + 4 > end {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    let size = u32::from_le_bytes(data[position..position + 4].try_into().unwrap()) as usize;
    if size < BSON_MIN_SIZE || size > end - position {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    if data[position + size - 1] != 0 {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    Ok(size)
}

/// Reads a length-prefixed string's length (which includes the trailing
/// NUL), guarding against negative lengths (the C's `goto invalid`).
#[inline(always)]
fn read_string_length(py: Python<'_>, data: &[u8], position: usize, end: usize) -> PyResult<usize> {
    if position + 4 > end {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    let length = i32::from_le_bytes(data[position..position + 4].try_into().unwrap());
    if length < 1 {
        return Err(invalid_bson(py, "invalid length or type code"));
    }
    Ok(length as usize)
}

#[inline(always)]
fn read_u32(py: Python<'_>, data: &[u8], position: usize) -> PyResult<u32> {
    let bytes = data
        .get(position..position + 4)
        .ok_or_else(|| invalid_bson(py, "invalid length or type code"))?;
    Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
}

/// Calls `callee` with positional arguments through `PyObject_Vectorcall`
/// (no argument tuple allocation).
#[inline(always)]
fn vectorcall<'py>(
    py: Python<'py>,
    callee: &Py<PyAny>,
    args: &[*mut pyo3::ffi::PyObject],
) -> PyResult<Bound<'py, PyAny>> {
    unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            pyo3::ffi::PyObject_Vectorcall(
                callee.as_ptr(),
                args.as_ptr(),
                args.len(),
                std::ptr::null_mut(),
            ),
        )
    }
}

/// Calls `obj.<name>(arg)` through `PyObject_VectorcallMethod`.
#[inline(always)]
fn vectorcall_method1<'py>(
    py: Python<'py>,
    obj: &Py<PyAny>,
    name: &Bound<'py, PyAny>,
    arg: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let args = [obj.as_ptr(), arg.as_ptr()];
    unsafe {
        Bound::from_owned_ptr_or_err(
            py,
            pyo3::ffi::PyObject_VectorcallMethod(
                name.as_ptr(),
                args.as_ptr(),
                2,
                std::ptr::null_mut(),
            ),
        )
    }
}

/// Decodes UTF-8 bytes with the codec options' error handler.
///
/// Fast path: strict (the default) passes a NULL handler.
#[inline(always)]
pub fn decode_utf8<'py>(
    py: Python<'py>,
    bytes: &[u8],
    options: &CodecOptionsData<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let ptr = unsafe {
        pyo3::ffi::PyUnicode_DecodeUTF8(
            bytes.as_ptr() as *const _,
            bytes.len() as _,
            options
                .utf8_handler
                .as_deref()
                .map(|h| h.as_ptr())
                .unwrap_or(std::ptr::null()),
        )
    };
    let decoded = unsafe { Bound::from_owned_ptr_or_err(py, ptr)? };
    Ok(decoded.into_any())
}

/// A direct-mapped cache of raw key bytes to decoded strings: decoding and
/// hashing fresh strings for every dict insertion is a large share of the
/// decode loop, and a hit reuses the cached string's CPython hash. Repeat
/// decodes now return the same str object for a repeated name (benign;
/// strings are immutable). Enabled only for valid-UTF-8 keys under the
/// strict handler with no registered override, where decoding is a pure
/// function of the bytes. Like `Types`, the cache is process-wide:
/// importing the module into more than one interpreter is not supported.
const KEY_CACHE_SLOTS: usize = 1 << 13;
/// Keys longer than this skip the cache: caching them would cost as much
/// as the decode it avoids.
const KEY_CACHE_MAX_KEY_LEN: usize = 64;

/// A cache slot: the raw key bytes and the decoded string.
type KeyCacheEntry = (Box<[u8]>, Py<PyAny>);

struct KeyCache {
    slots: Vec<Option<KeyCacheEntry>>,
}

impl KeyCache {
    const fn new() -> Self {
        Self { slots: Vec::new() }
    }
}

static KEY_CACHE: Mutex<KeyCache> = Mutex::new(KeyCache::new());

/// FNV-1a: cheap and adequate for a direct-mapped cache.
#[inline]
fn key_hash(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[inline]
fn decode_key_cached<'py>(
    py: Python<'py>,
    bytes: &[u8],
    options: &CodecOptionsData<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut cache = KEY_CACHE.lock().unwrap_or_else(|err| err.into_inner());
    if cache.slots.len() != KEY_CACHE_SLOTS {
        cache.slots.resize_with(KEY_CACHE_SLOTS, || None);
    }
    let index = (key_hash(bytes) as usize) & (KEY_CACHE_SLOTS - 1);
    let slot = &mut cache.slots[index];
    if let Some((cached, string)) = slot {
        if cached.as_ref() == bytes {
            return Ok(string.bind(py).clone());
        }
    }
    let decoded = decode_utf8(py, bytes, options)?;
    *slot = Some((Box::from(bytes), decoded.clone().unbind()));
    Ok(decoded)
}

/// Decodes a field-name key, using the intern cache for valid-UTF-8 keys
/// under the strict handler. Anything else can invoke a registered
/// "strict" handler (which may re-enter decoding or make decoding impure),
/// so it bypasses the cache.
#[inline]
pub fn decode_key<'py>(
    py: Python<'py>,
    bytes: &[u8],
    options: &CodecOptionsData<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    if options.utf8_handler.is_none()
        && bytes.len() <= KEY_CACHE_MAX_KEY_LEN
        && std::str::from_utf8(bytes).is_ok()
    {
        return decode_key_cached(py, bytes, options);
    }
    decode_utf8(py, bytes, options)
}

/// Wraps unexpected errors during value decoding in InvalidBSON, mirroring
/// `_rewrap_as_invalid_bson` in the C: only `Exception` subclasses are
/// wrapped, and an `InvalidBSON` is returned as-is.
#[cold]
pub fn rewrap_as_invalid_bson(py: Python<'_>, err: PyErr) -> PyErr {
    if !err
        .value(py)
        .is_instance_of::<pyo3::exceptions::PyException>()
    {
        return err;
    }
    let is_invalid_bson = Types::get(py)
        .ok()
        .and_then(|types| err.value(py).is_instance(types.invalid_bson.bind(py)).ok())
        .unwrap_or(false);
    if is_invalid_bson {
        return err;
    }
    let message = err
        .value(py)
        .str()
        .and_then(|m| m.to_cow().map(|c| c.into_owned()))
        .unwrap_or_default();
    bson_error(py, "InvalidBSON", &message)
}

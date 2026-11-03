//! Parsing of `bson.CodecOptions` (a `NamedTuple`) into a Rust-side struct.
//!
//! Mirrors `convert_codec_options` in `_cbsonmodule.c`: the options object is
//! consumed positionally with the field order
//! `(document_class, tz_aware, uuid_representation,
//!   unicode_decode_error_handler, tzinfo, type_registry, datetime_conversion)`.

use std::ffi::CString;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyInt, PyString, PyTuple};
use pyo3::PyTypeInfo;

/// The BSON type marker of `RawBSONDocument` (`bson/codec_options.py`).
pub const RAW_BSON_DOCUMENT_MARKER: i64 = 101;

// `DatetimeConversion` enum values (an int enum in bson/codec_options.py).
pub const DATETIME_MS: u8 = 3;
pub const DATETIME_CLAMP: u8 = 2;
pub const DATETIME_AUTO: u8 = 4;

/// Parsed codec options, valid for the lifetime of the enclosing Python call.
pub struct CodecOptionsData<'py> {
    /// The document class (e.g. `dict`, `SON`, `RawBSONDocument`).
    pub document_class: Bound<'py, PyAny>,
    pub tz_aware: bool,
    pub uuid_representation: u8,
    pub tzinfo: Option<Bound<'py, PyAny>>,
    pub datetime_conversion: u8,
    /// True when `document_class` is a raw BSON document class.
    pub is_raw_bson: bool,
    /// True when `document_class` is exactly `dict`.
    pub is_dict_class: bool,
    /// Cached `type_registry._encoder_map` (a dict, possibly empty).
    encoder_map: Bound<'py, PyAny>,
    /// Cached `type_registry._fallback_encoder` (None when unset).
    fallback_encoder: Bound<'py, PyAny>,
    /// The original CodecOptions object (passed to raw document classes).
    options_obj: Bound<'py, PyAny>,
    /// The decoder map as a dict, resolved once (None when empty).
    pub decoder_map_dict: Option<Bound<'py, PyDict>>,
    /// The UTF-8 decode error handler, resolved once (None = strict).
    pub utf8_handler: Option<CString>,
}

impl<'py> CodecOptionsData<'py> {
    /// The original options object, passed to raw document constructors.
    pub fn options_obj(&self, _py: Python<'_>) -> Bound<'py, PyAny> {
        self.options_obj.clone()
    }
}

/// Reads the `_type_marker` attribute of an object.
///
/// Mirrors `_type_marker()` in `_cbsonmodule.c`: the attribute is read
/// optionally and only accepted when it is an *exact* `int` (objects with
/// broken `__getattr__` implementations could return arbitrary types).
/// Returns `None` when the marker is absent or not an exact int; returns
/// `Err` only when the attribute read itself fails with a non-AttributeError.
pub fn read_type_marker<'py>(object: &Bound<'py, PyAny>) -> PyResult<Option<i64>> {
    let py = object.py();
    #[cfg(Py_3_13)]
    let marker = {
        // PyObject_GetOptionalAttr (the C's 3.13+ path) finds the attribute
        // without materializing an AttributeError — the common case is a
        // document_class without a marker (plain dict).
        let intern = pyo3::intern!(py, "_type_marker");
        let mut raw: *mut pyo3::ffi::PyObject = std::ptr::null_mut();
        let found = unsafe {
            pyo3::ffi::PyObject_GetOptionalAttr(object.as_ptr(), intern.as_ptr(), &mut raw)
        };
        if found < 0 {
            return Err(PyErr::fetch(py));
        }
        if found == 0 {
            None
        } else {
            Some(unsafe { Bound::from_owned_ptr_or_err(py, raw) }?)
        }
    };
    #[cfg(not(Py_3_13))]
    let marker = match object.getattr(pyo3::intern!(py, "_type_marker")) {
        Ok(marker) => Some(marker),
        Err(err) => {
            if err.is_instance_of::<pyo3::exceptions::PyAttributeError>(py) {
                None
            } else {
                return Err(err);
            }
        }
    };
    if let Some(marker) = marker {
        if marker.is_instance_of::<PyInt>() && marker.get_type().is(PyInt::type_object(py)) {
            // Exact int (not a bool or int subclass).
            if let Ok(value) = marker.extract::<i64>() {
                return Ok(Some(value));
            }
        }
    }
    Ok(None)
}

impl<'py> CodecOptionsData<'py> {
    /// Parses a `CodecOptions` (NamedTuple) instance positionally.
    pub fn parse(options_obj: &Bound<'py, PyAny>) -> PyResult<Self> {
        let py = options_obj.py();
        // Direct tuple access (mirrors PyArg_ParseTuple on the NamedTuple);
        // an out-of-line sequence would add per-field method dispatch.
        let seq = options_obj
            .cast::<PyTuple>()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("codec_options must be a tuple"))?;
        if seq.len() < 7 {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "tuple index out of range",
            ));
        }

        let document_class = unsafe { seq.get_item_unchecked(0) };
        let tz_aware: bool = unsafe { seq.get_item_unchecked(1) }.extract()?;
        let uuid_representation: u8 = unsafe { seq.get_item_unchecked(2) }.extract()?;
        let unicode_decode_error_handler = unsafe { seq.get_item_unchecked(3) };
        let tzinfo_obj = unsafe { seq.get_item_unchecked(4) };
        let type_registry = unsafe { seq.get_item_unchecked(5) };
        let datetime_conversion: u8 = unsafe { seq.get_item_unchecked(6) }.extract()?;

        let is_raw_bson = matches!(
            read_type_marker(&document_class)?,
            Some(RAW_BSON_DOCUMENT_MARKER)
        );
        let is_dict_class = document_class.is(PyDict::type_object(py));

        let encoder_map = type_registry.getattr(pyo3::intern!(py, "_encoder_map"))?;
        let fallback_encoder = type_registry.getattr(pyo3::intern!(py, "_fallback_encoder"))?;
        let decoder_map = type_registry.getattr(pyo3::intern!(py, "_decoder_map"))?;

        let decoder_map_dict = {
            let dict = decoder_map.cast::<PyDict>().ok().cloned();
            match dict {
                Some(d) if d.is_empty() => None,
                other => other,
            }
        };
        let utf8_handler = {
            let handler = unicode_decode_error_handler
                .cast::<PyString>()
                .ok()
                .cloned();
            match handler {
                Some(h) => {
                    // Mirrors the C's PyArg_ParseTuple "z" format, which
                    // fails the options conversion when the handler cannot
                    // be converted: an unencodable (surrogate) handler
                    // raises UnicodeEncodeError from to_str, and an
                    // embedded NUL is a ValueError (matching CPython's
                    // "embedded null character"). Python's CodecOptions
                    // constructor only checks isinstance(str), so both
                    // are reachable; silently treating either as strict
                    // would change decoding behavior.
                    let handler = h.to_str()?;
                    if handler == "strict" {
                        None
                    } else {
                        Some(CString::new(handler.to_string()).map_err(|_| {
                            pyo3::exceptions::PyValueError::new_err("embedded null character")
                        })?)
                    }
                }
                None => None,
            }
        };

        Ok(CodecOptionsData {
            document_class,
            tz_aware,
            uuid_representation,
            tzinfo: if tzinfo_obj.is_none() {
                None
            } else {
                Some(tzinfo_obj)
            },
            datetime_conversion,
            is_raw_bson,
            is_dict_class,
            encoder_map,
            fallback_encoder,
            options_obj: options_obj.clone(),
            decoder_map_dict,
            utf8_handler,
        })
    }

    /// The encoder map as a dict-like object, or None when empty.
    ///
    /// Mirrors the C's `is_encoder_empty` check: an empty map is skipped
    /// without a per-value lookup.
    pub fn encoder_map(&self, _py: Python<'_>) -> Option<Bound<'py, PyDict>> {
        // TypeRegistry always builds _encoder_map as a dict.
        let dict = self.encoder_map.cast::<PyDict>().ok()?.clone();
        if dict.is_empty() {
            None
        } else {
            Some(dict)
        }
    }

    /// The fallback encoder, or None when unset.
    pub fn fallback_encoder(&self, _py: Python<'_>) -> Option<Bound<'py, PyAny>> {
        if self.fallback_encoder.is_none() {
            None
        } else {
            Some(self.fallback_encoder.clone())
        }
    }
}

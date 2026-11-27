//! Python type objects needed by the codec, imported once and cached.
//!
//! Mirrors the module-state caching in `_cbsonmodule.c` (which resolves
//! `bson.int64.Int64`, `re`'s pattern type, etc. at module init).

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

pub struct Types {
    /// `bson.int64.Int64`
    pub int64: Py<PyAny>,
    /// `bson.objectid.ObjectId`
    pub object_id: Py<PyAny>,
    /// `bson.binary.Binary`
    pub binary: Py<PyAny>,
    /// `bson.code.Code`
    pub code: Py<PyAny>,
    /// `bson.dbref.DBRef`
    pub dbref: Py<PyAny>,
    /// `bson.regex.Regex`
    pub regex: Py<PyAny>,
    /// `bson.timestamp.Timestamp`
    pub timestamp: Py<PyAny>,
    /// `bson.decimal128.Decimal128`
    pub decimal128: Py<PyAny>,
    /// `bson.min_key.MinKey`
    pub min_key: Py<PyAny>,
    /// `bson.max_key.MaxKey`
    pub max_key: Py<PyAny>,
    /// `bson.datetime_ms.DatetimeMS`
    pub datetime_ms: Py<PyAny>,
    /// `uuid.UUID`
    pub uuid: Py<PyAny>,
    /// The type of `re.compile("")` (i.e. `re.Pattern`).
    pub re_type: Py<PyAny>,
    /// `collections.abc.Mapping`
    pub mapping: Py<PyAny>,
    /// `bson.tz_util.utc` (the canonical UTC FixedOffset).
    pub utc: Py<PyAny>,
    /// `bson.errors.InvalidDocument`
    pub invalid_document: Py<PyAny>,
    /// `bson.errors.InvalidBSON`
    pub invalid_bson: Py<PyAny>,
    /// `bson.datetime_ms._MIN_UTC_MS`
    pub min_millis: i64,
    /// `bson.datetime_ms._MAX_UTC_MS`
    pub max_millis: i64,
    /// `bson.datetime_ms._MIN_UTC`
    pub min_datetime: Py<PyAny>,
    /// `bson.datetime_ms._MAX_UTC`
    pub max_datetime: Py<PyAny>,
    /// `bson.codec_options._RAW_BSON_VIEW_THRESHOLD`
    pub raw_bson_view_threshold: usize,
}

fn import_type(py: Python<'_>, module: &str, attr: &str) -> PyResult<Py<PyAny>> {
    Ok(py.import(module)?.getattr(attr)?.unbind())
}

static TYPES: PyOnceLock<Types> = PyOnceLock::new();

impl Types {
    /// Returns the cached type objects, importing them on first use.
    pub fn get(py: Python<'_>) -> PyResult<&'static Types> {
        TYPES.get_or_try_init(py, || {
            let re_module = py.import("re")?;
            let re_type: Py<PyAny> = re_module
                .getattr("compile")?
                .call(("", 0), None)?
                .get_type()
                .into_any()
                .unbind();
            Ok(Types {
                int64: import_type(py, "bson.int64", "Int64")?,
                object_id: import_type(py, "bson.objectid", "ObjectId")?,
                binary: import_type(py, "bson.binary", "Binary")?,
                code: import_type(py, "bson.code", "Code")?,
                dbref: import_type(py, "bson.dbref", "DBRef")?,
                regex: import_type(py, "bson.regex", "Regex")?,
                timestamp: import_type(py, "bson.timestamp", "Timestamp")?,
                decimal128: import_type(py, "bson.decimal128", "Decimal128")?,
                min_key: import_type(py, "bson.min_key", "MinKey")?,
                max_key: import_type(py, "bson.max_key", "MaxKey")?,
                datetime_ms: import_type(py, "bson.datetime_ms", "DatetimeMS")?,
                uuid: import_type(py, "uuid", "UUID")?,
                re_type,
                mapping: import_type(py, "collections.abc", "Mapping")?,
                utc: import_type(py, "bson.tz_util", "utc")?,
                invalid_document: import_type(py, "bson.errors", "InvalidDocument")?,
                invalid_bson: import_type(py, "bson.errors", "InvalidBSON")?,
                min_millis: import_type(py, "bson.datetime_ms", "_MIN_UTC_MS")?
                    .bind(py)
                    .extract()?,
                max_millis: import_type(py, "bson.datetime_ms", "_MAX_UTC_MS")?
                    .bind(py)
                    .extract()?,
                min_datetime: import_type(py, "bson.datetime_ms", "_MIN_UTC")?,
                max_datetime: import_type(py, "bson.datetime_ms", "_MAX_UTC")?,
                raw_bson_view_threshold: import_type(
                    py,
                    "bson.codec_options",
                    "_RAW_BSON_VIEW_THRESHOLD",
                )?
                .bind(py)
                .extract()?,
            })
        })
    }
}

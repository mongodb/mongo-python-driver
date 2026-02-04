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

//! Type cache for Python type objects
//!
//! This module provides a cache for Python type objects to avoid repeated imports.
//! This matches the C extension's approach of caching all BSON types at module initialization.

use once_cell::sync::OnceCell;
use pyo3::prelude::*;
use pyo3::types::PyAny;

/// Cache for Python type objects to avoid repeated imports
/// This matches the C extension's approach of caching all BSON types at module initialization
pub(crate) struct TypeCache {
    // Standard library types
    pub(crate) uuid_class: OnceCell<PyObject>,
    pub(crate) datetime_class: OnceCell<PyObject>,
    pub(crate) pattern_class: OnceCell<PyObject>,

    // BSON types
    pub(crate) binary_class: OnceCell<PyObject>,
    pub(crate) code_class: OnceCell<PyObject>,
    pub(crate) objectid_class: OnceCell<PyObject>,
    pub(crate) dbref_class: OnceCell<PyObject>,
    pub(crate) regex_class: OnceCell<PyObject>,
    pub(crate) timestamp_class: OnceCell<PyObject>,
    pub(crate) int64_class: OnceCell<PyObject>,
    pub(crate) decimal128_class: OnceCell<PyObject>,
    pub(crate) minkey_class: OnceCell<PyObject>,
    pub(crate) maxkey_class: OnceCell<PyObject>,
    pub(crate) datetime_ms_class: OnceCell<PyObject>,

    // Utility objects
    pub(crate) utc: OnceCell<PyObject>,
    pub(crate) calendar_timegm: OnceCell<PyObject>,

    // Error classes
    pub(crate) invalid_document_class: OnceCell<PyObject>,
    pub(crate) invalid_bson_class: OnceCell<PyObject>,

    // Fallback decoder
    pub(crate) bson_to_dict_python: OnceCell<PyObject>,
}

pub(crate) static TYPE_CACHE: TypeCache = TypeCache {
    uuid_class: OnceCell::new(),
    datetime_class: OnceCell::new(),
    pattern_class: OnceCell::new(),
    binary_class: OnceCell::new(),
    code_class: OnceCell::new(),
    objectid_class: OnceCell::new(),
    dbref_class: OnceCell::new(),
    regex_class: OnceCell::new(),
    timestamp_class: OnceCell::new(),
    int64_class: OnceCell::new(),
    decimal128_class: OnceCell::new(),
    minkey_class: OnceCell::new(),
    maxkey_class: OnceCell::new(),
    datetime_ms_class: OnceCell::new(),
    utc: OnceCell::new(),
    calendar_timegm: OnceCell::new(),
    invalid_document_class: OnceCell::new(),
    invalid_bson_class: OnceCell::new(),
    bson_to_dict_python: OnceCell::new(),
};

impl TypeCache {
    /// Get or initialize the UUID class
    pub(crate) fn get_uuid_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.uuid_class.get_or_try_init(|| {
            py.import_bound("uuid")?
                .getattr("UUID")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the datetime class
    pub(crate) fn get_datetime_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.datetime_class.get_or_try_init(|| {
            py.import_bound("datetime")?
                .getattr("datetime")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the regex Pattern class
    pub(crate) fn get_pattern_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.pattern_class.get_or_try_init(|| {
            py.import_bound("re")?
                .getattr("Pattern")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Binary class
    pub(crate) fn get_binary_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.binary_class.get_or_try_init(|| {
            py.import_bound("bson.binary")?
                .getattr("Binary")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Code class
    pub(crate) fn get_code_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.code_class.get_or_try_init(|| {
            py.import_bound("bson.code")?
                .getattr("Code")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the ObjectId class
    pub(crate) fn get_objectid_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.objectid_class.get_or_try_init(|| {
            py.import_bound("bson.objectid")?
                .getattr("ObjectId")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the DBRef class
    pub(crate) fn get_dbref_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.dbref_class.get_or_try_init(|| {
            py.import_bound("bson.dbref")?
                .getattr("DBRef")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Regex class
    pub(crate) fn get_regex_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.regex_class.get_or_try_init(|| {
            py.import_bound("bson.regex")?
                .getattr("Regex")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Timestamp class
    pub(crate) fn get_timestamp_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.timestamp_class.get_or_try_init(|| {
            py.import_bound("bson.timestamp")?
                .getattr("Timestamp")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Int64 class
    pub(crate) fn get_int64_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.int64_class.get_or_try_init(|| {
            py.import_bound("bson.int64")?
                .getattr("Int64")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Decimal128 class
    pub(crate) fn get_decimal128_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.decimal128_class.get_or_try_init(|| {
            py.import_bound("bson.decimal128")?
                .getattr("Decimal128")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the MinKey class
    pub(crate) fn get_minkey_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.minkey_class.get_or_try_init(|| {
            py.import_bound("bson.min_key")?
                .getattr("MinKey")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the MaxKey class
    pub(crate) fn get_maxkey_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.maxkey_class.get_or_try_init(|| {
            py.import_bound("bson.max_key")?
                .getattr("MaxKey")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the DatetimeMS class
    pub(crate) fn get_datetime_ms_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.datetime_ms_class.get_or_try_init(|| {
            py.import_bound("bson.datetime_ms")?
                .getattr("DatetimeMS")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the UTC timezone object
    pub(crate) fn get_utc(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.utc.get_or_try_init(|| {
            py.import_bound("bson.tz_util")?
                .getattr("utc")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize calendar.timegm function
    pub(crate) fn get_calendar_timegm(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.calendar_timegm.get_or_try_init(|| {
            py.import_bound("calendar")?
                .getattr("timegm")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize InvalidDocument exception class
    pub(crate) fn get_invalid_document_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.invalid_document_class.get_or_try_init(|| {
            py.import_bound("bson.errors")?
                .getattr("InvalidDocument")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize InvalidBSON exception class
    pub(crate) fn get_invalid_bson_class(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.invalid_bson_class.get_or_try_init(|| {
            py.import_bound("bson.errors")?
                .getattr("InvalidBSON")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }

    /// Get or initialize the Python fallback decoder
    pub(crate) fn get_bson_to_dict_python(&self, py: Python) -> PyResult<Py<PyAny>> {
        Ok(self.bson_to_dict_python.get_or_try_init(|| {
            py.import_bound("bson")?
                .getattr("_bson_to_dict_python")
                .map(|c| c.unbind())
        })?.clone_ref(py))
    }
}

// Type markers for BSON objects
pub(crate) const BINARY_TYPE_MARKER: i32 = 5;
pub(crate) const OBJECTID_TYPE_MARKER: i32 = 7;
pub(crate) const DATETIME_TYPE_MARKER: i32 = 9;
pub(crate) const REGEX_TYPE_MARKER: i32 = 11;
pub(crate) const CODE_TYPE_MARKER: i32 = 13;
pub(crate) const SYMBOL_TYPE_MARKER: i32 = 14;
pub(crate) const DBPOINTER_TYPE_MARKER: i32 = 15;
pub(crate) const TIMESTAMP_TYPE_MARKER: i32 = 17;
pub(crate) const INT64_TYPE_MARKER: i32 = 18;
pub(crate) const DECIMAL128_TYPE_MARKER: i32 = 19;
pub(crate) const DBREF_TYPE_MARKER: i32 = 100;
pub(crate) const MAXKEY_TYPE_MARKER: i32 = 127;
pub(crate) const MINKEY_TYPE_MARKER: i32 = 255;

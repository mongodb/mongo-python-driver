//! Raising `bson.errors` exceptions from Rust.
//!
//! Mirrors `_cbsonmodule.c`'s `_error()` helper: the exception classes are
//! looked up on the `bson.errors` module. Error paths are cold, so a
//! per-call module lookup (cached after first use) is fine.

use pyo3::sync::PyOnceLock;

use pyo3::prelude::*;

static ERRORS_MODULE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Returns the `bson.errors` module, importing and caching it on first use.
fn errors_module(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    ERRORS_MODULE
        .get_or_try_init(py, || {
            let module = py.import("bson.errors")?;
            Ok::<_, PyErr>(module.into_any().unbind())
        })
        .map(|m| m.bind(py))
}

/// Builds a `PyErr` for `bson.errors.<name>` with `message`.
#[cold]
pub fn bson_error(py: Python<'_>, name: &str, message: &str) -> PyErr {
    let error = errors_module(py)
        .and_then(|m| m.getattr(name))
        .and_then(|exc| exc.call1((message,)));
    match error {
        Ok(value) => PyErr::from_value(value),
        Err(_) => pyo3::exceptions::PyRuntimeError::new_err(message.to_string()),
    }
}

/// Shorthand for `bson.errors.InvalidBSON`.
pub fn invalid_bson(py: Python<'_>, message: &str) -> PyErr {
    bson_error(py, "InvalidBSON", message)
}

/// Shorthand for `bson.errors.InvalidDocument`.
pub fn invalid_document(py: Python<'_>, message: &str) -> PyErr {
    bson_error(py, "InvalidDocument", message)
}

/// Shorthand for `bson.errors.InvalidStringData`.
pub fn invalid_string_data(py: Python<'_>, message: &str) -> PyErr {
    bson_error(py, "InvalidStringData", message)
}

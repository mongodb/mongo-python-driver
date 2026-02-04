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

//! Error handling utilities for BSON operations

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyTuple};

use crate::types::TYPE_CACHE;

/// Helper to create InvalidDocument exception
pub(crate) fn invalid_document_error(py: Python, msg: String) -> PyErr {
    let invalid_document = TYPE_CACHE.get_invalid_document_class(py)
        .expect("Failed to get InvalidDocument class");
    PyErr::from_value(
        invalid_document.bind(py)
            .call1((msg,))
            .expect("Failed to create InvalidDocument")
    )
}

/// Helper to create InvalidDocument exception with document property
pub(crate) fn invalid_document_error_with_doc(py: Python, msg: String, doc: &Bound<'_, PyAny>) -> PyErr {
    let invalid_document = TYPE_CACHE.get_invalid_document_class(py)
        .expect("Failed to get InvalidDocument class");
    // Call with positional arguments: InvalidDocument(message, document)
    let args = PyTuple::new_bound(py, &[msg.into_py(py), doc.clone().into_py(py)]);
    PyErr::from_value(
        invalid_document.bind(py)
            .call1(args)
            .expect("Failed to create InvalidDocument")
    )
}

/// Helper to create InvalidBSON exception
pub(crate) fn invalid_bson_error(py: Python, msg: String) -> PyErr {
    let invalid_bson = TYPE_CACHE.get_invalid_bson_class(py)
        .expect("Failed to get InvalidBSON class");
    PyErr::from_value(
        invalid_bson.bind(py)
            .call1((msg,))
            .expect("Failed to create InvalidBSON")
    )
}

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

//! Rust implementation of BSON encoding/decoding functions
//!
//! ⚠️ **NOT PRODUCTION READY** - Experimental implementation with incomplete features.
//!
//! This module provides a **partial implementation** of the C extension (bson._cbson)
//! interface, implemented in Rust using PyO3 and the bson library.
//!
//! # Implementation Status
//!
//! - ✅ Core BSON encoding/decoding: 86/88 tests passing
//! - ❌ Custom type encoders: NOT IMPLEMENTED (~85 tests skipped)
//! - ❌ RawBSONDocument: NOT IMPLEMENTED
//! - ❌ Performance: ~5x slower than C extension
//!
//! # Implementation History
//!
//! This implementation was developed as part of PYTHON-5683 to investigate
//! using Rust as an alternative to C for Python extension modules.
//!
//! See PR #2695 for the complete implementation history, including:
//! - Initial implementation with core BSON functionality
//! - Performance optimizations (type caching, fast paths, direct conversions)
//! - Modular refactoring (split into 6 modules)
//! - Test skip markers for unimplemented features
//!
//! # Performance
//!
//! Current performance: ~0.21x (5x slower than C extension)
//! Root cause: Architectural difference (Python ↔ Bson ↔ bytes vs Python ↔ bytes)
//! See README.md for detailed performance analysis and optimization opportunities.
//!
//! # Module Structure
//!
//! The codebase is organized into the following modules:
//! - `types`: Type cache and BSON type markers
//! - `errors`: Error handling utilities
//! - `utils`: Utility functions (datetime, regex, validation, string writing)
//! - `encode`: BSON encoding functions
//! - `decode`: BSON decoding functions

#![allow(clippy::useless_conversion)]

mod types;
mod errors;
mod utils;
mod encode;
mod decode;

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Test function to verify the Rust extension is loaded
#[pyfunction]
fn _test_rust_extension(py: Python) -> PyResult<PyObject> {
    let result = PyDict::new(py);
    result.set_item("implementation", "rust")?;
    result.set_item("version", "0.1.0")?;
    result.set_item("status", "experimental")?;
    result.set_item("pyo3_version", env!("CARGO_PKG_VERSION"))?;
    Ok(result.into())
}

/// Python module definition
#[pymodule]
fn _rbson(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode::_dict_to_bson, m)?)?;
    m.add_function(wrap_pyfunction!(encode::_dict_to_bson_direct, m)?)?;
    m.add_function(wrap_pyfunction!(decode::_bson_to_dict, m)?)?;
    m.add_function(wrap_pyfunction!(_test_rust_extension, m)?)?;
    Ok(())
}

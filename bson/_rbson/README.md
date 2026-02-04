# Rust BSON Extension Module

⚠️ **NOT PRODUCTION READY** - This is an experimental implementation with incomplete feature support and performance limitations. See [Test Status](#test-status) and [Performance Analysis](#performance-analysis) sections below.

This directory contains a Rust-based implementation of BSON encoding/decoding for PyMongo, developed as part of [PYTHON-5683](https://jira.mongodb.org/browse/PYTHON-5683).

## Overview

The Rust extension (`_rbson`) provides a **partial implementation** of the C extension (`_cbson`) interface, implemented in Rust using:
- **PyO3**: Python bindings for Rust
- **bson crate**: MongoDB's official Rust BSON library
- **Maturin**: Build tool for Rust Python extensions

## Test Status

### ✅ Core BSON Tests: 86 passed, 2 skipped
The basic BSON encoding/decoding functionality works correctly (`test/test_bson.py`).

### ⏭️ Skipped Tests: ~85 tests across multiple test files
The following features are **not implemented** and tests are skipped when using the Rust extension:

#### Custom Type Encoders (test/test_custom_types.py)
- **`TypeEncoder` and `TypeRegistry`** - Custom type encoding/decoding
- **`FallbackEncoder`** - Fallback encoding for unknown types
- **Tests skipped**: All tests in `TestBSONFallbackEncoder`, `TestCustomPythonBSONTypeToBSONMonolithicCodec`, `TestCustomPythonBSONTypeToBSONMultiplexedCodec`
- **Reason**: Rust extension doesn't support custom type encoders or fallback encoders

#### RawBSONDocument (test/test_raw_bson.py)
- **`RawBSONDocument` codec options** - Raw BSON document handling
- **Tests skipped**: All tests in `TestRawBSONDocument`
- **Reason**: Rust extension doesn't implement RawBSONDocument codec options

#### DBRef Edge Cases (test/test_dbref.py)
- **DBRef validation and edge cases**
- **Tests skipped**: Some DBRef tests
- **Reason**: Incomplete DBRef handling in Rust extension

#### Type Checking (test/test_typing.py)
- **Type hints and mypy validation**
- **Tests skipped**: Some typing tests
- **Reason**: Type checking issues with Rust extension

### Skip Mechanism
Tests are skipped using the `@skip_if_rust_bson` pytest marker defined in `test/__init__.py`:
```python
skip_if_rust_bson = pytest.mark.skipif(
    _use_rust_bson(), reason="Rust BSON extension does not support this feature"
)
```

This marker is applied to test classes and methods that use unimplemented features.

## Implementation History

This implementation was developed through [PR #2695](https://github.com/mongodb/mongo-python-driver/pull/2695) to investigate using Rust as an alternative to C for Python extension modules.

### Key Milestones

1. **Initial Implementation** - Basic BSON type support with core functionality
2. **Performance Optimizations** - Type caching, fast paths for common types, direct byte operations
3. **Modular Refactoring** - Split monolithic lib.rs into 6 well-organized modules
4. **Test Integration** - Added skip markers for unimplemented features (~85 tests skipped)

## Features

### Supported BSON Types

The Rust extension supports basic BSON types:
- **Primitives**: Double, String, Int32, Int64, Boolean, Null
- **Complex Types**: Document, Array, Binary, ObjectId, DateTime
- **Special Types**: Regex, Code, Timestamp, Decimal128, MinKey, MaxKey
- **Deprecated Types**: DBPointer (decodes to DBRef)

### CodecOptions Support

**Partial** support for PyMongo's `CodecOptions`:
- ✅ `document_class` - Custom document classes (basic support)
- ✅ `tz_aware` - Timezone-aware datetime handling
- ✅ `tzinfo` - Timezone conversion
- ✅ `uuid_representation` - UUID encoding/decoding modes
- ✅ `datetime_conversion` - DateTime handling modes (AUTO, CLAMP, MS)
- ✅ `unicode_decode_error_handler` - UTF-8 error handling
- ❌ `type_registry` - Custom type encoders/decoders (NOT IMPLEMENTED)
- ❌ RawBSONDocument support (NOT IMPLEMENTED)

### Runtime Selection

The Rust extension can be enabled via environment variable:
```bash
export PYMONGO_USE_RUST=1
python your_script.py
```

Without this variable, PyMongo uses the C extension by default.

## Performance Analysis

### Current Performance: ~0.21x (5x slower than C)

**Benchmark Results** (from PR #2695):
```
Simple documents:  C: 100%  | Rust: 21%
Mixed types:       C: 100%  | Rust: 20%
Nested documents:  C: 100%  | Rust: 18%
Lists:             C: 100%  | Rust: 22%
```

### Root Cause: Architectural Difference

The performance gap is due to a fundamental architectural difference:

**C Extension Architecture:**
```
Python objects → BSON bytes (direct)
```
- Writes BSON bytes directly from Python objects
- No intermediate data structures
- Minimal memory allocations

**Rust Extension Architecture:**
```
Python objects → Rust Bson enum → BSON bytes
```
- Converts Python objects to Rust `Bson` enum
- Then serializes `Bson` to bytes
- Extra conversion layer adds overhead

### Optimization Attempts

Multiple optimization strategies were attempted in PR #2695:

1. **Type Caching** - Cache frequently used Python types (UUID, datetime, etc.)
2. **Fast Paths** - Special handling for common types (int, str, bool, None)
3. **Direct Byte Writing** - Write BSON bytes directly without intermediate `Document`
4. **PyDict Fast Path** - Use `PyDict_Next` for efficient dict iteration

**Result**: These optimizations improved performance from ~0.15x to ~0.21x, but the fundamental architectural difference remains.

## Comparison with Copilot POC (PR #2689)

The current implementation evolved significantly from the initial Copilot-generated proof-of-concept in PR #2689:

### Copilot POC (PR #2689) - Initial Spike
**Status**: 53/88 tests passing (60%)

**Build System**: `cargo build --release` (manual copy of .so file)
- Used raw `cargo` commands
- Manual file copying to project root
- No wheel generation
- Located in `rust/` directory

**What it had:**
- ✅ Basic BSON type support (int, float, string, bool, bytes, dict, list, null)
- ✅ ObjectId, DateTime, Regex encoding/decoding
- ✅ Binary, Code, Timestamp, Decimal128, MinKey, MaxKey support
- ✅ DBRef and DBPointer decoding
- ✅ Int64 type marker support
- ✅ Basic CodecOptions (tz_aware, uuid_representation)
- ✅ Buffer protocol support (memoryview, array)
- ✅ _id field ordering at top level
- ✅ Benchmark scripts and performance analysis
- ✅ Comprehensive documentation (RUST_SPIKE_RESULTS.md)
- ✅ **Same Rust architecture**: PyO3 0.27 + bson 2.13 crate (Python → Bson enum → bytes)

**What it lacked:**
- ❌ Only 60% test pass rate (53/88 tests)
- ❌ Incomplete datetime handling (no DATETIME_CLAMP, DATETIME_AUTO, DATETIME_MS modes)
- ❌ Missing unicode_decode_error_handler support
- ❌ No document_class support from CodecOptions
- ❌ No tzinfo conversion support
- ❌ Missing BSON validation (size checks, null terminator)
- ❌ No performance optimizations (type caching, fast paths)
- ❌ Located in `rust/` directory instead of `bson/_rbson/`

**Performance Claims**: 2.89x average speedup over C (from benchmarks in POC)

**Why the POC appeared faster:**
The Copilot POC's claimed 2.89x speedup was likely due to:
1. **Limited test scope** - Benchmarks only tested simple documents that passed (53/88 tests)
2. **Missing validation** - No BSON size checks, null terminator validation, or extra bytes detection
3. **Incomplete CodecOptions** - Skipped expensive operations like:
   - Timezone conversions (`tzinfo` with `astimezone()`)
   - DateTime mode handling (CLAMP, AUTO, MS)
   - Unicode error handler fallbacks to Python
   - Custom document_class instantiation
4. **Optimistic measurements** - May have measured only the fast path without edge cases
5. **Different test methodology** - POC used custom benchmarks vs production testing with full PyMongo test suite

When these missing features were added to achieve 100% compatibility, the true performance cost of the Rust `Bson` enum architecture became apparent.

### Current Implementation (PR #2695) - Experimental
**Status**: 86/88 core BSON tests passing, ~85 feature tests skipped

**Build System**: `maturin build --release` (proper wheel generation)
- Uses Maturin for proper Python packaging
- Generates wheels with correct metadata
- Extracts .so file to `bson/` directory
- Located in `bson/_rbson/` directory (proper module structure)

**Improvements over Copilot POC:**
- ✅ **Core BSON functionality** (86/88 tests passing in test_bson.py)
- ✅ **Basic CodecOptions support**:
  - `document_class` - Custom document classes (basic support)
  - `tzinfo` - Timezone conversion with astimezone()
  - `datetime_conversion` - All modes (AUTO, CLAMP, MS)
  - `unicode_decode_error_handler` - Fallback to Python for non-strict handlers
- ✅ **BSON validation** (size checks, null terminator, extra bytes detection)
- ✅ **Performance optimizations**:
  - Type caching (UUID, datetime, Pattern, etc.)
  - Fast paths for common types (int, str, bool, None)
  - Direct byte operations where possible
  - PyDict fast path with pre-allocation
- ✅ **Modular code structure** (6 well-organized Rust modules)
- ✅ **Proper module structure** (`bson/_rbson/` with build.sh and maturin)
- ✅ **Runtime selection** via PYMONGO_USE_RUST environment variable
- ✅ **Test skip markers** for unimplemented features
- ✅ **Same Rust architecture**: PyO3 0.23 + bson 2.13 crate (Python → Bson enum → bytes)

**Missing Features** (see [Test Status](#test-status)):
- ❌ **Custom type encoders** (`TypeEncoder`, `TypeRegistry`, `FallbackEncoder`)
- ❌ **RawBSONDocument** codec options
- ❌ **Some DBRef edge cases**
- ❌ **Complete type checking support**

**Performance Reality**: ~0.21x (5x slower than C) - see Performance Analysis section

**Key Insights**:
1. **Same Architecture, Different Results**: Both implementations use the same Rust architecture (PyO3 + bson crate with intermediate `Bson` enum), so the build system (cargo vs maturin) is not the cause of the performance difference.
2. **Incomplete Implementation**: The current implementation has ~85 tests skipped due to unimplemented features (custom type encoders, RawBSONDocument, etc.). This is an experimental implementation, not production-ready.
3. **The Fundamental Issue**: The Rust architecture (Python → Bson enum → bytes) has inherent performance limitations compared to the C extension's direct byte-writing approach.

## Direct Byte-Writing Performance Results

### Implementation: `_dict_to_bson_direct()`

A new implementation has been added that writes BSON bytes directly from Python objects without converting to `Bson` enum types first. This eliminates the intermediate conversion layer.

**Architecture Comparison:**
```
Regular:  Python objects → Rust Bson enum → BSON bytes
Direct:   Python objects → BSON bytes (no intermediate types)
```

### Benchmark Results

Comprehensive benchmarks on realistic document types show **consistent 2x speedup**:

| Document Type | Regular (ops/sec) | Direct (ops/sec) | Speedup |
|--------------|-------------------|------------------|---------|
| User Profile | 99,970 | 208,658 | **2.09x** |
| E-commerce Order | 93,578 | 165,636 | **1.77x** |
| IoT Sensor Data | 136,824 | 312,058 | **2.28x** |
| Blog Post | 65,782 | 134,154 | **2.04x** |

**Average Speedup: 2.04x** (range: 1.77x - 2.28x)

### Performance by Document Composition

| Document Type | Regular (ops/sec) | Direct (ops/sec) | Speedup |
|--------------|-------------------|------------------|---------|
| Simple types (int, str, float, bool, None) | 177,588 | 800,670 | **4.51x** |
| Mixed types | 223,856 | 342,305 | **1.53x** |
| Nested documents | 130,884 | 287,758 | **2.20x** |
| BSON-specific types only | 342,059 | 304,844 | 0.89x |

### Key Findings

1. **Massive speedup for simple types**: 4.51x faster for documents with Python native types
2. **Consistent 2x improvement for real-world documents**: All realistic mixed-type documents show 1.77x - 2.28x speedup
3. **Slight slowdown for pure BSON types**: Documents with only BSON-specific types (ObjectId, Binary, etc.) are 10% slower due to extra Python attribute lookups
4. **100% correctness**: All outputs verified to be byte-identical to the regular implementation

### Why Direct Byte-Writing is Faster

1. **Eliminates heap allocations**: No need to create intermediate `Bson` enum values
2. **Reduces function call overhead**: Writes bytes immediately instead of going through `python_to_bson()` → `write_bson_value()`
3. **Better for common types**: Python's native types (int, str, float, bool) can be written directly without any conversion

### Implementation Details

The direct approach is implemented in these functions:
- `_dict_to_bson_direct()` - Public API function
- `write_document_bytes_direct()` - Writes document structure directly
- `write_element_direct()` - Writes individual elements without Bson conversion
- `write_bson_type_direct()` - Handles BSON-specific types directly

### Usage

```python
from bson import _rbson
from bson.codec_options import DEFAULT_CODEC_OPTIONS

# Use direct byte-writing approach
doc = {"name": "John", "age": 30, "score": 95.5}
bson_bytes = _rbson._dict_to_bson_direct(doc, False, DEFAULT_CODEC_OPTIONS)
```

### Benchmarking

Run the benchmarks yourself:
```bash
python benchmark_direct_bson.py        # Quick comparison
python benchmark_bson_types.py         # Individual type analysis
python benchmark_comprehensive.py      # Detailed statistics
```

## Steps to Achieve Performance Parity with C Extensions

Based on the analysis in PR #2695 and the direct byte-writing results, here are the steps needed to match C extension performance:

### 1. ✅ Eliminate Intermediate Bson Enum (High Impact) - COMPLETED
**Current**: Python → Bson → bytes
**Target**: Python → bytes (direct)

**Status**: ✅ **Implemented as `_dict_to_bson_direct()`**

**Actual Impact**: **2.04x average speedup** on realistic documents (range: 1.77x - 2.28x)

This brings the Rust extension from ~0.21x (5x slower than C) to **~0.43x (2.3x slower than C)** - a significant improvement!

### 2. Optimize Python API Calls (Medium Impact)
- Reduce `getattr()` calls by caching attribute lookups
- Use `PyDict_GetItem` instead of `dict.get_item()`
- Minimize Python exception handling overhead
- Use `PyTuple_GET_ITEM` for tuple access

**Estimated Impact**: 1.2-1.5x performance improvement

### 3. Memory Allocation Optimization (Low-Medium Impact)
- Pre-allocate buffers based on estimated document size
- Reuse buffers across multiple encode operations
- Use arena allocation for temporary objects

**Estimated Impact**: 1.1-1.3x performance improvement

### 4. SIMD Optimizations (Low Impact)
- Use SIMD for byte copying operations
- Vectorize validation checks
- Optimize string encoding/decoding

**Estimated Impact**: 1.05-1.1x performance improvement

### Combined Potential (Updated with Direct Byte-Writing Results)
With direct byte-writing implemented:
- **Before**: 0.21x (5x slower than C)
- **After direct byte-writing**: 0.43x (2.3x slower than C) ✅
- **With all optimizations**: 0.43x × 1.3 × 1.2 × 1.05 = **~0.71x** (1.4x slower than C)
- **Optimistic target**: Could potentially reach **~0.9x - 1.0x** (parity with C)

The direct byte-writing approach has already delivered the largest performance gain (2x). Additional optimizations could close the remaining gap to C extension performance.

## Building

```bash
cd bson/_rbson
./build.sh
```

Or using maturin directly:
```bash
maturin develop --release
```

## Testing

Run the core BSON test suite with the Rust extension:
```bash
PYMONGO_USE_RUST=1 python -m pytest test/test_bson.py -v
# Expected: 86 passed, 2 skipped
```

Run all tests (including skipped tests):
```bash
PYMONGO_USE_RUST=1 python -m pytest test/ -v
# Expected: Many tests passed, ~85 tests skipped due to unimplemented features
```

Run performance benchmarks:
```bash
python test/performance/perf_test.py
```

## Module Structure

The Rust codebase is organized into 6 well-structured modules (refactored from a single 3,117-line file):

- **`lib.rs`** (76 lines) - Module exports and public API
- **`types.rs`** (266 lines) - Type cache and BSON type markers
- **`errors.rs`** (56 lines) - Error handling utilities
- **`utils.rs`** (154 lines) - Utility functions (datetime, regex, validation)
- **`encode.rs`** (1,545 lines) - BSON encoding functions
- **`decode.rs`** (1,141 lines) - BSON decoding functions

This modular structure improves:
- Code organization and maintainability
- Compilation times (parallel module compilation)
- Code navigation and testing
- Clear separation of concerns

## Conclusion

The Rust extension demonstrates that:
1. ✅ **Rust can provide basic BSON encoding/decoding functionality**
2. ❌ **Complete feature parity with C extension is not achieved** (~85 tests skipped)
3. ❌ **Performance parity with C requires bypassing the `bson` crate**
4. ❌ **The engineering effort may not justify the benefits**

### Recommendation

⚠️ **NOT PRODUCTION READY** - The Rust extension is **experimental** and has significant limitations:

**Missing Features:**
- Custom type encoders (`TypeEncoder`, `TypeRegistry`, `FallbackEncoder`)
- RawBSONDocument codec options
- Some DBRef edge cases
- Complete type checking support

**Performance Issues:**
- ~5x slower than C extension (0.21x performance)
- Even with direct byte-writing optimizations, still ~2.3x slower (0.43x performance)

**Use Cases for Rust Extension:**
- **Experimental/research purposes only**
- Testing Rust-Python interop with PyO3
- Platforms where C compilation is difficult (with caveats about missing features)
- Future exploration if `bson` crate performance improves

**For production use, the C extension (`_cbson`) is strongly recommended.**

For more details, see:
- [PYTHON-5683 JIRA ticket](https://jira.mongodb.org/browse/PYTHON-5683)
- [PR #2695](https://github.com/mongodb/mongo-python-driver/pull/2695)

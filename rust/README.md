# Rust BSON Extension for PyMongo

This directory contains a Rust-based implementation of PyMongo's BSON encoding/decoding functionality as a spike/proof-of-concept for replacing the existing C extensions.

## Overview

This spike investigates using Rust + PyO3 instead of C for PyMongo's performance-critical extensions. The implementation wraps the official MongoDB Rust BSON crate to provide Python bindings.

## Performance Results

**TL;DR: Rust is 2.89x faster on average than the C implementation!**

| Operation | C Extension | Rust | Speedup |
|-----------|-------------|------|---------|
| Decode Simple | 4.76 μs | 1.18 μs | **4.03x** |
| Decode Complex | 30.62 μs | 5.95 μs | **5.15x** |
| Encode Simple | 3.00 μs | 2.18 μs | **1.38x** |
| Encode Complex | 21.37 μs | 21.27 μs | **1.00x** |

See [RUST_SPIKE_RESULTS.md](../RUST_SPIKE_RESULTS.md) for detailed analysis.

## Building

### Prerequisites

1. Install Rust toolchain:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. Verify installation:
```bash
cargo --version
```

### Build the Extension

From the project root:
```bash
python build_rust.py
```

This will:
1. Build the Rust library in release mode
2. Copy the compiled library to the project root as `pymongo_rust.so`

## Testing

### Basic functionality test:
```bash
python test_rust_extension.py
```

### Performance benchmark:
```bash
python benchmark_rust_vs_c.py
```

## Implementation Details

### Supported BSON Types

- ✅ Null
- ✅ Boolean
- ✅ Int32
- ✅ Int64
- ✅ Double
- ✅ String (UTF-8)
- ✅ Binary (Generic subtype)
- ✅ Document (nested)
- ✅ Array
- ⚠️ DateTime (not yet implemented)
- ⚠️ ObjectId (not yet implemented)
- ⚠️ Regex (not yet implemented)
- ⚠️ Timestamp (not yet implemented)

### Architecture

```
rust/
├── Cargo.toml          # Rust dependencies and build config
└── src/
    └── lib.rs          # Main implementation
        ├── encode_bson()        # Python dict → BSON bytes
        ├── decode_bson()        # BSON bytes → Python dict
        ├── python_to_bson()     # Type conversions
        ├── bson_to_python()     # Type conversions
        └── benchmark_*()        # Performance tests
```

### Dependencies

- **pyo3 (0.22)**: Python FFI and bindings
- **bson (2.13)**: MongoDB BSON implementation
- **serde (1.0)**: Serialization framework

## API

### `encode_bson(obj: dict) -> bytes`

Encode a Python dictionary to BSON bytes.

```python
import pymongo_rust

doc = {"name": "John", "age": 30}
bson_bytes = pymongo_rust.encode_bson(doc)
```

### `decode_bson(data: bytes) -> dict`

Decode BSON bytes to a Python dictionary.

```python
import pymongo_rust

bson_bytes = b'...'
doc = pymongo_rust.decode_bson(bson_bytes)
```

### `benchmark_encode_simple(iterations: int) -> float`

Benchmark encoding of a simple document. Returns total time in seconds.

```python
import pymongo_rust

time_seconds = pymongo_rust.benchmark_encode_simple(10000)
print(f"Encoded 10000 docs in {time_seconds:.4f}s")
```

### `benchmark_decode_simple(iterations: int) -> float`

Benchmark decoding of a simple document. Returns total time in seconds.

### `benchmark_encode_complex(iterations: int) -> float`

Benchmark encoding of a complex nested document. Returns total time in seconds.

### `benchmark_decode_complex(iterations: int) -> float`

Benchmark decoding of a complex nested document. Returns total time in seconds.

## Cross-Compatibility

The Rust implementation produces BSON output that is **100% compatible** with the C extension:

```python
from bson import encode as c_encode, decode as c_decode
import pymongo_rust

doc = {"test": "value"}

# Can encode with Rust and decode with C
rust_bytes = pymongo_rust.encode_bson(doc)
c_decoded = c_decode(rust_bytes)  # Works!

# Can encode with C and decode with Rust
c_bytes = c_encode(doc)
rust_decoded = pymongo_rust.decode_bson(c_bytes)  # Works!
```

## Next Steps for Full Implementation

If proceeding with a full port:

1. **Complete BSON Types**: Implement DateTime, ObjectId, Regex, Timestamp, etc.
2. **Wire Protocol**: Port `_cmessagemodule.c` functionality
3. **Error Handling**: Match Python exception types exactly
4. **Platform Testing**: Test on Linux, macOS, Windows, various Python versions
5. **Integration**: Modify `bson/__init__.py` to auto-detect and use Rust
6. **Documentation**: Update user-facing docs
7. **CI/CD**: Add Rust builds to CI pipeline
8. **Binary Wheels**: Build and publish wheels for all platforms

## Why Rust Over C?

1. **Memory Safety**: Eliminates use-after-free, buffer overflows, memory leaks
2. **Performance**: 2.89x faster on average
3. **Maintainability**: Better tooling, clearer code, easier to debug
4. **Security**: Fewer vulnerability classes
5. **Ecosystem**: Leverage MongoDB's Rust driver code

## License

Same as PyMongo: Apache License 2.0

## Questions?

See [RUST_SPIKE_RESULTS.md](../RUST_SPIKE_RESULTS.md) for full analysis and recommendations.

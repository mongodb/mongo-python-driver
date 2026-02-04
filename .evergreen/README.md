# Rust Extension Testing in Evergreen

This directory contains configuration and scripts for testing the Rust BSON extension in Evergreen CI.

## Files

### `run-rust-tests.sh`
Standalone script that:
1. Installs Rust toolchain if needed
2. Installs maturin (Rust-Python build tool)
3. Builds pymongo with Rust extension enabled
4. Verifies the Rust extension is active
5. Runs BSON tests with the Rust extension

**Usage:**
```bash
cd /path/to/mongo-python-driver
.evergreen/run-rust-tests.sh
```

**Environment Variables:**
- `PYMONGO_BUILD_RUST=1` - Enables building the Rust extension
- `PYMONGO_USE_RUST=1` - Forces runtime to use Rust extension

### `rust-extension.yml`
Evergreen configuration for Rust extension testing. Defines:
- **Functions**: `test rust extension` - Runs the Rust test script
- **Tasks**: Test tasks for different Python versions (3.10, 3.12, 3.14)
- **Build Variants**: Test configurations for RHEL8, macOS ARM64, and Windows

**To integrate into main config:**
Add to `.evergreen/config.yml`:
```yaml
include:
  - filename: .evergreen/generated_configs/functions.yml
  - filename: .evergreen/generated_configs/tasks.yml
  - filename: .evergreen/generated_configs/variants.yml
  - filename: .evergreen/rust-extension.yml  # Add this line
```

## Integration with Generated Config

The Rust extension tests can also be integrated into the generated Evergreen configuration.

### Modifications to `scripts/generate_config.py`

Three new functions have been added:

1. **`create_test_rust_tasks()`** - Creates test tasks for Python 3.10, 3.12, and 3.14
2. **`create_test_rust_variants()`** - Creates build variants for RHEL8, macOS ARM64, and Windows
3. **`create_test_rust_func()`** - Creates the function to run Rust tests

### Regenerating Config

To regenerate the Evergreen configuration with Rust tests:

```bash
cd .evergreen/scripts
python generate_config.py
```

**Note:** Requires the `shrub` Python package:
```bash
pip install shrub.py
```

## Test Coverage

The Rust extension currently passes **100% of BSON tests** (60 tests: 58 passing + 2 skipped):

### Passing Tests
- Basic BSON encoding/decoding
- All BSON types (ObjectId, DateTime, Decimal128, Regex, Binary, Code, Timestamp, etc.)
- Binary data handling (including UUID with all representation modes)
- Nested documents and arrays
- Exception handling (InvalidDocument, InvalidBSON, OverflowError)
- Error message formatting with document property
- Datetime clamping and timezone handling
- Custom classes and codec options
- Buffer protocol support (bytes, bytearray, memoryview, array, mmap)
- Unicode decode error handlers
- BSON validation (document structure, string null terminators, size fields)

### Skipped Tests
- **2 tests** - Require optional numpy dependency

## Platform Support

The Rust extension is tested on:
- **Linux (RHEL8)** - Primary platform, runs on PRs
- **macOS ARM64** - Secondary platform
- **Windows 64-bit** - Secondary platform

## Performance

The Rust extension is currently **slower than the C extension** for both encoding and decoding:
- Simple encoding: **0.84x** (16% slower than C)
- Complex encoding: **0.21x** (5x slower than C)
- Simple decoding: **0.42x** (2.4x slower than C)
- Complex decoding: **0.29x** (3.4x slower than C)

The main bottleneck is **Python FFI overhead** - creating Python objects from Rust incurs significant performance cost.

**Benefits of Rust implementation:**
- Memory safety guarantees (prevents buffer overflows and use-after-free bugs)
- Easier maintenance and debugging with strong type system
- Cross-platform compatibility via Rust's toolchain
- 100% test compatibility with C extension

**Recommendation:** C extension remains the default and recommended choice. The Rust extension demonstrates feasibility and correctness but is not yet performance-competitive for production use.

## Future Work

- Performance optimization (type caching, reduce FFI overhead)
- Performance benchmarking suite
- Additional BSON type optimizations

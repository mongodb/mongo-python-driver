# PyMongo

[![PyPI Version](https://img.shields.io/pypi/v/pymongo)](https://pypi.org/project/pymongo)
[![Python Versions](https://img.shields.io/pypi/pyversions/pymongo)](https://pypi.org/project/pymongo)
[![Monthly Downloads](https://static.pepy.tech/badge/pymongo/month)](https://pepy.tech/project/pymongo)
[![API Documentation Status](https://readthedocs.org/projects/pymongo/badge/?version=stable)](http://pymongo.readthedocs.io/en/stable/api?badge=stable)

## About

The PyMongo distribution contains tools for interacting with MongoDB
database from Python. The `bson` package is an implementation of the
[BSON format](http://bsonspec.org) for Python. The `pymongo` package is
a native Python driver for MongoDB, offering both synchronous and asynchronous APIs. The `gridfs` package is a
[gridfs](https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.md/)
implementation on top of `pymongo`.

PyMongo supports MongoDB 4.0, 4.2, 4.4, 5.0, 6.0, 7.0, and 8.0. PyMongo follows [semantic versioning](https://semver.org/spec/v2.0.0.html) for its releases.

## Documentation

Documentation is available at
[mongodb.com](https://www.mongodb.com/docs/languages/python/pymongo-driver/current/).

[API documentation](https://pymongo.readthedocs.io/en/stable/api/) and the [full changelog](https://pymongo.readthedocs.io/en/stable/changelog.html) for each release is available at [readthedocs.io](https://pymongo.readthedocs.io/en/stable/index.html).

## Support / Feedback

For issues with, questions about, or feedback for PyMongo, please look
into our [support channels](https://support.mongodb.com/welcome). Please
do not email any of the PyMongo developers directly with issues or
questions - you're more likely to get an answer on
[StackOverflow](https://stackoverflow.com/questions/tagged/mongodb)
(using a "mongodb" tag).

## Bugs / Feature Requests

Think you've found a bug? Want to see a new feature in PyMongo? Please
open a case in our issue management tool, JIRA:

-   [Create an account and login](https://jira.mongodb.org).
-   Navigate to [the PYTHON
    project](https://jira.mongodb.org/browse/PYTHON).
-   Click **Create Issue** - Please provide as much information as
    possible about the issue type and how to reproduce it.

Bug reports in JIRA for all driver projects (i.e. PYTHON, CSHARP, JAVA)
and the Core Server (i.e. SERVER) project are **public**.

### How To Ask For Help

Please include all of the following information when opening an issue:

-   Detailed steps to reproduce the problem, including full traceback,
    if possible.

-   The exact python version used, with patch level:

```bash
python -c "import sys; print(sys.version)"
```

-   The exact version of PyMongo used, with patch level:

```bash
python -c "import pymongo; print(pymongo.version); print(pymongo.has_c())"
```

-   The operating system and version (e.g. Windows 7, OSX 10.8, ...)

-   Web framework or asynchronous network library used, if any, with
    version (e.g. Django 1.7, mod_wsgi 4.3.0, gevent 1.0.1, Tornado
    4.0.2, ...)

### Security Vulnerabilities

If you've identified a security vulnerability in a driver or any other
MongoDB project, please report it according to the [instructions
here](https://www.mongodb.com/docs/manual/tutorial/create-a-vulnerability-report/).

## Installation

PyMongo can be installed with [pip](http://pypi.python.org/pypi/pip):

```bash
python -m pip install pymongo
```

You can also download the project source and do:

```bash
pip install .
```

Do **not** install the "bson" package from pypi. PyMongo comes with
its own bson package; running "pip install bson" installs a third-party
package that is incompatible with PyMongo.

## Dependencies

PyMongo supports CPython 3.9+ and PyPy3.9+.

Required dependencies:

Support for `mongodb+srv://` URIs requires [dnspython](https://pypi.python.org/pypi/dnspython)

Optional dependencies:

GSSAPI authentication requires
[pykerberos](https://pypi.python.org/pypi/pykerberos) on Unix or
[WinKerberos](https://pypi.python.org/pypi/winkerberos) on Windows. The
correct dependency can be installed automatically along with PyMongo:

```bash
python -m pip install "pymongo[gssapi]"
```

MONGODB-AWS authentication requires
[pymongo-auth-aws](https://pypi.org/project/pymongo-auth-aws/):

```bash
python -m pip install "pymongo[aws]"
```

OCSP (Online Certificate Status Protocol) requires
[PyOpenSSL](https://pypi.org/project/pyOpenSSL/),
[requests](https://pypi.org/project/requests/),
[service_identity](https://pypi.org/project/service_identity/) and may
require [certifi](https://pypi.python.org/pypi/certifi):

```bash
python -m pip install "pymongo[ocsp]"
```

Wire protocol compression with snappy requires
[python-snappy](https://pypi.org/project/python-snappy):

```bash
python -m pip install "pymongo[snappy]"
```

Wire protocol compression with zstandard requires
[backports.zstd](https://pypi.org/project/backports.zstd)
when used with Python versions before 3.14:

```bash
python -m pip install "pymongo[zstd]"
```

Client-Side Field Level Encryption requires
[pymongocrypt](https://pypi.org/project/pymongocrypt/) and
[pymongo-auth-aws](https://pypi.org/project/pymongo-auth-aws/):

```bash
python -m pip install "pymongo[encryption]"
```
You can install all dependencies automatically with the following
command:

```bash
python -m pip install "pymongo[gssapi,aws,ocsp,snappy,zstd,encryption]"
```

## Examples

Here's a basic example (for more see the *examples* section of the
docs):

```pycon
>>> import pymongo
>>> client = pymongo.MongoClient("localhost", 27017)
>>> db = client.test
>>> db.name
'test'
>>> db.my_collection
Collection(Database(MongoClient('localhost', 27017), 'test'), 'my_collection')
>>> db.my_collection.insert_one({"x": 10}).inserted_id
ObjectId('4aba15ebe23f6b53b0000000')
>>> db.my_collection.insert_one({"x": 8}).inserted_id
ObjectId('4aba160ee23f6b543e000000')
>>> db.my_collection.insert_one({"x": 11}).inserted_id
ObjectId('4aba160ee23f6b543e000002')
>>> db.my_collection.find_one()
{'x': 10, '_id': ObjectId('4aba15ebe23f6b53b0000000')}
>>> for item in db.my_collection.find():
...     print(item["x"])
...
10
8
11
>>> db.my_collection.create_index("x")
'x_1'
>>> for item in db.my_collection.find().sort("x", pymongo.ASCENDING):
...     print(item["x"])
...
8
10
11
>>> [item["x"] for item in db.my_collection.find().limit(2).skip(1)]
[8, 11]
```

## Learning Resources

- MongoDB Learn - [Python
courses](https://learn.mongodb.com/catalog?labels=%5B%22Language%22%5D&values=%5B%22Python%22%5D).
- [Python Articles on Developer
Center](https://www.mongodb.com/developer/languages/python/).

## Testing

The easiest way to run the tests is to run the following from the repository root.

```bash
pip install -e ".[test]"
pytest
```

For more advanced testing scenarios, see the [contributing guide](./CONTRIBUTING.md#running-tests-locally).

## Rust Extension (Experimental)

PyMongo includes an experimental Rust-based BSON extension (`_rbson`) as an alternative to the existing C extension (`_cbson`). This is a **proof-of-concept** demonstrating Rust's viability for Python extensions.

### Why Rust?

- **Memory Safety**: Prevents buffer overflows and use-after-free bugs
- **Maintainability**: Safer refactoring with strong type system
- **Modern Tooling**: Cargo, clippy, and excellent documentation
- **Compatibility**: 100% compatible with C extension's BSON format

### Installation

The Rust extension is **automatically built** if Rust is detected:

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install PyMongo (builds both C and Rust extensions)
pip install .
```

**Development build:**
```bash
cd bson/_rbson && ./build.sh
```

### Usage

By default, PyMongo uses the **C extension**. To use Rust:

```python
import os

os.environ["PYMONGO_USE_RUST"] = "1"
import bson

print(bson.get_bson_implementation())  # 'rust', 'c', or 'python'
```

Or via environment variable:
```bash
export PYMONGO_USE_RUST=1
python your_script.py
```

### Implementation Status

**✅ Complete (100% test pass rate - 60 tests, 58 passing + 2 skipped):**

**Encoding (Python → BSON bytes):**
- Direct implementation for: Double, String, Document, Array, Binary, ObjectId, Boolean, DateTime, Null, Regex, Int32, Timestamp, Int64, Decimal128
- Converts Python types to BSON using the Rust `bson` library
- Full codec_options support (document_class, tz_aware, uuid_representation, datetime_conversion, etc.)
- UUID encoding (all representation modes)
- Datetime clamping and conversion modes
- Key validation (checks for `$` prefix, `.` characters, null bytes)
- Buffer protocol support

**Decoding (BSON bytes → Python):**
- Fast-path direct byte reading for: Double (0x01), String (0x02), Document (0x03), Array (0x04), Boolean (0x08), Null (0x0A), Int32 (0x10), Int64 (0x12)
- Fallback to Rust `bson` library for: Binary (0x05), ObjectId (0x07), DateTime (0x09), Regex (0x0B), DBPointer (0x0C), Symbol (0x0E), Code (0x0F), Timestamp (0x11), Decimal128 (0x13), and other types
- BSON validation (document structure, string null terminators, size fields)
- Proper error messages matching C extension format
- Unicode decode error handlers
- Field name tracking for error reporting in nested structures

### Performance Results

**Current Performance (vs C extension):**
- Simple encoding: **0.84x** (16% slower than C)
- Complex encoding: **0.21x** (5x slower than C)
- Simple decoding: **0.42x** (2.4x slower than C)
- Complex decoding: **0.29x** (3.4x slower than C)

**Architecture:**
- ✅ Hybrid encoding strategy (fast path for PyDict, `items()` for other mappings)
- ✅ Direct buffer writing with `doc.to_writer()` for nested documents
- ✅ Efficient `_id` field ordering at top level
- ✅ Direct byte reading for common types (single-pass bytes → Python dict)
- ✅ Fallback to Rust `bson` library for less common types
- ✅ 100% test pass rate (60 tests: 58 passing + 2 skipped for optional numpy dependency)

**Performance Analysis:**

The Rust extension is currently slower than the C extension for both encoding and decoding. The main bottleneck is **Python FFI overhead** - creating Python objects from Rust incurs significant performance cost.

**Recommendation:** C extension remains the default and recommended choice. The Rust extension demonstrates feasibility and correctness but is not yet performance-competitive for production use.

### Path to Performance Parity

Analysis of the C extension reveals several optimization opportunities to achieve near-parity performance:

#### Priority 1: Type Caching (HIGH IMPACT)

**Problem:** The Rust implementation calls `py.import()` on every BSON type conversion:
```rust
// Called millions of times during decoding!
let int64_module = py.import("bson.int64")?;
let int64_class = int64_module.getattr("Int64")?;
```

**Solution:** Cache Python type objects in module state (like C extension does):
```rust
struct TypeCache {
    binary_class: OnceCell<PyObject>,
    int64_class: OnceCell<PyObject>,
    objectid_class: OnceCell<PyObject>,
    // ... etc
}
```

**Expected Impact:** 2-3x faster decoding, 1.5-2x faster encoding
**Effort:** 4-6 hours

#### Priority 2: Fast Paths for Common Types (MEDIUM IMPACT)

**Problem:** Every type conversion has overhead even with caching

**Solution:** Add fast paths for common types:
- Int32/Int64: Use `PyLong_FromLong()` directly when possible
- String: Use `PyUnicode_FromStringAndSize()` directly
- Boolean: Use `Py_True`/`Py_False` singletons
- Null: Use `py.None()` singleton

**Expected Impact:** 1.3-1.5x faster for simple documents
**Effort:** 2-3 hours

#### Priority 3: Reduce Allocations (MEDIUM IMPACT)

**Problem:** Creating intermediate `bson::Document` structures adds overhead

**Solution:** For simple documents, read bytes → Python directly without intermediate Rust structs

**Expected Impact:** 1.2-1.4x faster for simple documents
**Effort:** 6-8 hours (complex refactor)

#### Priority 4: Profile and Optimize Hotspots (LOW-MEDIUM IMPACT)

**Problem:** Unknown bottlenecks may exist

**Solution:** Use `cargo flamegraph` or `py-spy` to profile and identify remaining hotspots

**Expected Impact:** 1.1-1.3x faster overall
**Effort:** 3-4 hours

#### Projected Performance After Optimizations

| Optimization | Simple Encode | Complex Encode | Simple Decode | Complex Decode |
|--------------|---------------|----------------|---------------|----------------|
| **Current** | 0.84x | 0.21x | 0.42x | 0.29x |
| + Type Caching | 1.2x | 0.4x | 1.0x | 0.7x |
| + Fast Paths | 1.5x | 0.5x | 1.3x | 0.9x |
| + Reduce Allocs | 1.8x | 0.6x | 1.5x | 1.0x |
| + Profiling | **2.0x** | **0.7x** | **1.7x** | **1.1x** |

**Note:** Complex encoding will likely remain slower due to Python FFI overhead for nested structures.

**Total Estimated Effort:** 15-21 hours to reach near-parity performance

**Recommended Implementation Order:**
1. Type Caching (Priority 1) - Biggest impact
2. Fast Paths (Priority 2) - Quick wins
3. Profile (Priority 4) - Find remaining bottlenecks
4. Reduce Allocations (Priority 3) - Only if needed after profiling

**Run benchmarks:**
```bash
python test/performance/benchmark_bson.py
```

### Technical Details

For implementation details, see the source code at `bson/_rbson/src/lib.rs`. Key architectural components:

- **Buffer Management**: Auto-growing BSON byte buffer with little-endian encoding
- **Type System**: Support for all BSON types with `_type_marker` attribute detection
- **Codec Options**: Full support for document_class, tz_aware, uuid_representation, datetime_conversion, etc.
- **Key Validation**: Checks for `$` prefix, `.` characters, and null bytes
- **_id Ordering**: Ensures `_id` field is written first in top-level documents
- **Error Handling**: Matches C extension error messages for compatibility

---

#!/usr/bin/env python3
"""
Fuzz target for bson.decode().

Runs the C extension and pure-Python implementations on arbitrary bytes and
compares their results when both are present.
"""

from __future__ import annotations

import math
import struct
import sys
from typing import Optional

import atheris


def _load_pure_python_bson():
    """Re-import bson with the C extension blocked, then restore sys.modules."""
    saved = {
        k: sys.modules.pop(k) for k in list(sys.modules) if k == "bson" or k.startswith("bson.")
    }
    sys.modules["bson._cbson"] = None  # type: ignore[assignment]

    try:
        import bson as pure_bson
    except Exception as exc:
        raise RuntimeError("Could not load pure-Python bson for cross-checking") from exc
    finally:
        for k in list(sys.modules):
            if k == "bson" or k.startswith("bson."):
                del sys.modules[k]
        sys.modules.update(saved)

    return pure_bson


with atheris.instrument_imports():
    # Load both implementations while import instrumentation is active so the
    # pure-Python decoder gets coverage feedback; the C extension is a native
    # module and is skipped by atheris.
    import bson as _bson_c  # C extension when available

    _bson_pure = _load_pure_python_bson()

# Consistency checks apply only when the C extension is present.
_BOTH_IMPLS = _bson_c.has_c()


# InvalidBSON is excluded: each bson module has its own class, which
# _decode_with catches as bson_mod.InvalidBSON below.
_OTHER_ERRORS = (ValueError, struct.error, UnicodeDecodeError, OverflowError, KeyError)


def _normalize(obj: object) -> object:
    """Comparable form of a decoded document.

    The two bson handles are separate module instances, so bson type classes
    (ObjectId, Timestamp, ...) never compare equal across handles. Reduce
    leaves to "type-name:repr"; NaN never compares equal to itself.
    """
    if isinstance(obj, dict):
        return [(k, _normalize(v)) for k, v in obj.items()]
    if isinstance(obj, (list, tuple)):
        return [_normalize(v) for v in obj]
    if isinstance(obj, float) and math.isnan(obj):
        return "nan"
    return f"{type(obj).__name__}:{obj!r}"


def _decode_with(bson_mod, data: bytes) -> Optional[dict]:
    """Run bson_mod.decode(data), returning the document or None on expected errors."""
    try:
        return bson_mod.decode(data)
    except (bson_mod.InvalidBSON, *_OTHER_ERRORS):
        return None


@atheris.instrument_func
def TestOneInput(data: bytes) -> None:
    doc_c = _decode_with(_bson_c, data)

    # A successful decode must round-trip through encode().
    if doc_c is not None:
        try:
            _bson_c.encode(doc_c)
        except Exception as exc:
            raise AssertionError(
                f"bson.encode() failed after bson.decode() succeeded: {exc!r}\n"
                f"decoded document: {doc_c!r}"
            ) from exc

    doc_pure = _decode_with(_bson_pure, data)

    if doc_pure is not None:
        try:
            _bson_pure.encode(doc_pure)
        except Exception as exc:
            raise AssertionError(
                f"pure-Python bson.encode() failed after bson.decode() succeeded: {exc!r}\n"
                f"decoded document: {doc_pure!r}"
            ) from exc

    # Consistency check.
    if _BOTH_IMPLS:
        c_succeeded = doc_c is not None
        pure_succeeded = doc_pure is not None
        if c_succeeded != pure_succeeded:
            raise AssertionError(
                f"C extension and pure-Python bson.decode() disagree: "
                f"C={'success' if c_succeeded else 'failure'}, "
                f"pure={'success' if pure_succeeded else 'failure'} "
                f"for input: {data!r}"
            )
        if doc_c is not None and doc_pure is not None and _normalize(doc_c) != _normalize(doc_pure):
            raise AssertionError(
                f"C extension and pure-Python bson.decode() produced different "
                f"documents: C={doc_c!r}, pure={doc_pure!r}, input: {data!r}"
            )


def main() -> None:
    import os

    # FUZZ_NATIVE=1: libFuzzer is preloaded externally; skip atheris's own.
    internal_libfuzzer = os.environ.get("FUZZ_NATIVE") != "1"
    atheris.Setup(sys.argv, TestOneInput, internal_libfuzzer=internal_libfuzzer)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

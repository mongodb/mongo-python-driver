#!/usr/bin/env python3
"""
Fuzz target for bson.decode_all().

Runs both implementations on arbitrary bytes (including concatenated
documents) and compares their results when both are present.
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
    import bson as _bson_c

    _bson_pure = _load_pure_python_bson()

# Consistency checks apply only when the C extension is present.
_BOTH_IMPLS = _bson_c.has_c()

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


def _decode_all_with(bson_mod, data: bytes) -> Optional[list]:
    """Run bson_mod.decode_all(data), returning the documents or None on expected errors."""
    try:
        return bson_mod.decode_all(data)
    except (bson_mod.InvalidBSON, *_OTHER_ERRORS):
        return None


def _encode_all_with(bson_mod, docs) -> None:
    """Re-encode every document to exercise the encoder as well."""
    for doc in docs:
        try:
            bson_mod.encode(doc)
        except Exception as exc:
            raise AssertionError(
                f"bson.encode() failed after bson.decode_all() succeeded: {exc!r}\n"
                f"decoded documents: {docs!r}"
            ) from exc


@atheris.instrument_func
def TestOneInput(data: bytes) -> None:
    docs_c = _decode_all_with(_bson_c, data)
    if docs_c is not None:
        _encode_all_with(_bson_c, docs_c)

    docs_pure = _decode_all_with(_bson_pure, data)
    if docs_pure is not None:
        _encode_all_with(_bson_pure, docs_pure)

    # Consistency checks.
    if _BOTH_IMPLS:
        c_succeeded = docs_c is not None
        pure_succeeded = docs_pure is not None

        if c_succeeded != pure_succeeded:
            raise AssertionError(
                f"C extension and pure-Python bson.decode_all() disagree: "
                f"C={'success' if c_succeeded else 'failure'}, "
                f"pure={'success' if pure_succeeded else 'failure'} "
                f"for input: {data!r}"
            )

        if docs_c is not None and docs_pure is not None and len(docs_c) != len(docs_pure):
            raise AssertionError(
                f"C extension decoded {len(docs_c)} document(s) but pure Python "
                f"decoded {len(docs_pure)} document(s) for input: {data!r}"
            )

        if (
            docs_c is not None
            and docs_pure is not None
            and _normalize(docs_c) != _normalize(docs_pure)
        ):
            raise AssertionError(
                f"C extension and pure-Python bson.decode_all() produced different "
                f"documents for input: {data!r}"
            )


def main() -> None:
    import os

    internal_libfuzzer = os.environ.get("FUZZ_NATIVE") != "1"
    atheris.Setup(sys.argv, TestOneInput, internal_libfuzzer=internal_libfuzzer)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

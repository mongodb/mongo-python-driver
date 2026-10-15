#!/usr/bin/env python3
"""
Fuzz target for bson.decode_all().

Runs the multi-document decoder on arbitrary bytes (including concatenated
BSON documents) under both the C extension and pure Python implementations,
and compares their results when both are present.

Usage (with atheris / OSS-Fuzz):
    python fuzz_decode_all.py corpus/
"""

from __future__ import annotations

import struct
import sys
from typing import Optional

import atheris

# Load both bson implementations before atheris instruments imports.
import bson as _bson_c


def _load_pure_python_bson():
    saved = {
        k: sys.modules.pop(k) for k in list(sys.modules) if k == "bson" or k.startswith("bson.")
    }
    sys.modules["bson._cbson"] = None  # type: ignore[assignment]
    try:
        import bson as pure_bson
    except Exception as exc:
        print(f"Warning: could not load pure-Python bson: {exc}", file=sys.stderr)
        pure_bson = None
    finally:
        for k in list(sys.modules):
            if k == "bson" or k.startswith("bson."):
                del sys.modules[k]
        sys.modules.update(saved)
    return pure_bson


def _c_extension_available() -> bool:
    try:
        import bson._cbson as cbson

        return cbson is not None
    except ImportError:
        return False


_bson_pure = _load_pure_python_bson()
_BOTH_IMPLS = _bson_pure is not None and _c_extension_available()

with atheris.instrument_imports():
    pass  # bson is already imported above; nothing further to instrument

_OTHER_ERRORS = (ValueError, struct.error, UnicodeDecodeError, OverflowError, KeyError)


def _decode_all_with(bson_mod, data: bytes) -> Optional[list]:
    try:
        docs = bson_mod.decode_all(data)
        # Re-encode every document to exercise the encoder as well.
        for doc in docs:
            bson_mod.encode(doc)
        return docs
    except (bson_mod.InvalidBSON, *_OTHER_ERRORS):
        return None


@atheris.instrument_func
def TestOneInput(data: bytes) -> None:
    docs_c = _decode_all_with(_bson_c, data)

    if _bson_pure is not None:
        docs_pure = _decode_all_with(_bson_pure, data)

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

            if c_succeeded and pure_succeeded and len(docs_c) != len(docs_pure):
                raise AssertionError(
                    f"C extension decoded {len(docs_c)} document(s) but pure Python "
                    f"decoded {len(docs_pure)} document(s) for input: {data!r}"
                )


def main() -> None:
    import os

    internal_libfuzzer = os.environ.get("FUZZ_NATIVE") != "1"
    atheris.Setup(sys.argv, TestOneInput, internal_libfuzzer=internal_libfuzzer)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

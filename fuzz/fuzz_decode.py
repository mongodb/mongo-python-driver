#!/usr/bin/env python3
"""
Fuzz target for bson.decode().

Runs the C extension and pure-Python implementations on arbitrary bytes and
compares their results when both are present.

Usage (with atheris / OSS-Fuzz):
    python fuzz_decode.py corpus/
"""

from __future__ import annotations

import struct
import sys
from typing import Optional

import atheris

# Load both bson implementations before atheris instruments imports.
import bson as _bson_c  # C extension when available


def _load_pure_python_bson():
    """
    Return a fresh import of the bson package with the C extension blocked.

    ``None`` for ``bson._cbson`` in sys.modules forces the pure-Python
    fallback; the original modules are restored afterwards so ``_bson_c``
    remains valid.
    """
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
    """Return True if the bson C extension is actually loaded."""
    try:
        import bson._cbson as cbson

        return cbson is not None
    except ImportError:
        return False


_bson_pure = _load_pure_python_bson()

# Consistency checks only apply when the two handles genuinely differ.
_BOTH_IMPLS = _bson_pure is not None and _c_extension_available()

with atheris.instrument_imports():
    pass  # bson is already imported above; nothing further to instrument


# InvalidBSON is excluded here: each bson module instance has its own class,
# so _decode_with catches bson_mod.InvalidBSON rather than a shared tuple.
_OTHER_ERRORS = (ValueError, struct.error, UnicodeDecodeError, OverflowError, KeyError)


def _decode_with(bson_mod, data: bytes) -> Optional[dict]:
    """Run bson_mod.decode(data), returning the document or None on expected errors."""
    try:
        return bson_mod.decode(data)
    except (bson_mod.InvalidBSON, *_OTHER_ERRORS):
        return None


@atheris.instrument_func
def TestOneInput(data: bytes) -> None:
    # C extension (or the pure-Python fallback).
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

    # Pure Python.
    if _bson_pure is not None:
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


def main() -> None:
    import os

    # FUZZ_NATIVE=1: libFuzzer is preloaded externally; skip atheris's own.
    internal_libfuzzer = os.environ.get("FUZZ_NATIVE") != "1"
    atheris.Setup(sys.argv, TestOneInput, internal_libfuzzer=internal_libfuzzer)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

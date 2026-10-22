#!/usr/bin/env python3
"""
Fuzz target for bson.decode().

Runs the C extension and pure-Python implementations on arbitrary bytes and
compares their results when both are present.
"""

from __future__ import annotations

from typing import Optional

import atheris
from fuzz_shared import OTHER_ERRORS, load_pure_python_bson, main, normalize

with atheris.instrument_imports():
    # Load both implementations while import instrumentation is active so the
    # pure-Python decoder gets coverage feedback; the C extension is a native
    # module and is skipped by atheris.
    import bson as _bson_c  # C extension when available

    _bson_pure = load_pure_python_bson()

# Consistency checks apply only when the C extension is present.
_BOTH_IMPLS = _bson_c.has_c()


def _decode_with(bson_mod, data: bytes) -> Optional[dict]:
    """Run bson_mod.decode(data), returning the document or None on expected errors."""
    try:
        return bson_mod.decode(data)
    except (bson_mod.InvalidBSON, *OTHER_ERRORS):
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
        if doc_c is not None and doc_pure is not None and normalize(doc_c) != normalize(doc_pure):
            raise AssertionError(
                f"C extension and pure-Python bson.decode() produced different "
                f"documents: C={doc_c!r}, pure={doc_pure!r}, input: {data!r}"
            )


if __name__ == "__main__":
    main(TestOneInput)

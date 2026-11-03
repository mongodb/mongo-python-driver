#!/usr/bin/env python3
"""
Fuzz target for bson.decode_all().

Runs both implementations on arbitrary bytes (including concatenated
documents) and compares their results when both are present.
"""

from __future__ import annotations

from typing import Optional

import atheris
from fuzz_shared import OTHER_ERRORS, load_pure_python_bson, main, normalize

with atheris.instrument_imports():
    # Load both implementations while import instrumentation is active so the
    # pure-Python decoder gets coverage feedback; the C extension is a native
    # module and is skipped by atheris.
    import bson as _bson_c

    _bson_pure = load_pure_python_bson()

# Consistency checks apply only when the C extension is present.
_BOTH_IMPLS = _bson_c.has_c()


def _decode_all_with(bson_mod, data: bytes) -> Optional[list]:
    """Run bson_mod.decode_all(data), returning the documents or None on expected errors."""
    try:
        return bson_mod.decode_all(data)
    except (bson_mod.InvalidBSON, *OTHER_ERRORS):
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
            and normalize(docs_c) != normalize(docs_pure)
        ):
            raise AssertionError(
                f"C extension and pure-Python bson.decode_all() produced different "
                f"documents for input: {data!r}"
            )


if __name__ == "__main__":
    main(TestOneInput)

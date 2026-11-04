#!/usr/bin/env python3
"""
Fuzz target for bson.is_valid().

Checks that is_valid() returning True means decode() succeeds, and that
the C extension and pure Python agree on the result.
"""

from __future__ import annotations

import atheris
from fuzz_shared import load_pure_python_bson, main

with atheris.instrument_imports():
    # Load both implementations while import instrumentation is active so the
    # pure-Python parser gets coverage feedback; the C extension is a native
    # module and is skipped by atheris.
    import bson as _bson_c

    _bson_pure = load_pure_python_bson()

# Consistency checks apply only when the C extension is present.
_BOTH_IMPLS = _bson_c.has_c()


@atheris.instrument_func
def TestOneInput(data: bytes) -> None:
    valid_c = _bson_c.is_valid(data)

    if valid_c:
        try:
            _bson_c.decode(data)
        except Exception as exc:
            # is_valid() performs a full decode and swallows every exception,
            # so a True result guarantees decode() raises none.
            raise AssertionError(
                f"bson.is_valid() returned True but bson.decode() raised "
                f"{type(exc).__name__}: {exc!r}\ninput: {data!r}"
            ) from exc

    valid_pure = _bson_pure.is_valid(data)

    if valid_pure:
        try:
            _bson_pure.decode(data)
        except Exception as exc:
            raise AssertionError(
                f"pure-Python bson.is_valid() returned True but bson.decode() "
                f"raised {type(exc).__name__}: {exc!r}\ninput: {data!r}"
            ) from exc

    if _BOTH_IMPLS and valid_c != valid_pure:
        raise AssertionError(
            f"C extension and pure-Python bson.is_valid() disagree: "
            f"C={valid_c}, pure={valid_pure}\ninput: {data!r}"
        )


if __name__ == "__main__":
    main(TestOneInput)

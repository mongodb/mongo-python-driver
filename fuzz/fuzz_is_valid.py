#!/usr/bin/env python3
"""
Fuzz target for bson.is_valid().

Checks that is_valid() returning True means decode() raises no InvalidBSON,
and that the C extension and pure Python agree on the result.
"""

from __future__ import annotations

import struct
import sys

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
    # pure-Python parser gets coverage feedback; the C extension is a native
    # module and is skipped by atheris.
    import bson as _bson_c

    _bson_pure = _load_pure_python_bson()

# Consistency checks apply only when the C extension is present.
_BOTH_IMPLS = _bson_c.has_c()

_EXPECTED_DECODE_ERRORS = (
    _bson_c.InvalidBSON,
    ValueError,
    struct.error,
    UnicodeDecodeError,
    OverflowError,
    KeyError,
)


@atheris.instrument_func
def TestOneInput(data: bytes) -> None:
    valid_c = _bson_c.is_valid(data)

    if valid_c:
        try:
            _bson_c.decode(data)
        except _bson_c.InvalidBSON as exc:
            raise AssertionError(
                f"bson.is_valid() returned True but bson.decode() raised "
                f"InvalidBSON: {exc!r}\ninput: {data!r}"
            ) from exc
        except _EXPECTED_DECODE_ERRORS:
            # is_valid() only guarantees structural validity.
            pass

    valid_pure = _bson_pure.is_valid(data)

    if valid_pure:
        try:
            _bson_pure.decode(data)
        except _bson_pure.InvalidBSON as exc:
            raise AssertionError(
                f"pure-Python bson.is_valid() returned True but bson.decode() "
                f"raised InvalidBSON: {exc!r}\ninput: {data!r}"
            ) from exc
        except _EXPECTED_DECODE_ERRORS:
            pass

    if _BOTH_IMPLS and valid_c != valid_pure:
        raise AssertionError(
            f"C extension and pure-Python bson.is_valid() disagree: "
            f"C={valid_c}, pure={valid_pure}\ninput: {data!r}"
        )


def main() -> None:
    import os

    internal_libfuzzer = os.environ.get("FUZZ_NATIVE") != "1"
    atheris.Setup(sys.argv, TestOneInput, internal_libfuzzer=internal_libfuzzer)
    atheris.Fuzz()


if __name__ == "__main__":
    main()

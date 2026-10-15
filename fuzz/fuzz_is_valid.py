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
    import bson

_EXPECTED_DECODE_ERRORS = (
    bson.InvalidBSON,
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
        except bson.InvalidBSON as exc:
            raise AssertionError(
                f"bson.is_valid() returned True but bson.decode() raised "
                f"InvalidBSON: {exc!r}\ninput: {data!r}"
            ) from exc
        except _EXPECTED_DECODE_ERRORS:
            # is_valid() only guarantees structural validity.
            pass

    if _bson_pure is not None:
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

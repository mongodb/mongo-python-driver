#!/usr/bin/env python3
"""Code shared by the BSON fuzz targets."""

from __future__ import annotations

import math
import os
import struct
import sys

import atheris


def load_pure_python_bson():
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


# InvalidBSON is excluded: each bson module has its own class, caught as
# bson_mod.InvalidBSON. RecursionError is defensive: decode() rewraps
# all exceptions into InvalidBSON.
OTHER_ERRORS = (
    ValueError,
    struct.error,
    UnicodeDecodeError,
    OverflowError,
    KeyError,
    RecursionError,
)


def normalize(obj: object) -> object:
    """Comparable form of a decoded document.

    The two bson handles are separate module instances, so bson type classes
    (ObjectId, Timestamp, ...) never compare equal across handles. Reduce
    leaves to "type-name:repr"; NaN never compares equal to itself.
    """
    if isinstance(obj, dict):
        return [(k, normalize(v)) for k, v in obj.items()]
    if isinstance(obj, (list, tuple)):
        return [normalize(v) for v in obj]
    if isinstance(obj, float) and math.isnan(obj):
        return "nan"
    if type(obj).__name__ == "Regex":
        # int() canonicalizes the C extension's plain int flags and pure
        # Python's re.RegexFlag.
        return f"Regex:{obj.pattern!r}:{int(obj.flags)}"
    return f"{type(obj).__name__}:{obj!r}"


def main(test_one_input) -> None:
    """Set up atheris for a fuzz target and start fuzzing."""
    # FUZZ_NATIVE=1: libFuzzer is preloaded externally; skip atheris's own.
    internal_libfuzzer = os.environ.get("FUZZ_NATIVE") != "1"
    atheris.Setup(sys.argv, test_one_input, internal_libfuzzer=internal_libfuzzer)
    atheris.Fuzz()

"""Selects the BSON codec implementation for the performance benchmarks.

Set ``BSON_IMPL`` to ``c`` (the default) or ``rust``. For ``rust``, the
private entry points that ``bson/__init__.py`` binds from ``_cbson``, and
the driver modules that captured them with ``from bson import ...``, are
rebound to the Rust module. The public ``bson.encode``/``decode`` look
these names up at call time, so rebinding is transparent.
"""

from __future__ import annotations

import os

_IMPLS = ("c", "rust")

# The private entry points exposed by both _cbson and _rbson. They have
# the C-style signature: bson/__init__.py's Python wrappers (which add a
# `view` argument) are dead code when the C extension is importable.
_RBSON_ENTRY_POINTS = (
    "_dict_to_bson",
    "_bson_to_dict",
    "_decode_all",
    "_element_to_dict",
    "_raw_to_dict",
    "_array_of_documents_to_buffer",
)

# Modules that captured a private entry point with `from bson import ...`
# and hold a direct reference; (module, attribute) pairs to rebind.
_CAPTURING_MODULES = (
    ("bson.raw_bson", "_raw_to_dict"),
    ("pymongo.message", "_dict_to_bson"),
    ("pymongo.synchronous.change_stream", "_bson_to_dict"),
    ("pymongo.asynchronous.change_stream", "_bson_to_dict"),
    ("pymongo.synchronous.encryption", "_dict_to_bson"),
    ("pymongo.asynchronous.encryption", "_dict_to_bson"),
)


def select_impl() -> str:
    """Applies the BSON_IMPL selection and returns the active impl name."""
    impl = os.environ.get("BSON_IMPL", "c").strip().lower()
    if impl not in _IMPLS:
        raise ValueError(f"BSON_IMPL must be one of {_IMPLS}, got {impl!r}")

    if impl == "rust":
        import _rbson

        import bson

        for name in _RBSON_ENTRY_POINTS:
            setattr(bson, name, getattr(_rbson, name))
        for module_name, attr in _CAPTURING_MODULES:
            try:
                module = __import__(module_name, fromlist=[attr])
            except ImportError:
                continue  # optional dependency, not loaded
            if getattr(module, attr, None) is not None:
                setattr(module, attr, getattr(_rbson, attr))
    return impl


ACTIVE_IMPL = select_impl()

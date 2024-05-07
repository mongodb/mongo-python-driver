# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
from __future__ import annotations

import importlib.util
import sys
from types import ModuleType


def lazy_import(name: str) -> ModuleType:
    """Lazily import a module by name

    From https://docs.python.org/3/library/importlib.html#implementing-lazy-imports
    """
    # Workaround for PYTHON-4424.
    if "__compiled__" in globals():
        return importlib.import_module(name)
    try:
        spec = importlib.util.find_spec(name)
    except ValueError:
        # Note: this cannot be ModuleNotFoundError, see PYTHON-4424.
        raise ImportError(name=name) from None
    if spec is None:
        # Note: this cannot be ModuleNotFoundError, see PYTHON-4424.
        raise ImportError(name=name)
    assert spec is not None
    loader = importlib.util.LazyLoader(spec.loader)  # type:ignore[arg-type]
    spec.loader = loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    loader.exec_module(module)
    return module

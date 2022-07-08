# Copyright 2022-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test the default exports of the top level packages."""
import inspect
import unittest

import bson
import gridfs
import pymongo

BSON_IGNORE = []
GRIDFS_IGNORE = [
    "ASCENDING",
    "DESCENDING",
    "ClientSession",
    "Collection",
    "ObjectId",
    "validate_string",
    "Database",
    "ConfigurationError",
    "WriteConcern",
]
PYMONGO_IGNORE = []
GLOBAL_INGORE = ["TYPE_CHECKING"]


class TestDefaultExports(unittest.TestCase):
    def check_module(self, mod, ignores):
        names = dir(mod)
        names.remove("__all__")
        for name in mod.__all__:
            if name not in names and name not in ignores:
                self.fail(f"{name} was included in {mod}.__all__ but is not a valid symbol")

        for name in names:
            if name not in mod.__all__ and name not in ignores:
                if name in GLOBAL_INGORE:
                    continue
                value = getattr(mod, name)
                if inspect.ismodule(value):
                    continue
                if getattr(value, "__module__", None) == "typing":
                    continue
                if not name.startswith("_"):
                    self.fail(f"{name} was not included in {mod}.__all__")

    def test_pymongo(self):
        self.check_module(pymongo, PYMONGO_IGNORE)

    def test_gridfs(self):
        self.check_module(gridfs, GRIDFS_IGNORE)

    def test_bson(self):
        self.check_module(bson, BSON_IGNORE)


if __name__ == "__main__":
    unittest.main()

# Copyright 2026-present MongoDB, Inc.
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

"""Test the pymongo.common option validators (no server required)."""

from __future__ import annotations

import sys
import unittest
from typing import Any

sys.path[0:0] = [""]

from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo.common import validate_document_class
from pymongo.mongo_client import MongoClient

_EXPECTED_MESSAGE = (
    "document_class must be dict, bson.son.SON, "
    "bson.raw_bson.RawBSONDocument, or a "
    "subclass of collections.MutableMapping"
)


class TestValidateDocumentClass(unittest.TestCase):
    """validate_document_class must report bad input as a document_class error."""

    def test_rejects_non_class_values(self):
        # An instance (or any other non-class) is not a valid document_class.
        # This must surface the document_class message rather than the bare
        # "issubclass() arg 1 must be a class" raised by the stdlib.
        for value in (42, "dict", None, {}, object(), [dict], (dict,), list[int]):
            with self.subTest(value=value):
                with self.assertRaises(TypeError) as ctx:
                    validate_document_class("document_class", value)
                self.assertEqual(_EXPECTED_MESSAGE, str(ctx.exception))
                self.assertNotIn("issubclass()", str(ctx.exception))

    def test_accepts_valid_document_classes(self):
        for value in (
            dict,
            SON,
            RawBSONDocument,
            SON[str, Any],
            dict[str, Any],
            type("MyDict", (dict,), {}),
            type("MyRaw", (RawBSONDocument,), {}),
        ):
            with self.subTest(value=value):
                self.assertIs(value, validate_document_class("document_class", value))

    def test_rejects_non_mapping_classes(self):
        for value in (int, str, list, object):
            with self.subTest(value=value):
                with self.assertRaises(TypeError) as ctx:
                    validate_document_class("document_class", value)
                self.assertEqual(_EXPECTED_MESSAGE, str(ctx.exception))

    def test_mongo_client_reports_invalid_document_class(self):
        # MongoClient validates document_class before any server is contacted.
        with self.assertRaises(TypeError) as ctx:
            MongoClient(document_class=42)  # type: ignore[arg-type]
        self.assertEqual(_EXPECTED_MESSAGE, str(ctx.exception))


if __name__ == "__main__":
    unittest.main()

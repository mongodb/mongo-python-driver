# -*- coding: utf-8 -*-
#
# Copyright 2018-present MongoDB, Inc.
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

"""Test the bson writer."""

import sys

sys.path[0:0] = [""]

from bson import BSON
from bson.bson_writer import BSONWriter, BSONDocumentWriter

from test import unittest


class TestBSONWriter(unittest.TestCase):
    def test_basic(self):
        test_doc = {"a": 1, "b": "some text", }
        expected_bytes = BSON.encode(test_doc)

        writer = BSONWriter()
        writer.start_document()
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_subdocument(self):
        test_doc = {"a": {"b": "some text", }, "b": {"c": 3}}
        expected_bytes = BSON.encode(test_doc)

        writer = BSONWriter()
        writer.start_document()
        writer.start_document("a")
        writer.write_name_value("b", "some text")
        writer.end_document()
        writer.write_name("b")
        writer.start_document()
        writer.write_name("c")
        writer.write_value(3)
        writer.end_document()
        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_implicit_subdocument(self):
        test_doc = {"a": {"b": "some text", }, }
        expected_bytes = BSON.encode(test_doc)

        # Test writing name and value together.
        writer = BSONWriter()
        writer.start_document()
        writer.write_name_value("a", {"b": "some text", })
        writer.end_document()
        self.assertEqual(expected_bytes, writer.as_bytes())

        # Test writing name and value separately.
        writer = BSONWriter()
        writer.start_document()
        writer.write_name("a")
        writer.write_value({"b": "some text", })
        writer.end_document()
        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_array(self):
        test_doc = {"a": [0.0, "one"], "b": [1, "two", 3.0], }
        expected_bytes = BSON.encode(test_doc)

        writer = BSONWriter()
        writer.start_document()

        writer.start_array("a")
        writer.write_value(0.0)
        writer.write_value("one")
        writer.end_array()

        writer.write_name("b")
        writer.start_array()
        writer.write_value(1)
        writer.write_value("two")
        writer.write_value(3.0)
        writer.end_array()

        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_implicit_array(self):
        test_doc = {"a": [1, "two", 3.0], }
        expected_bytes = BSON.encode(test_doc)

        # Test writing name and value together.
        writer = BSONWriter()
        writer.start_document()
        writer.write_name_value("a", [1, "two", 3.0])
        writer.end_document()
        self.assertEqual(expected_bytes, writer.as_bytes())

        # Test writing name and value separately.
        writer = BSONWriter()
        writer.start_document()
        writer.write_name("a")
        writer.write_value([1, "two", 3.0])
        writer.end_document()
        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_nested_containers(self):
        # Scenario 1: documents nested in documents
        test_doc = {"a": {"b": {"c": 1}}}
        expected_bytes = BSON.encode(test_doc)

        writer = BSONWriter()
        writer.start_document()
        writer.start_document("a")
        writer.start_document("b")
        writer.write_name("c")
        writer.write_value(1)
        writer.end_document()
        writer.end_document()
        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 2: arrays nested in documents
        test_doc = {"a": {"b": [0.0, "1.0", 2]}}
        expected_bytes = BSON.encode(test_doc)

        writer.initialize()
        writer.start_document()
        writer.start_document("a")
        writer.start_array("b")
        writer.write_value(0.0)
        writer.write_value("1.0")
        writer.write_value(2)
        writer.end_array()
        writer.end_document()
        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 3: arrays nested in arrays
        test_doc = {"a": [0, ["a", "b", ], 1, 2]}
        expected_bytes = BSON.encode(test_doc)

        writer.initialize()
        writer.start_document()
        writer.start_array("a")
        writer.write_value(0)
        writer.start_array()
        writer.write_value("a")
        writer.write_value("b")
        writer.end_array()
        writer.write_value(1)
        writer.write_value(2)
        writer.end_array()
        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 4: documents nested in arrays
        test_doc = {"a": [0, {"b": "hello world", }, 1, 2]}
        expected_bytes = BSON.encode(test_doc)

        writer.initialize()
        writer.start_document()
        writer.start_array("a")
        writer.write_value(0)
        writer.start_document()
        writer.write_name_value("b", "hello world")
        writer.end_document()
        writer.write_value(1)
        writer.write_value(2)
        writer.end_array()
        writer.end_document()

        self.assertEqual(expected_bytes, writer.as_bytes())


class TestBSONDocumentWriter(unittest.TestCase):
    def test_implicit_id_basic(self):
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": writer.get_document_id(), "a": 1, "b": "some text"}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_explicit_id_basic(self):
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.write_name_value("_id", 12345)
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": writer.get_document_id(), "a": 1, "b": "some text"}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())
        self.assertEqual(12345, writer.get_document_id())

    def test_explicit_id_document_id(self):
        # Implicit document as ID
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.write_name_value("_id", {'a': 1})
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": {'a': 1}, "a": 1, "b": "some text"}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Explicit document as ID
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.start_document("_id")
        writer.write_name_value("a", 1)
        writer.end_document()
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": {'a': 1}, "a": 1, "b": "some text"}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_explicit_id_array_id(self):
        # Implicit document as ID
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.write_name_value("_id", [1, 2, 3])
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": [1, 2, 3], "a": 1, "b": "some text"}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Explicit document as ID
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.start_array("_id")
        for num in range(1, 4):
            writer.write_value(num)
        writer.end_array()
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": [1, 2, 3], "a": 1, "b": "some text"}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_implicit_id_nested_containers(self):
        # Scenario 1: documents nested in documents
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.start_document("a")
        writer.start_document("b")
        writer.write_name("c")
        writer.write_value(1)
        writer.end_document()
        writer.end_document()
        writer.end_document()

        test_doc = {"_id": writer.get_document_id(), "a": {"b": {"c": 1}}}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 2: arrays nested in documents
        writer.initialize()
        writer.start_document()
        writer.start_document("a")
        writer.start_array("b")
        writer.write_value(0.0)
        writer.write_value("1.0")
        writer.write_value(2)
        writer.end_array()
        writer.end_document()
        writer.end_document()

        test_doc = {
            "_id": writer.get_document_id(), "a": {"b": [0.0, "1.0", 2]}}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 3: arrays nested in arrays
        writer.initialize()
        writer.start_document()
        writer.start_array("a")
        writer.write_value(0)
        writer.start_array()
        writer.write_value("a")
        writer.write_value("b")
        writer.end_array()
        writer.write_value(1)
        writer.write_value(2)
        writer.end_array()
        writer.end_document()

        test_doc = {
            "_id": writer.get_document_id(), "a": [0, ["a", "b", ], 1, 2]}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 4: documents nested in arrays
        writer.initialize()
        writer.start_document()
        writer.start_array("a")
        writer.write_value(0)
        writer.start_document()
        writer.write_name_value("b", "hello world")
        writer.end_document()
        writer.write_value(1)
        writer.write_value(2)
        writer.end_array()
        writer.end_document()

        test_doc = {
            "_id": writer.get_document_id(),
            "a": [0, {"b": "hello world", }, 1, 2]}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

    def test_explicit_id_nested_containers(self):
        # Scenario 1: documents nested in documents
        writer = BSONDocumentWriter()
        writer.start_document()
        writer.write_name_value("_id", 12345)
        writer.start_document("a")
        writer.start_document("b")
        writer.write_name("c")
        writer.write_value(1)
        writer.end_document()
        writer.end_document()
        writer.end_document()

        test_doc = {"_id": 12345, "a": {"b": {"c": 1}}}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 2: arrays nested in documents
        writer.initialize()
        writer.start_document()
        writer.write_name_value("_id", 12345)
        writer.start_document("a")
        writer.start_array("b")
        writer.write_value(0.0)
        writer.write_value("1.0")
        writer.write_value(2)
        writer.end_array()
        writer.end_document()
        writer.end_document()

        test_doc = {"_id": 12345, "a": {"b": [0.0, "1.0", 2]}}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 3: arrays nested in arrays
        writer.initialize()
        writer.start_document()
        writer.write_name_value("_id", 12345)
        writer.start_array("a")
        writer.write_value(0)
        writer.start_array()
        writer.write_value("a")
        writer.write_value("b")
        writer.end_array()
        writer.write_value(1)
        writer.write_value(2)
        writer.end_array()
        writer.end_document()

        test_doc = {"_id": 12345, "a": [0, ["a", "b", ], 1, 2]}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())

        # Scenario 4: documents nested in arrays
        writer.initialize()
        writer.start_document()
        writer.write_name_value("_id", 12345)
        writer.start_array("a")
        writer.write_value(0)
        writer.start_document()
        writer.write_name_value("b", "hello world")
        writer.end_document()
        writer.write_value(1)
        writer.write_value(2)
        writer.end_array()
        writer.end_document()

        test_doc = {"_id": 12345, "a": [0, {"b": "hello world", }, 1, 2]}
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())


if __name__ == "__main__":
    unittest.main()

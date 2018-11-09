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
from bson.bson_writer import BSONWriter, ImplicitIDBSONWriter

from test import unittest

import ipdb as pdb


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
        # Need to test 4 scenarios:
        #   - docs in docs
        #   - arrays in docs
        #   - arrays in arrays
        #   - docs in arrays

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

        writer = BSONWriter()
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

        writer = BSONWriter()
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

        writer = BSONWriter()
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


class TestImplicitIDBSONWriter(unittest.TestCase):
    def test_implicit_id(self):
        writer = ImplicitIDBSONWriter()
        writer.start_document()
        writer.write_name_value("a", 1)
        writer.write_name("b")
        writer.write_value("some text")
        writer.end_document()

        test_doc = {"_id": writer.get_id(), "a": 1, "b": "some text", }
        expected_bytes = BSON.encode(test_doc)

        self.assertEqual(expected_bytes, writer.as_bytes())


if __name__ == "__main__":
    unittest.main()

# Copyright 2015 MongoDB, Inc.
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

"""Tests for the write_concern module."""

import sys
import unittest

sys.path[0:0] = [""]

from pymongo.errors import ConfigurationError
from pymongo.write_concern import WriteConcern

class TestWriteConcern(unittest.TestCase):

    def test_validation(self):

        # No error.
        WriteConcern(w=100)
        WriteConcern(w=-1)
        WriteConcern(w="majority")
        WriteConcern(w="foo")
        WriteConcern(w=u"foo")
        WriteConcern(wtimeout=1000)
        WriteConcern(j=True)
        WriteConcern(fsync=True)

        self.assertRaises(
            TypeError, WriteConcern, w=1.0)
        self.assertRaises(
            TypeError, WriteConcern, j=1.0)
        self.assertRaises(
            TypeError, WriteConcern, fsync=1.0)
        self.assertRaises(
            TypeError, WriteConcern, wtimeout=1.0)
        self.assertRaises(
            ConfigurationError, WriteConcern, j=True, fsync=True)
        self.assertRaises(
            ConfigurationError, WriteConcern, w=0, j=True)
        self.assertRaises(
            ConfigurationError, WriteConcern, w=0, fsync=True)
        self.assertRaises(
            ConfigurationError, WriteConcern, w=0, wtimeout=1000)

    def test_document(self):
        self.assertEqual({}, WriteConcern().document)
        self.assertEqual({"w": 0}, WriteConcern(w=0).document)
        self.assertEqual(
            {"w": 5, "wtimeout": 1000, "j": True, "fsync": False},
            WriteConcern(w=5, wtimeout=1000, j=True, fsync=False).document)

    def test_acknowledged(self):
        self.assertTrue(WriteConcern().acknowledged)
        self.assertTrue(WriteConcern(j=True, fsync=False).acknowledged)
        self.assertFalse(WriteConcern(w=0).acknowledged)
        self.assertFalse(WriteConcern(w=-1).acknowledged)

    def test_immutable(self):
        wcn = WriteConcern()
        doc = wcn.document
        doc["w"] = 5
        self.assertEqual({}, wcn.document)

    def test_equality(self):
        self.assertEqual(WriteConcern(), WriteConcern())
        self.assertEqual(WriteConcern(w=5), WriteConcern(w=5))
        self.assertNotEqual(WriteConcern(w=5), WriteConcern(w=1))

if __name__ == "__main__":
    unittest.main()

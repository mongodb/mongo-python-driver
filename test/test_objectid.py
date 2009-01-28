# Copyright 2009 10gen, Inc.
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

"""Tests for the objectid module."""

import unittest
import sys
sys.path[0:0] = [""]

from pymongo.objectid import ObjectId
from pymongo.errors import InvalidId

class TestObjectId(unittest.TestCase):
    def setUp(self):
        pass

    def test_creation(self):
        self.assertRaises(TypeError, ObjectId, 4)
        self.assertRaises(TypeError, ObjectId, u"hello")
        self.assertRaises(TypeError, ObjectId, 175.0)
        self.assertRaises(TypeError, ObjectId, {"test": 4})
        self.assertRaises(TypeError, ObjectId, ["something"])
        self.assertRaises(InvalidId, ObjectId, "")
        self.assertRaises(InvalidId, ObjectId, "12345678901")
        self.assertRaises(InvalidId, ObjectId, "1234567890123")
        self.assertTrue(ObjectId())
        self.assertTrue(ObjectId("123456789012"))
        a = ObjectId()
        self.assertTrue(ObjectId(a))

    def test_repr_str(self):
        self.assertEqual(repr(ObjectId("123456789012")), "ObjectId('123456789012')")
        self.assertEqual(str(ObjectId("123456789012")), "123456789012")

    def test_cmp(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertEqual(ObjectId("123456789012"), ObjectId("123456789012"))
        self.assertNotEqual(ObjectId(), ObjectId())
        self.assertNotEqual(ObjectId("123456789012"), "123456789012")

if __name__ == "__main__":
    unittest.main()

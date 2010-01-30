# Copyright 2009-2010 10gen, Inc.
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

"""Tests for the Binary wrapper."""

import unittest
import sys
sys.path[0:0] = [""]

from pymongo.binary import Binary


class TestBinary(unittest.TestCase):

    def setUp(self):
        pass

    def test_binary(self):
        a_string = "hello world"
        a_binary = Binary("hello world")
        self.assert_(a_binary.startswith("hello"))
        self.assert_(a_binary.endswith("world"))
        self.assert_(isinstance(a_binary, Binary))
        self.failIf(isinstance(a_string, Binary))

    def test_exceptions(self):
        self.assertRaises(TypeError, Binary, None)
        self.assertRaises(TypeError, Binary, u"hello")
        self.assertRaises(TypeError, Binary, 5)
        self.assertRaises(TypeError, Binary, 10.2)
        self.assertRaises(TypeError, Binary, "hello", None)
        self.assertRaises(TypeError, Binary, "hello", "100")
        self.assertRaises(ValueError, Binary, "hello", -1)
        self.assertRaises(ValueError, Binary, "hello", 256)
        self.assert_(Binary("hello", 0))
        self.assert_(Binary("hello", 255))

    def test_subtype(self):
        b = Binary("hello")
        self.assertEqual(b.subtype, 2)
        c = Binary("hello", 100)
        self.assertEqual(c.subtype, 100)

    def test_equality(self):
        b = Binary("hello")
        c = Binary("hello", 100)
        self.assertNotEqual(b, c)
        self.assertEqual(c, Binary("hello", 100))
        self.assertEqual(b, Binary("hello"))
        self.assertNotEqual(b, Binary("hello "))
        self.assertNotEqual("hello", Binary("hello"))

    def test_repr(self):
        b = Binary("hello world")
        self.assertEqual(repr(b), "Binary('hello world', 2)")
        c = Binary("\x08\xFF")
        self.assertEqual(repr(c), "Binary('\\x08\\xff', 2)")
        d = Binary("test", 100)
        self.assertEqual(repr(d), "Binary('test', 100)")

if __name__ == "__main__":
    unittest.main()

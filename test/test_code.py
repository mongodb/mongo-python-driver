# -*- coding: utf-8 -*-
#
# Copyright 2009-present MongoDB, Inc.
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

"""Tests for the Code wrapper."""

import sys

sys.path[0:0] = [""]

from test import unittest

from bson.code import Code


class TestCode(unittest.TestCase):
    def test_types(self):
        self.assertRaises(TypeError, Code, 5)
        self.assertRaises(TypeError, Code, None)
        self.assertRaises(TypeError, Code, "aoeu", 5)
        self.assertRaises(TypeError, Code, u"aoeu", 5)
        self.assertTrue(Code("aoeu"))
        self.assertTrue(Code(u"aoeu"))
        self.assertTrue(Code("aoeu", {}))
        self.assertTrue(Code(u"aoeu", {}))

    def test_read_only(self):
        c = Code("blah")

        def set_c():
            c.scope = 5

        self.assertRaises(AttributeError, set_c)

    def test_code(self):
        a_string = "hello world"
        a_code = Code("hello world")
        self.assertTrue(a_code.startswith("hello"))
        self.assertTrue(a_code.endswith("world"))
        self.assertTrue(isinstance(a_code, Code))
        self.assertFalse(isinstance(a_string, Code))
        self.assertIsNone(a_code.scope)
        with_scope = Code("hello world", {"my_var": 5})
        self.assertEqual({"my_var": 5}, with_scope.scope)
        empty_scope = Code("hello world", {})
        self.assertEqual({}, empty_scope.scope)
        another_scope = Code(with_scope, {"new_var": 42})
        self.assertEqual(str(with_scope), str(another_scope))
        self.assertEqual({"new_var": 42, "my_var": 5}, another_scope.scope)
        # No error.
        Code(u"héllø world¡")

    def test_repr(self):
        c = Code("hello world", {})
        self.assertEqual(repr(c), "Code('hello world', {})")
        c.scope["foo"] = "bar"
        self.assertEqual(repr(c), "Code('hello world', {'foo': 'bar'})")
        c = Code("hello world", {"blah": 3})
        self.assertEqual(repr(c), "Code('hello world', {'blah': 3})")
        c = Code("\x08\xFF")
        self.assertEqual(repr(c), "Code(%s, None)" % (repr("\x08\xFF"),))

    def test_equality(self):
        b = Code("hello")
        c = Code("hello", {"foo": 5})
        self.assertNotEqual(b, c)
        self.assertEqual(c, Code("hello", {"foo": 5}))
        self.assertNotEqual(c, Code("hello", {"foo": 6}))
        self.assertEqual(b, Code("hello"))
        self.assertEqual(b, Code("hello", None))
        self.assertNotEqual(b, Code("hello "))
        self.assertNotEqual("hello", Code("hello"))

        # Explicitly test inequality
        self.assertFalse(c != Code("hello", {"foo": 5}))
        self.assertFalse(b != Code("hello"))
        self.assertFalse(b != Code("hello", None))

    def test_hash(self):
        self.assertRaises(TypeError, hash, Code("hello world"))

    def test_scope_preserved(self):
        a = Code("hello")
        b = Code("hello", {"foo": 5})

        self.assertEqual(a, Code(a))
        self.assertEqual(b, Code(b))
        self.assertNotEqual(a, Code(b))
        self.assertNotEqual(b, Code(a))

    def test_scope_kwargs(self):
        self.assertEqual({"a": 1}, Code("", a=1).scope)
        self.assertEqual({"a": 1}, Code("", {"a": 2}, a=1).scope)
        self.assertEqual({"a": 1, "b": 2, "c": 3}, Code("", {"b": 2}, a=1, c=3).scope)


if __name__ == "__main__":
    unittest.main()

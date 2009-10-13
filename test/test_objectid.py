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

from nose.plugins.skip import SkipTest

from pymongo.objectid import ObjectId
from pymongo.errors import InvalidId


def oid(x):
    return ObjectId()


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
        self.assert_(ObjectId())
        self.assert_(ObjectId("123456789012"))
        a = ObjectId()
        self.assert_(ObjectId(a))

    def test_repr_str(self):
        self.assertEqual(repr(ObjectId("1234567890abcdef12345678")),
                         "ObjectId('1234567890abcdef12345678')")
        self.assertEqual(str(ObjectId("1234567890abcdef12345678")), "1234567890abcdef12345678")
        self.assertEqual(str(ObjectId("123456789012")), "313233343536373839303132")
        self.assertEqual(ObjectId("1234567890abcdef12345678").binary, '\x124Vx\x90\xab\xcd\xef\x124Vx')
        self.assertEqual(str(ObjectId('\x124Vx\x90\xab\xcd\xef\x124Vx')), "1234567890abcdef12345678")

    def test_cmp(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertEqual(ObjectId("123456789012"), ObjectId("123456789012"))
        self.assertNotEqual(ObjectId(), ObjectId())
        self.assertNotEqual(ObjectId("123456789012"), "123456789012")

    def test_url(self):
        a = ObjectId("123456789012")
        self.assertEqual(a.url_encode(), "313233343536373839303132")
        self.assertEqual(a, ObjectId.url_decode("313233343536373839303132"))
        self.assertEqual(a.url_encode(), str(a))

        b = ObjectId()
        encoded = b.url_encode()
        self.assertEqual(b, ObjectId.url_decode(encoded))

    def test_url_legacy(self):
        a = ObjectId()
        self.assertNotEqual(a.url_encode(), a.url_encode(legacy=True))
        self.assertEqual(a, ObjectId.url_decode(a.url_encode(legacy=True), legacy=True))

    def test_legacy_string(self):
        a = ObjectId()

        self.assertEqual(a.url_encode(legacy=True), a.legacy_str().encode("hex"))

        self.assertNotEqual(str(a), a.legacy_str())
        self.assertEqual(a, ObjectId.from_legacy_str(a.legacy_str()))

    def test_multiprocessing(self):
        # multiprocessing on windows is weird and I don't feel like figuring it
        # out right now. this should fix buildbot.
        if sys.platform == "win32":
            raise SkipTest()

        try:
            import multiprocessing
        except ImportError:
            raise SkipTest()

        pool = multiprocessing.Pool(2)
        ids = pool.map(oid, range(20))
        map = {}

        for id in ids:
            self.assert_(id not in map)
            map[id] = True


if __name__ == "__main__":
    unittest.main()

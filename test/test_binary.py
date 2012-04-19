# Copyright 2009-2012 10gen, Inc.
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
try:
    import uuid
    should_test_uuid = True
except ImportError:
    should_test_uuid = False
sys.path[0:0] = [""]

from bson.binary import Binary, UUIDLegacy
from bson.py3compat import b, binary_type
from nose.plugins.skip import SkipTest
from test.test_connection import get_connection


class TestBinary(unittest.TestCase):

    def setUp(self):
        pass

    def test_binary(self):
        a_string = "hello world"
        a_binary = Binary(b("hello world"))
        self.assertTrue(a_binary.startswith(b("hello")))
        self.assertTrue(a_binary.endswith(b("world")))
        self.assertTrue(isinstance(a_binary, Binary))
        self.assertFalse(isinstance(a_string, Binary))

    def test_exceptions(self):
        self.assertRaises(TypeError, Binary, None)
        self.assertRaises(TypeError, Binary, u"hello")
        self.assertRaises(TypeError, Binary, 5)
        self.assertRaises(TypeError, Binary, 10.2)
        self.assertRaises(TypeError, Binary, b("hello"), None)
        self.assertRaises(TypeError, Binary, b("hello"), "100")
        self.assertRaises(ValueError, Binary, b("hello"), -1)
        self.assertRaises(ValueError, Binary, b("hello"), 256)
        self.assertTrue(Binary(b("hello"), 0))
        self.assertTrue(Binary(b("hello"), 255))

    def test_subtype(self):
        one = Binary(b("hello"))
        self.assertEqual(one.subtype, 0)
        two = Binary(b("hello"), 2)
        self.assertEqual(two.subtype, 2)
        three = Binary(b("hello"), 100)
        self.assertEqual(three.subtype, 100)

    def test_equality(self):
        two = Binary(b("hello"))
        three = Binary(b("hello"), 100)
        self.assertNotEqual(two, three)
        self.assertEqual(three, Binary(b("hello"), 100))
        self.assertEqual(two, Binary(b("hello")))
        self.assertNotEqual(two, Binary(b("hello ")))
        self.assertNotEqual(b("hello"), Binary(b("hello")))

    def test_repr(self):
        one = Binary(b("hello world"))
        self.assertEqual(repr(one),
                         "Binary(%s, 0)" % (repr(b("hello world")),))
        two = Binary(b("hello world"), 2)
        self.assertEqual(repr(two),
                         "Binary(%s, 2)" % (repr(b("hello world")),))
        three = Binary(b("\x08\xFF"))
        self.assertEqual(repr(three),
                         "Binary(%s, 0)" % (repr(b("\x08\xFF")),))
        four = Binary(b("\x08\xFF"), 2)
        self.assertEqual(repr(four),
                         "Binary(%s, 2)" % (repr(b("\x08\xFF")),))
        five = Binary(b("test"), 100)
        self.assertEqual(repr(five),
                         "Binary(%s, 100)" % (repr(b("test")),))

    def test_uuid_queries(self):
        if not should_test_uuid:
            raise SkipTest()

        c = get_connection()
        coll = c.pymongo_test.test
        coll.drop()

        uu = uuid.uuid4()
        # Wrap uu.bytes in binary_type to work
        # around http://bugs.python.org/issue7380.
        coll.insert({'uuid': Binary(binary_type(uu.bytes), 3)})
        self.assertEqual(1, coll.count())

        # Test UUIDLegacy queries.
        coll.uuid_subtype = 4
        self.assertEqual(0, coll.find({'uuid': uu}).count())
        cur = coll.find({'uuid': UUIDLegacy(uu)})
        self.assertEqual(1, cur.count())
        retrieved = cur.next()
        self.assertEqual(uu, retrieved['uuid'])

        # Test regular UUID queries (using subtype 4).
        coll.insert({'uuid': uu})
        self.assertEqual(2, coll.count())
        cur = coll.find({'uuid': uu})
        self.assertEqual(1, cur.count())
        retrieved = cur.next()
        self.assertEqual(uu, retrieved['uuid'])

        # Test both.
        cur = coll.find({'uuid': {'$in': [uu, UUIDLegacy(uu)]}})
        self.assertEqual(2, cur.count())
        coll.drop()


if __name__ == "__main__":
    unittest.main()

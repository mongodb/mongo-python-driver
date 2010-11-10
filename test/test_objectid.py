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

"""Tests for the objectid module."""

import datetime
import warnings
import unittest
import sys
import time
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.errors import InvalidId
from bson.objectid import ObjectId
from bson.tz_util import (FixedOffset,
                          utc)

def oid(x):
    return ObjectId()


class TestObjectId(unittest.TestCase):

    def setUp(self):
        pass

    def test_creation(self):
        self.assertRaises(TypeError, ObjectId, 4)
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

    def test_unicode(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(unicode(a)))
        self.assertEqual(ObjectId("123456789012123456789012"),
                         ObjectId(u"123456789012123456789012"))
        self.assertRaises(InvalidId, ObjectId, u"hello")

    def test_from_hex(self):
        ObjectId("123456789012123456789012")
        self.assertRaises(InvalidId, ObjectId, "123456789012123456789G12")
        self.assertRaises(InvalidId, ObjectId, u"123456789012123456789G12")

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

    def test_binary_str_equivalence(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a.binary))
        self.assertEqual(a, ObjectId(str(a)))

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
        pool.close()
        pool.join()

        map = {}

        for id in ids:
            self.assert_(id not in map)
            map[id] = True

    def test_generation_time(self):
        d1 = datetime.datetime.utcnow()
        d2 = ObjectId().generation_time

        self.assertEqual(utc, d2.tzinfo)
        d2 = d2.replace(tzinfo=None)
        self.assert_(d2 - d1 < datetime.timedelta(seconds = 2))

    def test_from_datetime(self):
        d = datetime.datetime.utcnow()
        d = d - datetime.timedelta(microseconds=d.microsecond)
        oid = ObjectId.from_datetime(d)
        self.assertEqual(d, oid.generation_time.replace(tzinfo=None))
        self.assertEqual("0" * 16, str(oid)[8:])

        aware = datetime.datetime(1993, 4, 4, 2, tzinfo=FixedOffset(555, "SomeZone"))
        as_utc = (aware - aware.utcoffset()).replace(tzinfo=utc)
        oid = ObjectId.from_datetime(aware)
        self.assertEqual(as_utc, oid.generation_time)

if __name__ == "__main__":
    unittest.main()

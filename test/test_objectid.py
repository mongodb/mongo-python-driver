# Copyright 2009-2014 MongoDB, Inc.
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
import pickle
import unittest
import sys
import time
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from bson.errors import InvalidId
from bson.objectid import ObjectId
from bson.py3compat import b, binary_type
from bson.tz_util import (FixedOffset,
                          utc)

PY3 = sys.version_info[0] == 3


def oid(x):
    return ObjectId()


class TestObjectId(unittest.TestCase):
    def test_creation(self):
        self.assertRaises(TypeError, ObjectId, 4)
        self.assertRaises(TypeError, ObjectId, 175.0)
        self.assertRaises(TypeError, ObjectId, {"test": 4})
        self.assertRaises(TypeError, ObjectId, ["something"])
        self.assertRaises(InvalidId, ObjectId, "")
        self.assertRaises(InvalidId, ObjectId, "12345678901")
        self.assertRaises(InvalidId, ObjectId, "1234567890123")
        self.assertTrue(ObjectId())
        self.assertTrue(ObjectId(b("123456789012")))
        a = ObjectId()
        self.assertTrue(ObjectId(a))

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
        self.assertEqual(str(ObjectId("1234567890abcdef12345678")),
                         "1234567890abcdef12345678")
        self.assertEqual(str(ObjectId(b("123456789012"))),
                         "313233343536373839303132")
        self.assertEqual(ObjectId("1234567890abcdef12345678").binary,
                         b('\x124Vx\x90\xab\xcd\xef\x124Vx'))
        self.assertEqual(str(ObjectId(b('\x124Vx\x90\xab\xcd\xef\x124Vx'))),
                         "1234567890abcdef12345678")

    def test_equality(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertEqual(ObjectId(b("123456789012")),
                         ObjectId(b("123456789012")))
        self.assertNotEqual(ObjectId(), ObjectId())
        self.assertNotEqual(ObjectId(b("123456789012")), b("123456789012"))

        # Explicitly test inequality
        self.assertFalse(a != ObjectId(a))
        self.assertFalse(ObjectId(b("123456789012")) !=
                         ObjectId(b("123456789012")))

    def test_binary_str_equivalence(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a.binary))
        self.assertEqual(a, ObjectId(str(a)))

    def test_multiprocessing(self):
        # multiprocessing on windows is weird and I don't feel like figuring it
        # out right now. this should fix buildbot.
        if sys.platform == "win32":
            raise SkipTest("Can't fork on Windows")

        try:
            import multiprocessing
        except ImportError:
            raise SkipTest("No multiprocessing module")

        pool = multiprocessing.Pool(2)
        ids = pool.map(oid, range(20))
        pool.close()
        pool.join()

        map = {}

        for id in ids:
            self.assertTrue(id not in map)
            map[id] = True

    def test_generation_time(self):
        d1 = datetime.datetime.utcnow()
        d2 = ObjectId().generation_time

        self.assertEqual(utc, d2.tzinfo)
        d2 = d2.replace(tzinfo=None)
        self.assertTrue(d2 - d1 < datetime.timedelta(seconds=2))

    def test_from_datetime(self):
        if 'PyPy 1.8.0' in sys.version:
            # See https://bugs.pypy.org/issue1092
            raise SkipTest("datetime.timedelta is broken in pypy 1.8.0")
        d = datetime.datetime.utcnow()
        d = d - datetime.timedelta(microseconds=d.microsecond)
        oid = ObjectId.from_datetime(d)
        self.assertEqual(d, oid.generation_time.replace(tzinfo=None))
        self.assertEqual("0" * 16, str(oid)[8:])

        aware = datetime.datetime(1993, 4, 4, 2,
                                  tzinfo=FixedOffset(555, "SomeZone"))
        as_utc = (aware - aware.utcoffset()).replace(tzinfo=utc)
        oid = ObjectId.from_datetime(aware)
        self.assertEqual(as_utc, oid.generation_time)

    def test_pickling(self):
        orig = ObjectId()
        for protocol in [0, 1, 2, -1]:
            pkl = pickle.dumps(orig, protocol=protocol)
            self.assertEqual(orig, pickle.loads(pkl))

    def test_pickle_backwards_compatability(self):
        # For a full discussion see http://bugs.python.org/issue6137
        if sys.version.startswith('3.0'):
            raise SkipTest("Python 3.0.x can't unpickle "
                           "objects pickled in Python 2.x.")

        # This string was generated by pickling an ObjectId in pymongo
        # version 1.9
        pickled_with_1_9 = b(
            "ccopy_reg\n_reconstructor\np0\n"
            "(cbson.objectid\nObjectId\np1\nc__builtin__\n"
            "object\np2\nNtp3\nRp4\n"
            "(dp5\nS'_ObjectId__id'\np6\n"
            "S'M\\x9afV\\x13v\\xc0\\x0b\\x88\\x00\\x00\\x00'\np7\nsb.")

        # We also test against a hardcoded "New" pickle format so that we
        # make sure we're backward compatible with the current version in
        # the future as well.
        pickled_with_1_10 = b(
            "ccopy_reg\n_reconstructor\np0\n"
            "(cbson.objectid\nObjectId\np1\nc__builtin__\n"
            "object\np2\nNtp3\nRp4\n"
            "S'M\\x9afV\\x13v\\xc0\\x0b\\x88\\x00\\x00\\x00'\np5\nb."
            )

        if PY3:
            # Have to load using 'latin-1' since these were pickled in python2.x.
            oid_1_9 = pickle.loads(pickled_with_1_9, encoding='latin-1')
            oid_1_10 = pickle.loads(pickled_with_1_10, encoding='latin-1')
        else:
            oid_1_9 = pickle.loads(pickled_with_1_9)
            oid_1_10 = pickle.loads(pickled_with_1_10)

        self.assertEqual(oid_1_9, ObjectId("4d9a66561376c00b88000000"))
        self.assertEqual(oid_1_9, oid_1_10)

    def test_is_valid(self):
        self.assertFalse(ObjectId.is_valid(None))
        self.assertFalse(ObjectId.is_valid(4))
        self.assertFalse(ObjectId.is_valid(175.0))
        self.assertFalse(ObjectId.is_valid({"test": 4}))
        self.assertFalse(ObjectId.is_valid(["something"]))
        self.assertFalse(ObjectId.is_valid(""))
        self.assertFalse(ObjectId.is_valid("12345678901"))
        self.assertFalse(ObjectId.is_valid("1234567890123"))

        self.assertTrue(ObjectId.is_valid(b("123456789012")))
        self.assertTrue(ObjectId.is_valid("123456789012123456789012"))

if __name__ == "__main__":
    unittest.main()

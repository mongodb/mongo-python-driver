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

"""Tests for the objectid module."""

from __future__ import annotations

import datetime
import pickle
import struct
import sys

sys.path[0:0] = [""]

from bson.errors import InvalidId
from bson.objectid import _MAX_COUNTER_VALUE, ObjectId
from bson.tz_util import FixedOffset, utc
from test import SkipTest, unittest
from test.utils_shared import oid_generated_on_process


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
        self.assertTrue(ObjectId(b"123456789012"))
        a = ObjectId()
        self.assertTrue(ObjectId(a))

    def test_unicode(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertRaises(InvalidId, ObjectId, "hello")

    def test_from_hex(self):
        ObjectId("123456789012123456789012")
        self.assertRaises(InvalidId, ObjectId, "123456789012123456789G12")

    def test_from_hex_whitespace(self):
        # bytes.fromhex() skips whitespace, which must not shorten the ObjectId.
        self.assertRaises(InvalidId, ObjectId, "0123                0011")
        self.assertRaises(InvalidId, ObjectId, "00 " * 8)
        self.assertRaises(InvalidId, ObjectId, "01\n" * 8)
        self.assertRaises(InvalidId, ObjectId, " " * 24)

    def test_repr_str(self):
        self.assertEqual(
            repr(ObjectId("1234567890abcdef12345678")), "ObjectId('1234567890abcdef12345678')"
        )
        self.assertEqual(str(ObjectId("1234567890abcdef12345678")), "1234567890abcdef12345678")
        self.assertEqual(str(ObjectId(b"123456789012")), "313233343536373839303132")
        self.assertEqual(
            ObjectId("1234567890abcdef12345678").binary, b"\x124Vx\x90\xab\xcd\xef\x124Vx"
        )
        self.assertEqual(
            str(ObjectId(b"\x124Vx\x90\xab\xcd\xef\x124Vx")), "1234567890abcdef12345678"
        )

    def test_equality(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a))
        self.assertEqual(ObjectId(b"123456789012"), ObjectId(b"123456789012"))
        self.assertNotEqual(ObjectId(), ObjectId())
        self.assertNotEqual(ObjectId(b"123456789012"), b"123456789012")

        # Explicitly test inequality
        self.assertFalse(a != ObjectId(a))
        self.assertFalse(ObjectId(b"123456789012") != ObjectId(b"123456789012"))

    def test_binary_str_equivalence(self):
        a = ObjectId()
        self.assertEqual(a, ObjectId(a.binary))
        self.assertEqual(a, ObjectId(str(a)))

    def test_generation_time(self):
        d1 = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        d2 = ObjectId().generation_time

        self.assertEqual(utc, d2.tzinfo)
        d2 = d2.replace(tzinfo=None)
        self.assertLess(d2 - d1, datetime.timedelta(seconds=2))

    def test_from_datetime(self):
        d = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        d = d - datetime.timedelta(microseconds=d.microsecond)
        oid = ObjectId.from_datetime(d)
        self.assertEqual(d, oid.generation_time.replace(tzinfo=None))
        self.assertEqual("0" * 16, str(oid)[8:])

        aware = datetime.datetime(1993, 4, 4, 2, tzinfo=FixedOffset(555, "SomeZone"))
        offset = aware.utcoffset()
        assert offset is not None
        as_utc = (aware - offset).replace(tzinfo=utc)
        oid = ObjectId.from_datetime(aware)
        self.assertEqual(as_utc, oid.generation_time)

    def test_from_datetime_unique(self):
        d = datetime.datetime(2020, 5, 17, 10, 30, 15)
        oid1 = ObjectId.from_datetime(d, unique=True)
        oid2 = ObjectId.from_datetime(d, unique=True)

        # Same timestamp, but unique and not the zero-filled dummy.
        self.assertEqual(d, oid1.generation_time.replace(tzinfo=None))
        self.assertEqual(d, oid2.generation_time.replace(tzinfo=None))
        self.assertNotEqual(oid1, oid2)
        self.assertNotEqual("0" * 16, str(oid1)[8:])

        # Same layout as ObjectId(): 5-byte process random, then the counter.
        self.assertEqual(oid1.binary[4:9], ObjectId._random())
        self.assertEqual(oid1.binary[4:9], ObjectId().binary[4:9])
        inc1 = struct.unpack(">I", b"\x00" + oid1.binary[9:12])[0]
        inc2 = struct.unpack(">I", b"\x00" + oid2.binary[9:12])[0]
        self.assertEqual((inc1 + 1) % (_MAX_COUNTER_VALUE + 1), inc2)

        # Aware datetimes are converted to UTC, as with unique=False.
        aware = datetime.datetime(1993, 4, 4, 2, tzinfo=FixedOffset(555, "SomeZone"))
        self.assertEqual(
            ObjectId.from_datetime(aware).generation_time,
            ObjectId.from_datetime(aware, unique=True).generation_time,
        )

    def test_from_datetime_unique_is_keyword_only(self):
        with self.assertRaises(TypeError):
            ObjectId.from_datetime(datetime.datetime(2020, 1, 1), True)  # type: ignore[misc]

    def test_pickling(self):
        orig = ObjectId()
        for protocol in [0, 1, 2, -1]:
            pkl = pickle.dumps(orig, protocol=protocol)
            self.assertEqual(orig, pickle.loads(pkl))

    def test_pickle_backwards_compatability(self):
        # This string was generated by pickling an ObjectId in pymongo
        # version 1.9
        pickled_with_1_9 = (
            b"ccopy_reg\n_reconstructor\np0\n"
            b"(cbson.objectid\nObjectId\np1\nc__builtin__\n"
            b"object\np2\nNtp3\nRp4\n"
            b"(dp5\nS'_ObjectId__id'\np6\n"
            b"S'M\\x9afV\\x13v\\xc0\\x0b\\x88\\x00\\x00\\x00'\np7\nsb."
        )

        # We also test against a hardcoded "New" pickle format so that we
        # make sure we're backward compatible with the current version in
        # the future as well.
        pickled_with_1_10 = (
            b"ccopy_reg\n_reconstructor\np0\n"
            b"(cbson.objectid\nObjectId\np1\nc__builtin__\n"
            b"object\np2\nNtp3\nRp4\n"
            b"S'M\\x9afV\\x13v\\xc0\\x0b\\x88\\x00\\x00\\x00'\np5\nb."
        )

        # Have to load using 'latin-1' since these were pickled in python2.x.
        oid_1_9 = pickle.loads(pickled_with_1_9, encoding="latin-1")
        oid_1_10 = pickle.loads(pickled_with_1_10, encoding="latin-1")

        self.assertEqual(oid_1_9, ObjectId("4d9a66561376c00b88000000"))
        self.assertEqual(oid_1_9, oid_1_10)

    def test_random_bytes(self):
        self.assertTrue(oid_generated_on_process(ObjectId()))

    def test_is_valid(self):
        self.assertFalse(ObjectId.is_valid(None))
        self.assertFalse(ObjectId.is_valid(4))
        self.assertFalse(ObjectId.is_valid(175.0))
        self.assertFalse(ObjectId.is_valid({"test": 4}))
        self.assertFalse(ObjectId.is_valid(["something"]))
        self.assertFalse(ObjectId.is_valid(""))
        self.assertFalse(ObjectId.is_valid("12345678901"))
        self.assertFalse(ObjectId.is_valid("1234567890123"))
        self.assertFalse(ObjectId.is_valid("0123                0011"))
        self.assertFalse(ObjectId.is_valid("00 " * 8))
        self.assertFalse(ObjectId.is_valid("01\n" * 8))
        self.assertFalse(ObjectId.is_valid(" " * 24))

        self.assertTrue(ObjectId.is_valid(b"123456789012"))
        self.assertTrue(ObjectId.is_valid("123456789012123456789012"))

    def test_counter_overflow(self):
        # Spec-test to check counter overflows from max value to 0.
        ObjectId._inc = _MAX_COUNTER_VALUE
        ObjectId()
        self.assertEqual(ObjectId._inc, 0)

    def test_timestamp_values(self):
        # Spec-test to check timestamp field is interpreted correctly.
        TEST_DATA = {
            0x00000000: (1970, 1, 1, 0, 0, 0),
            0x7FFFFFFF: (2038, 1, 19, 3, 14, 7),
            0x80000000: (2038, 1, 19, 3, 14, 8),
            0xFFFFFFFF: (2106, 2, 7, 6, 28, 15),
        }

        def generate_objectid_with_timestamp(timestamp):
            oid = ObjectId()
            _, trailing_bytes = struct.unpack(">IQ", oid.binary)
            new_oid = struct.pack(">IQ", timestamp, trailing_bytes)
            return ObjectId(new_oid)

        for tstamp, exp_datetime_args in TEST_DATA.items():
            oid = generate_objectid_with_timestamp(tstamp)
            # 32-bit platforms may overflow in datetime.fromtimestamp.
            if tstamp > 0x7FFFFFFF and sys.maxsize < 2**32:
                try:
                    oid.generation_time
                except (OverflowError, ValueError):
                    continue
            self.assertEqual(oid.generation_time, datetime.datetime(*exp_datetime_args, tzinfo=utc))

    def test_random_regenerated_on_pid_change(self):
        # Test that change of pid triggers new random number generation.
        random_original = ObjectId._random()
        ObjectId._pid += 1
        random_new = ObjectId._random()
        self.assertNotEqual(random_original, random_new)


if __name__ == "__main__":
    unittest.main()

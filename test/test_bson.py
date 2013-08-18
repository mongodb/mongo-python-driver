# -*- coding: utf-8 -*-
#
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

"""Test the bson module."""

import unittest
import datetime
import re
import sys
try:
    import uuid
    should_test_uuid = True
except ImportError:
    should_test_uuid = False
sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

import bson
from bson import (BSON,
                  decode_all,
                  is_valid)
from bson.binary import Binary, UUIDLegacy
from bson.code import Code
from bson.objectid import ObjectId
from bson.dbref import DBRef
from bson.py3compat import b
from bson.son import SON
from bson.timestamp import Timestamp
from bson.errors import (InvalidDocument,
                         InvalidStringData)
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.tz_util import (FixedOffset,
                          utc)

from test import qcheck

PY3 = sys.version_info[0] == 3


class TestBSON(unittest.TestCase):

    def setUp(self):
        pass

    def test_basic_validation(self):
        self.assertRaises(TypeError, is_valid, 100)
        self.assertRaises(TypeError, is_valid, u"test")
        self.assertRaises(TypeError, is_valid, 10.4)

        self.assertFalse(is_valid(b("test")))

        # the simplest valid BSON document
        self.assertTrue(is_valid(b("\x05\x00\x00\x00\x00")))
        self.assertTrue(is_valid(BSON(b("\x05\x00\x00\x00\x00"))))
        self.assertFalse(is_valid(b("\x04\x00\x00\x00\x00")))
        self.assertFalse(is_valid(b("\x05\x00\x00\x00\x01")))
        self.assertFalse(is_valid(b("\x05\x00\x00\x00")))
        self.assertFalse(is_valid(b("\x05\x00\x00\x00\x00\x00")))
        self.assertFalse(is_valid(b("\x07\x00\x00\x00\x02a\x00\x78\x56\x34\x12")))
        self.assertFalse(is_valid(b("\x09\x00\x00\x00\x10a\x00\x05\x00")))
        self.assertFalse(is_valid(b("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")))

    def test_random_data_is_not_bson(self):
        qcheck.check_unittest(self, qcheck.isnt(is_valid),
                              qcheck.gen_string(qcheck.gen_range(0, 40)))

    def test_basic_decode(self):
        self.assertEqual({"test": u"hello world"},
                         BSON(b("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C"
                                "\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F"
                                "\x72\x6C\x64\x00\x00")).decode())
        self.assertEqual([{"test": u"hello world"}, {}],
                         decode_all(b("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74"
                                      "\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C"
                                      "\x6f\x20\x77\x6F\x72\x6C\x64\x00\x00"
                                      "\x05\x00\x00\x00\x00")))

    def test_data_timestamp(self):
        self.assertEqual({"test": Timestamp(4, 20)},
                         BSON(b("\x13\x00\x00\x00\x11\x74\x65\x73\x74\x00\x14"
                                "\x00\x00\x00\x04\x00\x00\x00\x00")).decode())

    def test_basic_encode(self):
        self.assertRaises(TypeError, BSON.encode, 100)
        self.assertRaises(TypeError, BSON.encode, "hello")
        self.assertRaises(TypeError, BSON.encode, None)
        self.assertRaises(TypeError, BSON.encode, [])

        self.assertEqual(BSON.encode({}), BSON(b("\x05\x00\x00\x00\x00")))
        self.assertEqual(BSON.encode({"test": u"hello world"}),
                         b("\x1B\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0C\x00"
                           "\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C"
                           "\x64\x00\x00"))
        self.assertEqual(BSON.encode({u"mike": 100}),
                         b("\x0F\x00\x00\x00\x10\x6D\x69\x6B\x65\x00\x64\x00"
                           "\x00\x00\x00"))
        self.assertEqual(BSON.encode({"hello": 1.5}),
                         b("\x14\x00\x00\x00\x01\x68\x65\x6C\x6C\x6F\x00\x00"
                           "\x00\x00\x00\x00\x00\xF8\x3F\x00"))
        self.assertEqual(BSON.encode({"true": True}),
                         b("\x0C\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00"))
        self.assertEqual(BSON.encode({"false": False}),
                         b("\x0D\x00\x00\x00\x08\x66\x61\x6C\x73\x65\x00\x00"
                           "\x00"))
        self.assertEqual(BSON.encode({"empty": []}),
                         b("\x11\x00\x00\x00\x04\x65\x6D\x70\x74\x79\x00\x05"
                           "\x00\x00\x00\x00\x00"))
        self.assertEqual(BSON.encode({"none": {}}),
                         b("\x10\x00\x00\x00\x03\x6E\x6F\x6E\x65\x00\x05\x00"
                           "\x00\x00\x00\x00"))
        self.assertEqual(BSON.encode({"test": Binary(b("test"), 0)}),
                         b("\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00"
                           "\x00\x00\x00\x74\x65\x73\x74\x00"))
        self.assertEqual(BSON.encode({"test": Binary(b("test"), 2)}),
                         b("\x18\x00\x00\x00\x05\x74\x65\x73\x74\x00\x08\x00"
                           "\x00\x00\x02\x04\x00\x00\x00\x74\x65\x73\x74\x00"))
        self.assertEqual(BSON.encode({"test": Binary(b("test"), 128)}),
                         b("\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00"
                           "\x00\x00\x80\x74\x65\x73\x74\x00"))
        self.assertEqual(BSON.encode({"test": None}),
                         b("\x0B\x00\x00\x00\x0A\x74\x65\x73\x74\x00\x00"))
        self.assertEqual(BSON.encode({"date": datetime.datetime(2007, 1, 8,
                                                                0, 30, 11)}),
                         b("\x13\x00\x00\x00\x09\x64\x61\x74\x65\x00\x38\xBE"
                           "\x1C\xFF\x0F\x01\x00\x00\x00"))
        self.assertEqual(BSON.encode({"regex": re.compile(b("a*b"),
                                                          re.IGNORECASE)}),
                         b("\x12\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61"
                           "\x2A\x62\x00\x69\x00\x00"))
        self.assertEqual(BSON.encode({"$where": Code("test")}),
                         b("\x16\x00\x00\x00\r$where\x00\x05\x00\x00\x00test"
                           "\x00\x00"))
        self.assertEqual(BSON.encode({"$field":
                         Code("function(){ return true;}", scope=None)}),
                         b("+\x00\x00\x00\r$field\x00\x1a\x00\x00\x00"
                           "function(){ return true;}\x00\x00"))
        self.assertEqual(BSON.encode({"$field":
                          Code("return function(){ return x; }",
                            scope={'x': False})}),
                         b("=\x00\x00\x00\x0f$field\x000\x00\x00\x00\x1f\x00"
                           "\x00\x00return function(){ return x; }\x00\t\x00"
                           "\x00\x00\x08x\x00\x00\x00\x00"))
        a = ObjectId(b("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B"))
        self.assertEqual(BSON.encode({"oid": a}),
                         b("\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x00\x01\x02"
                           "\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00"))
        self.assertEqual(BSON.encode({"ref": DBRef("coll", a)}),
                         b("\x2F\x00\x00\x00\x03ref\x00\x25\x00\x00\x00\x02"
                           "$ref\x00\x05\x00\x00\x00coll\x00\x07$id\x00\x00"
                           "\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00"
                           "\x00"))

    def test_encode_then_decode(self):

        def helper(dict):
            self.assertEqual(dict, (BSON.encode(dict)).decode())
        helper({})
        helper({"test": u"hello"})
        self.assertTrue(isinstance(BSON.encode({"hello": "world"})
                                .decode()["hello"],
                                unicode))
        helper({"mike": -10120})
        helper({"long": long(10)})
        helper({"really big long": 2147483648})
        helper({u"hello": 0.0013109})
        helper({"something": True})
        helper({"false": False})
        helper({"an array": [1, True, 3.8, u"world"]})
        helper({"an object": {"test": u"something"}})
        helper({"a binary": Binary(b("test"), 100)})
        helper({"a binary": Binary(b("test"), 128)})
        helper({"a binary": Binary(b("test"), 254)})
        helper({"another binary": Binary(b("test"), 2)})
        helper(SON([(u'test dst', datetime.datetime(1993, 4, 4, 2))]))
        helper(SON([(u'test negative dst', datetime.datetime(1, 1, 1, 1, 1, 1))]))
        helper({"big float": float(10000000000)})
        helper({"ref": DBRef("coll", 5)})
        helper({"ref": DBRef("coll", 5, foo="bar", bar=4)})
        helper({"ref": DBRef("coll", 5, "foo")})
        helper({"ref": DBRef("coll", 5, "foo", foo="bar")})
        helper({"ref": Timestamp(1, 2)})
        helper({"foo": MinKey()})
        helper({"foo": MaxKey()})
        helper({"$field": Code("function(){ return true; }")})
        helper({"$field": Code("return function(){ return x; }", scope={'x': False})})

        doc_class = dict
        # Work around http://bugs.jython.org/issue1728
        if (sys.platform.startswith('java') and
            sys.version_info[:3] >= (2, 5, 2)):
            doc_class = SON

        def encode_then_decode(doc):
            return doc == (BSON.encode(doc)).decode(as_class=doc_class)

        qcheck.check_unittest(self, encode_then_decode,
                              qcheck.gen_mongo_dict(3))

    def test_bytes_as_keys(self):
        doc = {b("foo"): 'bar'}
        # Since `bytes` are stored as Binary you can't use them
        # as keys in python 3.x. Using binary data as a key makes
        # no sense in BSON anyway and little sense in python.
        if PY3:
            self.assertRaises(InvalidDocument, BSON.encode, doc)
        else:
            self.assertTrue(BSON.encode(doc))

    def test_datetime_encode_decode(self):
        # Negative timestamps
        dt1 = datetime.datetime(1, 1, 1, 1, 1, 1, 111000)
        dt2 = BSON.encode({"date": dt1}).decode()["date"]
        self.assertEqual(dt1, dt2)

        dt1 = datetime.datetime(1959, 6, 25, 12, 16, 59, 999000)
        dt2 = BSON.encode({"date": dt1}).decode()["date"]
        self.assertEqual(dt1, dt2)

        # Positive timestamps
        dt1 = datetime.datetime(9999, 12, 31, 23, 59, 59, 999000)
        dt2 = BSON.encode({"date": dt1}).decode()["date"]
        self.assertEqual(dt1, dt2)

        dt1 = datetime.datetime(2011, 6, 14, 10, 47, 53, 444000)
        dt2 = BSON.encode({"date": dt1}).decode()["date"]
        self.assertEqual(dt1, dt2)

    def test_aware_datetime(self):
        aware = datetime.datetime(1993, 4, 4, 2,
                                  tzinfo=FixedOffset(555, "SomeZone"))
        as_utc = (aware - aware.utcoffset()).replace(tzinfo=utc)
        self.assertEqual(datetime.datetime(1993, 4, 3, 16, 45, tzinfo=utc),
                         as_utc)
        after = BSON.encode({"date": aware}).decode(tz_aware=True)["date"]
        self.assertEqual(utc, after.tzinfo)
        self.assertEqual(as_utc, after)

    def test_naive_decode(self):
        aware = datetime.datetime(1993, 4, 4, 2,
                                  tzinfo=FixedOffset(555, "SomeZone"))
        naive_utc = (aware - aware.utcoffset()).replace(tzinfo=None)
        self.assertEqual(datetime.datetime(1993, 4, 3, 16, 45), naive_utc)
        after = BSON.encode({"date": aware}).decode()["date"]
        self.assertEqual(None, after.tzinfo)
        self.assertEqual(naive_utc, after)

    def test_dst(self):
        d = {"x": datetime.datetime(1993, 4, 4, 2)}
        self.assertEqual(d, BSON.encode(d).decode())

    def test_bad_encode(self):
        if not PY3:
            # Python3 treats this as a unicode string which won't raise
            # an exception. If we passed the string as bytes instead we
            # still wouldn't get an error since we store bytes as BSON
            # binary subtype 0.
            self.assertRaises(InvalidStringData, BSON.encode,
                              {"lalala": '\xf4\xe0\xf0\xe1\xc0 Color Touch'})
        evil_list = {'a': []}
        evil_list['a'].append(evil_list)
        evil_dict = {}
        evil_dict['a'] = evil_dict
        for evil_data in [evil_dict, evil_list]:
            self.assertRaises(RuntimeError, BSON.encode, evil_data)

    def test_overflow(self):
        self.assertTrue(BSON.encode({"x": 9223372036854775807L}))
        self.assertRaises(OverflowError, BSON.encode,
                          {"x": 9223372036854775808L})

        self.assertTrue(BSON.encode({"x": -9223372036854775808L}))
        self.assertRaises(OverflowError, BSON.encode,
                          {"x": -9223372036854775809L})

    def test_small_long_encode_decode(self):
        if PY3:
            raise SkipTest("No long type in Python 3.")

        encoded1 = BSON.encode({'x': 256})
        decoded1 = BSON.decode(encoded1)['x']
        self.assertEqual(256, decoded1)
        self.assertEqual(type(256), type(decoded1))

        encoded2 = BSON.encode({'x': 256L})
        decoded2 = BSON.decode(encoded2)['x']
        self.assertEqual(256L, decoded2)
        self.assertEqual(type(256L), type(decoded2))

        self.assertNotEqual(type(decoded1), type(decoded2))

    def test_tuple(self):
        self.assertEqual({"tuple": [1, 2]},
                          BSON.encode({"tuple": (1, 2)}).decode())

    def test_uuid(self):
        if not should_test_uuid:
            raise SkipTest("No uuid module")

        id = uuid.uuid4()
        transformed_id = (BSON.encode({"id": id})).decode()["id"]

        self.assertTrue(isinstance(transformed_id, uuid.UUID))
        self.assertEqual(id, transformed_id)
        self.assertNotEqual(uuid.uuid4(), transformed_id)

    def test_uuid_legacy(self):
        if not should_test_uuid:
            raise SkipTest("No uuid module")

        id = uuid.uuid4()
        legacy = UUIDLegacy(id)
        self.assertEqual(3, legacy.subtype)
        transformed = (BSON.encode({"uuid": legacy})).decode()["uuid"]
        self.assertTrue(isinstance(transformed, uuid.UUID))
        self.assertEqual(id, transformed)
        self.assertNotEqual(UUIDLegacy(uuid.uuid4()), UUIDLegacy(transformed))

    # The C extension was segfaulting on unicode RegExs, so we have this test
    # that doesn't really test anything but the lack of a segfault.
    def test_unicode_regex(self):
        regex = re.compile(u'revisi\xf3n')
        BSON.encode({"regex": regex}).decode()

    def test_non_string_keys(self):
        self.assertRaises(InvalidDocument, BSON.encode, {8.9: "test"})

    def test_utf8(self):
        w = {u"aéあ": u"aéあ"}
        self.assertEqual(w, BSON.encode(w).decode())

        iso8859_bytes = u"aé".encode("iso-8859-1")
        y = {"hello": iso8859_bytes}
        if PY3:
            # Stored as BSON binary subtype 0.
            out = BSON.encode(y).decode()
            self.assertTrue(isinstance(out['hello'], bytes))
            self.assertEqual(out['hello'], iso8859_bytes)
        else:
            # Python 2.
            try:
                BSON.encode(y)
            except InvalidStringData, e:
                self.assertTrue(repr(iso8859_bytes) in str(e))

            # The next two tests only make sense in python 2.x since
            # you can't use `bytes` type as document keys in python 3.x.
            x = {u"aéあ".encode("utf-8"): u"aéあ".encode("utf-8")}
            self.assertEqual(w, BSON.encode(x).decode())

            z = {iso8859_bytes: "hello"}
            self.assertRaises(InvalidStringData, BSON.encode, z)

    def test_null_character(self):
        doc = {"a": "\x00"}
        self.assertEqual(doc, BSON.encode(doc).decode())

        # This test doesn't make much sense in Python2
        # since {'a': '\x00'} == {'a': u'\x00'}.
        # Decoding here actually returns {'a': '\x00'}
        doc = {"a": u"\x00"}
        self.assertEqual(doc, BSON.encode(doc).decode())

        self.assertRaises(InvalidDocument, BSON.encode, {b("\x00"): "a"})
        self.assertRaises(InvalidDocument, BSON.encode, {u"\x00": "a"})

        self.assertRaises(InvalidDocument, BSON.encode,
                          {"a": re.compile(b("ab\x00c"))})
        self.assertRaises(InvalidDocument, BSON.encode,
                          {"a": re.compile(u"ab\x00c")})

    def test_move_id(self):
        self.assertEqual(b("\x19\x00\x00\x00\x02_id\x00\x02\x00\x00\x00a\x00"
                           "\x02a\x00\x02\x00\x00\x00a\x00\x00"),
                         BSON.encode(SON([("a", "a"), ("_id", "a")])))

        self.assertEqual(b("\x2c\x00\x00\x00"
                           "\x02_id\x00\x02\x00\x00\x00b\x00"
                           "\x03b\x00"
                           "\x19\x00\x00\x00\x02a\x00\x02\x00\x00\x00a\x00"
                           "\x02_id\x00\x02\x00\x00\x00a\x00\x00\x00"),
                         BSON.encode(SON([("b",
                                           SON([("a", "a"), ("_id", "a")])),
                                          ("_id", "b")])))

    def test_dates(self):
        doc = {"early": datetime.datetime(1686, 5, 5),
               "late": datetime.datetime(2086, 5, 5)}
        try:
            self.assertEqual(doc, BSON.encode(doc).decode())
        except ValueError:
            # Ignore ValueError when no C ext, since it's probably
            # a problem w/ 32-bit Python - we work around this in the
            # C ext, though.
            if bson.has_c():
                raise

    def test_custom_class(self):
        self.assertTrue(isinstance(BSON.encode({}).decode(), dict))
        self.assertFalse(isinstance(BSON.encode({}).decode(), SON))
        self.assertTrue(isinstance(BSON.encode({}).decode(SON), SON))

        self.assertEqual(1, BSON.encode({"x": 1}).decode(SON)["x"])

        x = BSON.encode({"x": [{"y": 1}]})
        self.assertTrue(isinstance(x.decode(SON)["x"][0], SON))

    def test_subclasses(self):
        # make sure we can serialize subclasses of native Python types.
        class _myint(int):
            pass

        class _myfloat(float):
            pass

        class _myunicode(unicode):
            pass

        d = {'a': _myint(42), 'b': _myfloat(63.9),
             'c': _myunicode('hello world')
            }
        d2 = BSON.encode(d).decode()
        for key, value in d2.iteritems():
            orig_value = d[key]
            orig_type = orig_value.__class__.__bases__[0]
            self.assertEqual(type(value), orig_type)
            self.assertEqual(value, orig_type(value))

    def test_ordered_dict(self):
        try:
            from collections import OrderedDict
        except ImportError:
            raise SkipTest("No OrderedDict")
        d = OrderedDict([("one", 1), ("two", 2), ("three", 3), ("four", 4)])
        self.assertEqual(d, BSON.encode(d).decode(as_class=OrderedDict))

if __name__ == "__main__":
    unittest.main()

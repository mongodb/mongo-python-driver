# -*- coding: utf-8 -*-
#
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

import pymongo
from pymongo.binary import Binary
from pymongo.code import Code
from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef
from pymongo.son import SON
from pymongo.timestamp import Timestamp
from pymongo.bson import BSON, is_valid, _to_dicts
from pymongo.errors import InvalidDocument, InvalidStringData
import qcheck

class SomeZone(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(minutes=555)
    def dst(self, dt):
        # a fixed-offset class:  doesn't account for DST
        return datetime.timedelta(0)

class TestBSON(unittest.TestCase):

    def setUp(self):
        pass

    def test_basic_validation(self):
        self.assertRaises(TypeError, is_valid, 100)
        self.assertRaises(TypeError, is_valid, u"test")
        self.assertRaises(TypeError, is_valid, 10.4)

        self.failIf(is_valid("test"))

        # the simplest valid BSON document
        self.assert_(is_valid("\x05\x00\x00\x00\x00"))
        self.assert_(is_valid(BSON("\x05\x00\x00\x00\x00")))
        self.failIf(is_valid("\x04\x00\x00\x00\x00"))
        self.failIf(is_valid("\x05\x00\x00\x00\x01"))
        self.failIf(is_valid("\x05\x00\x00\x00"))
        self.failIf(is_valid("\x05\x00\x00\x00\x00\x00"))

    def test_random_data_is_not_bson(self):
        qcheck.check_unittest(self, qcheck.isnt(is_valid),
                              qcheck.gen_string(qcheck.gen_range(0, 40)))

    def test_basic_to_dict(self):
        self.assertEqual({"test": u"hello world"},
                         BSON("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C"
                              "\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F"
                              "\x72\x6C\x64\x00\x00").to_dict())
        self.assertEqual([{"test": u"hello world"}, {}],
                         _to_dicts("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00"
                                   "\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20"
                                   "\x77\x6F\x72\x6C\x64\x00\x00\x05\x00\x00"
                                   "\x00\x00"))

    def test_data_timestamp(self):
        self.assertEqual({"test": Timestamp(4, 20)},
                         BSON("\x13\x00\x00\x00\x11\x74\x65\x73\x74\x00\x14"
                              "\x00\x00\x00\x04\x00\x00\x00\x00").to_dict())

    def test_basic_from_dict(self):
        self.assertRaises(TypeError, BSON.from_dict, 100)
        self.assertRaises(TypeError, BSON.from_dict, "hello")
        self.assertRaises(TypeError, BSON.from_dict, None)
        self.assertRaises(TypeError, BSON.from_dict, [])

        self.assertEqual(BSON.from_dict({}), BSON("\x05\x00\x00\x00\x00"))
        self.assertEqual(BSON.from_dict({"test": u"hello world"}),
                         "\x1B\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0C\x00\x00"
                         "\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00"
                         "\x00")
        self.assertEqual(BSON.from_dict({u"mike": 100}),
                         "\x0F\x00\x00\x00\x10\x6D\x69\x6B\x65\x00\x64\x00\x00"
                         "\x00\x00")
        self.assertEqual(BSON.from_dict({"hello": 1.5}),
                         "\x14\x00\x00\x00\x01\x68\x65\x6C\x6C\x6F\x00\x00\x00"
                         "\x00\x00\x00\x00\xF8\x3F\x00")
        self.assertEqual(BSON.from_dict({"true": True}),
                         "\x0C\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00")
        self.assertEqual(BSON.from_dict({"false": False}),
                         "\x0D\x00\x00\x00\x08\x66\x61\x6C\x73\x65\x00\x00"
                         "\x00")
        self.assertEqual(BSON.from_dict({"empty": []}),
                         "\x11\x00\x00\x00\x04\x65\x6D\x70\x74\x79\x00\x05\x00"
                         "\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"none": {}}),
                         "\x10\x00\x00\x00\x03\x6E\x6F\x6E\x65\x00\x05\x00\x00"
                         "\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"test": Binary("test")}),
                         "\x18\x00\x00\x00\x05\x74\x65\x73\x74\x00\x08\x00\x00"
                         "\x00\x02\x04\x00\x00\x00\x74\x65\x73\x74\x00")
        self.assertEqual(BSON.from_dict({"test": Binary("test", 128)}),
                         "\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00\x00"
                         "\x00\x80\x74\x65\x73\x74\x00")
        self.assertEqual(BSON.from_dict({"test": None}),
                         "\x0B\x00\x00\x00\x0A\x74\x65\x73\x74\x00\x00")
        self.assertEqual(BSON.from_dict({"date": datetime.datetime(2007, 1, 8,
                                                                   0, 30,
                                                                   11)}),
                         "\x13\x00\x00\x00\x09\x64\x61\x74\x65\x00\x38\xBE\x1C"
                         "\xFF\x0F\x01\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"regex": re.compile("a*b",
                                                             re.IGNORECASE)}),
                         "\x12\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61\x2A"
                         "\x62\x00\x69\x00\x00")
        self.assertEqual(BSON.from_dict({"$where": Code("test")}),
                         "\x1F\x00\x00\x00\x0F\x24\x77\x68\x65\x72\x65\x00\x12"
                         "\x00\x00\x00\x05\x00\x00\x00\x74\x65\x73\x74\x00\x05"
                         "\x00\x00\x00\x00\x00")
        a = ObjectId("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B")
        self.assertEqual(BSON.from_dict({"oid": a}),
                         "\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x00\x01\x02\x03"
                         "\x04\x05\x06\x07\x08\x09\x0A\x0B\x00")
        self.assertEqual(BSON.from_dict({"ref": DBRef("coll", a)}),
                         "\x2F\x00\x00\x00\x03ref\x00\x25\x00\x00\x00\x02$ref"
                         "\x00\x05\x00\x00\x00coll\x00\x07$id\x00\x00\x01\x02"
                         "\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x00\x00")

    def test_from_then_to_dict(self):

        def helper(dict):
            self.assertEqual(dict, (BSON.from_dict(dict)).to_dict())
        helper({})
        helper({"test": u"hello"})
        self.assert_(isinstance(BSON.from_dict({"hello": "world"})
                                .to_dict()["hello"],
                                unicode))
        helper({"mike": -10120})
        helper({"long": long(10)})
        helper({"really big long": 2147483648})
        helper({u"hello": 0.0013109})
        helper({"something": True})
        helper({"false": False})
        helper({"an array": [1, True, 3.8, u"world"]})
        helper({"an object": {"test": u"something"}})
        helper({"a binary": Binary("test", 100)})
        helper({"a binary": Binary("test", 128)})
        helper({"a binary": Binary("test", 254)})
        helper({"another binary": Binary("test")})
        helper(SON([(u'test dst', datetime.datetime(1993, 4, 4, 2))]))
        helper({"big float": float(10000000000)})
        helper({"ref": DBRef("coll", 5)})
        helper({"ref": DBRef("coll", 5, "foo")})
        helper({"ref": Timestamp(1,2)})

        def from_then_to_dict(dict):
            return dict == (BSON.from_dict(dict)).to_dict()

        qcheck.check_unittest(self, from_then_to_dict,
                              qcheck.gen_mongo_dict(3))

    def test_reject_timezone(self):
        try:
            BSON.from_dict({u'test zone dst' :
                            datetime.datetime(1993, 4, 4, 2, tzinfo=SomeZone())})
        except NotImplementedError:
            return
        raise ValueError("datetime with zone didn't error")

    def test_bad_encode(self):
        self.assertRaises(InvalidStringData, BSON.from_dict,
                          {"lalala": '\xf4\xe0\xf0\xe1\xc0 Color Touch'})

    def test_overflow(self):
        self.assert_(BSON.from_dict({"x": 9223372036854775807L}))
        self.assertRaises(OverflowError, BSON.from_dict, {"x": 9223372036854775808L})

        self.assert_(BSON.from_dict({"x": -9223372036854775808L}))
        self.assertRaises(OverflowError, BSON.from_dict, {"x": -9223372036854775809L})

    def test_tuple(self):
        self.assertEqual({"tuple": [1, 2]},
                          BSON.from_dict({"tuple": (1, 2)}).to_dict())

    def test_uuid(self):
        if not should_test_uuid:
            raise SkipTest()

        id = uuid.uuid4()
        transformed_id = (BSON.from_dict({"id": id})).to_dict()["id"]

        self.assert_(isinstance(transformed_id, uuid.UUID))
        self.assertEqual(id, transformed_id)
        self.assertNotEqual(uuid.uuid4(), transformed_id)

    # The C extension was segfaulting on unicode RegExs, so we have this test
    # that doesn't really test anything but the lack of a segfault.
    def test_unicode_regex(self):
        regex = re.compile(u'revisi\xf3n')
        BSON.from_dict({"regex": regex}).to_dict()

    def test_non_string_keys(self):
        self.assertRaises(InvalidDocument, BSON.from_dict, {8.9: "test"})

    def test_large_document(self):
        self.assertRaises(InvalidDocument, BSON.from_dict, {"key": "x"*4*1024*1024})

    def test_utf8(self):
        w = {u"aéあ": u"aéあ"}
        self.assertEqual(w, BSON.from_dict(w).to_dict())

        x = {u"aéあ".encode("utf-8"): u"aéあ".encode("utf-8")}
        self.assertEqual(w, BSON.from_dict(x).to_dict())

        y = {"hello": u"aé".encode("iso-8859-1")}
        self.assertRaises(InvalidStringData, BSON.from_dict, y)

        z = {u"aé".encode("iso-8859-1"): "hello"}
        self.assertRaises(InvalidStringData, BSON.from_dict, z)

    def test_null_character(self):
        doc = {"a": "\x00"}
        self.assertEqual(doc, BSON.from_dict(doc).to_dict())

        doc = {"a": u"\x00"}
        self.assertEqual(doc, BSON.from_dict(doc).to_dict())

        self.assertRaises(InvalidDocument, BSON.from_dict, {"\x00": "a"})
        self.assertRaises(InvalidDocument, BSON.from_dict, {u"\x00": "a"})

        self.assertRaises(InvalidDocument, BSON.from_dict, {"a": re.compile("ab\x00c")})
        self.assertRaises(InvalidDocument, BSON.from_dict, {"a": re.compile(u"ab\x00c")})

    def test_move_id(self):
        self.assertEqual("\x19\x00\x00\x00\x02_id\x00\x02\x00\x00\x00a\x00"
                         "\x02a\x00\x02\x00\x00\x00a\x00\x00",
                         BSON.from_dict(SON([("a", "a"), ("_id", "a")])))

        self.assertEqual("\x2c\x00\x00\x00"
                         "\x02_id\x00\x02\x00\x00\x00b\x00"
                         "\x03b\x00"
                         "\x19\x00\x00\x00\x02a\x00\x02\x00\x00\x00a\x00"
                         "\x02_id\x00\x02\x00\x00\x00a\x00\x00\x00",
                         BSON.from_dict(SON([("b", SON([("a", "a"), ("_id", "a")])),
                                             ("_id", "b")])))


    def test_dates(self):
        doc = {"early": datetime.datetime(1686, 5, 5),
               "late": datetime.datetime(2086, 5, 5)}
        try:
            self.assertEqual(doc, BSON.from_dict(doc).to_dict())
        except ValueError:
            # Ignore ValueError when no C ext, since it's probably
            # a problem w/ 32-bit Python - we work around this in the
            # C ext, though.
            if pymongo.has_c():
                raise

if __name__ == "__main__":
    unittest.main()

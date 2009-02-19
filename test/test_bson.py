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

"""Test the bson module."""

import unittest
import datetime
import re
import glob
import sys
import types

sys.path[0:0] = [""]

import qcheck
from pymongo.binary import Binary
from pymongo.code import Code
from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef
from pymongo.son import SON
from pymongo.bson import BSON, is_valid, _to_dicts
from pymongo.errors import UnsupportedTag

class TestBSON(unittest.TestCase):
    def setUp(self):
        pass

    def test_basic_validation(self):
        self.assertRaises(TypeError, is_valid, 100)
        self.assertRaises(TypeError, is_valid, u"test")
        self.assertRaises(TypeError, is_valid, 10.4)

        self.assertFalse(is_valid("test"))

        # the simplest valid BSON document
        self.assertTrue(is_valid("\x05\x00\x00\x00\x00"))
        self.assertTrue(is_valid(BSON("\x05\x00\x00\x00\x00")))
        self.assertFalse(is_valid("\x04\x00\x00\x00\x00"))
        self.assertFalse(is_valid("\x05\x00\x00\x00\x01"))
        self.assertFalse(is_valid("\x05\x00\x00\x00"))
        self.assertFalse(is_valid("\x05\x00\x00\x00\x00\x00"))

    def test_random_data_is_not_bson(self):
        qcheck.check_unittest(self, qcheck.isnt(is_valid), qcheck.gen_string(qcheck.gen_range(0, 40)))

    def test_basic_to_dict(self):
        self.assertEqual({"test": u"hello world"},
                         BSON("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00\x00").to_dict())
        self.assertEqual([{"test": u"hello world"}, {}],
                         _to_dicts("\x1B\x00\x00\x00\x0E\x74\x65\x73\x74\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00\x00\x05\x00\x00\x00\x00"))

    def test_basic_from_dict(self):
        self.assertRaises(TypeError, BSON.from_dict, 100)
        self.assertRaises(TypeError, BSON.from_dict, "hello")
        self.assertRaises(TypeError, BSON.from_dict, None)
        self.assertRaises(TypeError, BSON.from_dict, [])

        self.assertEqual(BSON.from_dict({}), BSON("\x05\x00\x00\x00\x00"))
        self.assertEqual(BSON.from_dict({"test": u"hello world"}),
                         "\x1B\x00\x00\x00\x02\x74\x65\x73\x74\x00\x0C\x00\x00\x00\x68\x65\x6C\x6C\x6F\x20\x77\x6F\x72\x6C\x64\x00\x00")
        self.assertEqual(BSON.from_dict({u"mike": 100}),
                         "\x0F\x00\x00\x00\x10\x6D\x69\x6B\x65\x00\x64\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"hello": 1.5}),
                         "\x14\x00\x00\x00\x01\x68\x65\x6C\x6C\x6F\x00\x00\x00\x00\x00\x00\x00\xF8\x3F\x00")
        self.assertEqual(BSON.from_dict({"true": True}),
                         "\x0C\x00\x00\x00\x08\x74\x72\x75\x65\x00\x01\x00")
        self.assertEqual(BSON.from_dict({"false": False}),
                         "\x0D\x00\x00\x00\x08\x66\x61\x6C\x73\x65\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"empty": []}),
                         "\x11\x00\x00\x00\x04\x65\x6D\x70\x74\x79\x00\x05\x00\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"none": {}}),
                         "\x10\x00\x00\x00\x03\x6E\x6F\x6E\x65\x00\x05\x00\x00\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"test": Binary("test")}),
                         "\x18\x00\x00\x00\x05\x74\x65\x73\x74\x00\x08\x00\x00\x00\x02\x04\x00\x00\x00\x74\x65\x73\x74\x00")
        self.assertEqual(BSON.from_dict({"test": Binary("test", 128)}),
                         "\x14\x00\x00\x00\x05\x74\x65\x73\x74\x00\x04\x00\x00\x00\x80\x74\x65\x73\x74\x00")
        self.assertEqual(BSON.from_dict({"test": None}),
                         "\x0B\x00\x00\x00\x0A\x74\x65\x73\x74\x00\x00")
        self.assertEqual(BSON.from_dict({"date": datetime.datetime(2007, 1, 8, 0, 30, 11)}),
                         "\x13\x00\x00\x00\x09\x64\x61\x74\x65\x00\x38\xBE\x1C\xFF\x0F\x01\x00\x00\x00")
        self.assertEqual(BSON.from_dict({"regex": re.compile("a*b", re.IGNORECASE)}),
                         "\x12\x00\x00\x00\x0B\x72\x65\x67\x65\x78\x00\x61\x2A\x62\x00\x69\x00\x00")
        self.assertEqual(BSON.from_dict({"$where": Code("test")}),
                         "\x16\x00\x00\x00\x0D\x24\x77\x68\x65\x72\x65\x00\x05\x00\x00\x00\x74\x65\x73\x74\x00\x00")
        a = ObjectId("\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B")
        self.assertEqual(BSON.from_dict({"oid": a}),
                         "\x16\x00\x00\x00\x07\x6F\x69\x64\x00\x07\x06\x05\x04\x03\x02\x01\x00\x0B\x0A\x09\x08\x00")
        self.assertEqual(BSON.from_dict({"ref": DBRef("coll", a)}),
                         "\x1F\x00\x00\x00\x0C\x72\x65\x66\x00\x05\x00\x00\x00\x63\x6F\x6C\x6C\x00\x07\x06\x05\x04\x03\x02\x01\x00\x0B\x0A\x09\x08\x00")

    def test_from_then_to_dict(self):
        def helper(dict):
            self.assertEqual(dict, (BSON.from_dict(dict)).to_dict())
        helper({})
        helper({"test": u"hello"})
        self.assertTrue(isinstance(BSON.from_dict({"hello": "world"}).to_dict()["hello"],
                                   types.UnicodeType))
        helper({"mike": -10120})
        helper({"long": long(10)})
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

        def from_then_to_dict(dict):
            return dict == (BSON.from_dict(dict)).to_dict()

        qcheck.check_unittest(self, from_then_to_dict, qcheck.gen_mongo_dict(3))

    def test_data_files(self):
        # TODO don't hardcode this, actually clone the repo
        data_files = "../mongo-qa/modules/bson_tests/tests/*/*.xson"
        generate = True

        for file_name in glob.iglob(data_files):
            f = open(file_name, "r")
            xml = f.read()
            f.close()

            try:
                doc = SON.from_xml(xml)
                bson = BSON.from_dict(doc)
            except UnsupportedTag:
                print "skipped file %s: %s" % (file_name, sys.exc_info()[1])
                continue

            try:
                f = open(file_name.replace(".xson", ".bson"), "r")
                expected = f.read()
                f.close()

                self.assertEqual(bson, expected, file_name)
                self.assertEqual(doc, bson.to_dict(), file_name)

            except IOError:
                if generate:
                    print "generating .bson for %s" % file_name

                    f = open(file_name.replace(".xson", ".bson"), "w")
                    f.write(bson)
                    f.close()

    def test_bad_encode(self):
        self.assertRaises(UnicodeDecodeError, BSON.from_dict,
                          {"lalala": '\xf4\xe0\xf0\xe1\xc0 Color Touch'})

if __name__ == "__main__":
    unittest.main()

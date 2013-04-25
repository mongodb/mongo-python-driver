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

"""Test some utilities for working with JSON and PyMongo."""

import unittest
import datetime
import re
import sys

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

import bson
from bson.py3compat import b
from bson import json_util
from bson.binary import Binary, MD5_SUBTYPE, USER_DEFINED_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from bson.tz_util import utc

from test.test_client import get_client

PY3 = sys.version_info[0] == 3


class TestJsonUtil(unittest.TestCase):

    def setUp(self):
        if not json_util.json_lib:
            raise SkipTest("No json or simplejson module")

        self.db = get_client().pymongo_test

    def round_tripped(self, doc):
        return json_util.loads(json_util.dumps(doc))

    def round_trip(self, doc):
        self.assertEqual(doc, self.round_tripped(doc))

    def test_basic(self):
        self.round_trip({"hello": "world"})

    def test_objectid(self):
        self.round_trip({"id": ObjectId()})

    def test_dbref(self):
        self.round_trip({"ref": DBRef("foo", 5)})
        self.round_trip({"ref": DBRef("foo", 5, "db")})
        self.round_trip({"ref": DBRef("foo", ObjectId())})
        self.round_trip({"ref": DBRef("foo", ObjectId(), "db")})

    def test_datetime(self):
        # only millis, not micros
        self.round_trip({"date": datetime.datetime(2009, 12, 9, 15,
                                                   49, 45, 191000, utc)})

    def test_regex(self):
        res = self.round_tripped({"r": re.compile("a*b", re.IGNORECASE)})["r"]
        self.assertEqual("a*b", res.pattern)
        if PY3:
            # re.UNICODE is a default in python 3.
            self.assertEqual(re.IGNORECASE | re.UNICODE, res.flags)
        else:
            self.assertEqual(re.IGNORECASE, res.flags)

        all_options = re.I|re.L|re.M|re.S|re.U|re.X
        regex = re.compile("a*b", all_options)
        res = self.round_tripped({"r": regex})["r"]
        self.assertEqual(all_options, res.flags)

        # Some tools may not add $options if no flags are set.
        res = json_util.loads('{"r": {"$regex": "a*b"}}')['r']
        expected_flags = 0
        if PY3:
            expected_flags = re.U
        self.assertEqual(expected_flags, res.flags)

    def test_minkey(self):
        self.round_trip({"m": MinKey()})

    def test_maxkey(self):
        self.round_trip({"m": MaxKey()})

    def test_timestamp(self):
        res = json_util.json.dumps({"ts": Timestamp(4, 13)},
                                     default=json_util.default)
        dct = json_util.json.loads(res)
        self.assertEqual(dct['ts']['t'], 4)
        self.assertEqual(dct['ts']['i'], 13)

    def test_uuid(self):
        if not bson.has_uuid():
            raise SkipTest("No uuid module")
        self.round_trip(
                {'uuid': bson.uuid.UUID(
                            'f47ac10b-58cc-4372-a567-0e02b2c3d479')})

    def test_binary(self):
        bin_type_dict = {"bin": Binary(b("\x00\x01\x02\x03\x04"))}
        md5_type_dict = {
            "md5": Binary(b(' n7\x18\xaf\t/\xd1\xd1/\x80\xca\xe7q\xcc\xac'),
                MD5_SUBTYPE)}
        custom_type_dict = {"custom": Binary(b("hello"), USER_DEFINED_SUBTYPE)}

        self.round_trip(bin_type_dict)
        self.round_trip(md5_type_dict)
        self.round_trip(custom_type_dict)

        # PYTHON-443 ensure old type formats are supported
        json_bin_dump = json_util.dumps(bin_type_dict)
        self.assertTrue('"$type": "00"' in json_bin_dump)
        self.assertEqual(bin_type_dict,
            json_util.loads('{"bin": {"$type": 0, "$binary": "AAECAwQ="}}'))

        json_bin_dump = json_util.dumps(md5_type_dict)
        self.assertTrue('"$type": "05"' in json_bin_dump)
        self.assertEqual(md5_type_dict,
            json_util.loads('{"md5": {"$type": 5, "$binary":'
                            ' "IG43GK8JL9HRL4DK53HMrA=="}}'))

        json_bin_dump = json_util.dumps(custom_type_dict)
        self.assertTrue('"$type": "80"' in json_bin_dump)
        self.assertEqual(custom_type_dict,
            json_util.loads('{"custom": {"$type": 128, "$binary":'
                            ' "aGVsbG8="}}'))

        # Handle mongoexport where subtype >= 128
        self.assertEqual(128,
            json_util.loads('{"custom": {"$type": "ffffff80", "$binary":'
                            ' "aGVsbG8="}}')['custom'].subtype)

        self.assertEqual(255,
            json_util.loads('{"custom": {"$type": "ffffffff", "$binary":'
                            ' "aGVsbG8="}}')['custom'].subtype)

    def test_code(self):
        self.round_trip({"code": Code("function x() { return 1; }")})
        self.round_trip({"code": Code("function y() { return z; }", z=2)})

    def test_cursor(self):
        db = self.db

        db.drop_collection("test")
        docs = [
            {'foo': [1, 2]},
            {'bar': {'hello': 'world'}},
            {'code': Code("function x() { return 1; }")},
            {'bin': Binary(b("\x00\x01\x02\x03\x04"))},
            {'dbref': {'_ref': DBRef('simple',
                               ObjectId('509b8db456c02c5ab7e63c34'))}}
        ]

        db.test.insert(docs)
        reloaded_docs = json_util.loads(json_util.dumps(db.test.find()))
        for doc in docs:
            self.assertTrue(doc in reloaded_docs)

if __name__ == "__main__":
    unittest.main()

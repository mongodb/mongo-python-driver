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
from bson.binary import Binary, MD5_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from bson.tz_util import utc

from test.test_connection import get_connection

PY3 = sys.version_info[0] == 3


class TestJsonUtil(unittest.TestCase):

    def setUp(self):
        if not json_util.json_lib:
            raise SkipTest("No json or simplejson module")

        self.db = get_connection().pymongo_test

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

        # TODO this is broken when using cjson. See:
        #   http://jira.mongodb.org/browse/PYTHON-153
        #   http://bugs.python.org/issue6105
        #
        # self.assertEqual("{\"ref\": {\"$ref\": \"foo\", \"$id\": 5}}",
        #                  json.dumps({"ref": DBRef("foo", 5)},
        #                  default=json_util.default))
        # self.assertEqual("{\"ref\": {\"$ref\": \"foo\",
        #                              \"$id\": 5, \"$db\": \"bar\"}}",
        #                  json.dumps({"ref": DBRef("foo", 5, "bar")},
        #                  default=json_util.default))

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
        self.round_trip({"bin": Binary(b("\x00\x01\x02\x03\x04"))})
        self.round_trip({
            "md5": Binary(b(' n7\x18\xaf\t/\xd1\xd1/\x80\xca\xe7q\xcc\xac'),
                MD5_SUBTYPE)})

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
            {'bin': Binary(b("\x00\x01\x02\x03\x04"))}
        ]

        db.test.insert(docs)
        reloaded_docs = json_util.loads(json_util.dumps(db.test.find()))
        for doc in docs:
            self.assertTrue(doc in reloaded_docs)

if __name__ == "__main__":
    unittest.main()

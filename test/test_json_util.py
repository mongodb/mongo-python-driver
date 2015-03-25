# Copyright 2009-2015 MongoDB, Inc.
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

import datetime
import json
import re
import sys
import uuid

sys.path[0:0] = [""]

from bson import json_util, EPOCH_AWARE
from bson.binary import Binary, MD5_SUBTYPE, USER_DEFINED_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.regex import Regex
from bson.timestamp import Timestamp
from bson.tz_util import utc

from test import unittest, IntegrationTest

PY3 = sys.version_info[0] == 3


class TestJsonUtil(unittest.TestCase):
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

        # Check order.
        self.assertEqual(
            '{"$ref": "collection", "$id": 1, "$db": "db"}',
            json_util.dumps(DBRef('collection', 1, 'db')))

    def test_datetime(self):
        # only millis, not micros
        self.round_trip({"date": datetime.datetime(2009, 12, 9, 15,
                                                   49, 45, 191000, utc)})

        jsn = '{"dt": { "$date" : "1970-01-01T00:00:00.000+0000"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        jsn = '{"dt": { "$date" : "1970-01-01T00:00:00.000+00:00"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        jsn = '{"dt": { "$date" : "1970-01-01T00:00:00.000Z"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        # No explicit offset
        jsn = '{"dt": { "$date" : "1970-01-01T00:00:00.000"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        # Localtime behind UTC
        jsn = '{"dt": { "$date" : "1969-12-31T16:00:00.000-0800"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        jsn = '{"dt": { "$date" : "1969-12-31T16:00:00.000-08:00"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        # Localtime ahead of UTC
        jsn = '{"dt": { "$date" : "1970-01-01T01:00:00.000+0100"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])
        jsn = '{"dt": { "$date" : "1970-01-01T01:00:00.000+01:00"}}'
        self.assertEqual(EPOCH_AWARE, json_util.loads(jsn)["dt"])

        dtm = datetime.datetime(1, 1, 1, 1, 1, 1, 0, utc)
        jsn = '{"dt": {"$date": -62135593139000}}'
        self.assertEqual(dtm, json_util.loads(jsn)["dt"])
        jsn = '{"dt": {"$date": {"$numberLong": "-62135593139000"}}}'
        self.assertEqual(dtm, json_util.loads(jsn)["dt"])

    def test_regex_object_hook(self):
        # Extended JSON format regular expression.
        pat = 'a*b'
        json_re = '{"$regex": "%s", "$options": "u"}' % pat
        loaded = json_util.object_hook(json.loads(json_re))
        self.assertTrue(isinstance(loaded, Regex))
        self.assertEqual(pat, loaded.pattern)
        self.assertEqual(re.U, loaded.flags)

    def test_regex(self):
        for regex_instance in (
                re.compile("a*b", re.IGNORECASE),
                Regex("a*b", re.IGNORECASE)):
            res = self.round_tripped({"r": regex_instance})["r"]

            self.assertEqual("a*b", res.pattern)
            res = self.round_tripped({"r": Regex("a*b", re.IGNORECASE)})["r"]
            self.assertEqual("a*b", res.pattern)
            self.assertEqual(re.IGNORECASE, res.flags)

        all_options = re.I|re.L|re.M|re.S|re.U|re.X
        regex = re.compile("a*b", all_options)
        res = self.round_tripped({"r": regex})["r"]
        self.assertEqual(all_options, res.flags)

        # Some tools may not add $options if no flags are set.
        res = json_util.loads('{"r": {"$regex": "a*b"}}')['r']
        self.assertEqual(0, res.flags)

        self.assertEqual(
            Regex('.*', 'ilm'),
            json_util.loads(
                '{"r": {"$regex": ".*", "$options": "ilm"}}')['r'])

        # Check order.
        self.assertEqual(
            '{"$regex": ".*", "$options": "mx"}',
            json_util.dumps(Regex('.*', re.M | re.X)))

        self.assertEqual(
            '{"$regex": ".*", "$options": "mx"}',
            json_util.dumps(re.compile(b'.*', re.M | re.X)))

    def test_minkey(self):
        self.round_trip({"m": MinKey()})

    def test_maxkey(self):
        self.round_trip({"m": MaxKey()})

    def test_timestamp(self):
        dct = {"ts": Timestamp(4, 13)}
        res = json_util.dumps(dct, default=json_util.default)
        self.assertEqual('{"ts": {"$timestamp": {"t": 4, "i": 13}}}', res)

        rtdct = json_util.loads(res)
        self.assertEqual(dct, rtdct)

    def test_uuid(self):
        self.round_trip(
                {'uuid': uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')})

    def test_binary(self):
        bin_type_dict = {"bin": Binary(b"\x00\x01\x02\x03\x04")}
        md5_type_dict = {
            "md5": Binary(b' n7\x18\xaf\t/\xd1\xd1/\x80\xca\xe7q\xcc\xac',
                MD5_SUBTYPE)}
        custom_type_dict = {"custom": Binary(b"hello", USER_DEFINED_SUBTYPE)}

        self.round_trip(bin_type_dict)
        self.round_trip(md5_type_dict)
        self.round_trip(custom_type_dict)

        # PYTHON-443 ensure old type formats are supported
        json_bin_dump = json_util.dumps(bin_type_dict)
        self.assertTrue('"$type": "00"' in json_bin_dump)
        self.assertEqual(bin_type_dict,
            json_util.loads('{"bin": {"$type": 0, "$binary": "AAECAwQ="}}'))

        json_bin_dump = json_util.dumps(md5_type_dict)
        # Check order.
        self.assertEqual(
            '{"md5": {"$binary": "IG43GK8JL9HRL4DK53HMrA==",'
            + ' "$type": "05"}}',
            json_bin_dump)

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

        code = Code("return z", z=2)
        res = json_util.dumps(code)
        self.assertEqual(code, json_util.loads(res))

        # Check order.
        self.assertEqual('{"$code": "return z", "$scope": {"z": 2}}', res)

    def test_undefined(self):
        json = '{"name": {"$undefined": true}}'
        self.assertIsNone(json_util.loads(json)['name'])

    def test_numberlong(self):
        json = '{"weight": {"$numberLong": 65535}}'
        self.assertEqual(json_util.loads(json)['weight'],
                         Int64(65535))


class TestJsonUtilRoundtrip(IntegrationTest):
    def test_cursor(self):
        db = self.db

        db.drop_collection("test")
        docs = [
            {'foo': [1, 2]},
            {'bar': {'hello': 'world'}},
            {'code': Code("function x() { return 1; }")},
            {'bin': Binary(b"\x00\x01\x02\x03\x04")},
            {'dbref': {'_ref': DBRef('simple',
                               ObjectId('509b8db456c02c5ab7e63c34'))}}
        ]

        db.test.insert_many(docs)
        reloaded_docs = json_util.loads(json_util.dumps(db.test.find()))
        for doc in docs:
            self.assertTrue(doc in reloaded_docs)

if __name__ == "__main__":
    unittest.main()

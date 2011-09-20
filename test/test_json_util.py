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

"""Test some utilities for working with JSON and PyMongo."""

import unittest
import datetime
import re
import sys

try:
    import uuid
    should_test_uuid = True
except ImportError:
    should_test_uuid = False

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from bson.objectid import ObjectId
from bson.binary import Binary, MD5_SUBTYPE
from bson.code import Code
from bson.dbref import DBRef
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.timestamp import Timestamp
from bson.tz_util import utc
from bson.json_util import JSONEncoder, JSONDecoder, json


class TestJsonUtil(unittest.TestCase):

    def round_tripped(self, doc):
        return json.loads(json.dumps(doc, cls=JSONEncoder),
                          cls=JSONDecoder)

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
        #                  default=default))
        # self.assertEqual("{\"ref\": {\"$ref\": \"foo\",
        #                              \"$id\": 5, \"$db\": \"bar\"}}",
        #                  json.dumps({"ref": DBRef("foo", 5, "bar")},
        #                  default=default))

    def test_datetime(self):
        # only millis, not micros
        self.round_trip({"date": datetime.datetime(2009, 12, 9, 15,
                                                   49, 45, 191000, utc)})

    def test_regex(self):
        res = self.round_tripped({"r": re.compile("a*b", re.IGNORECASE)})["r"]
        self.assertEqual("a*b", res.pattern)
        self.assertEqual(re.IGNORECASE, res.flags)

    def test_minkey(self):
        self.round_trip({"m": MinKey()})

    def test_maxkey(self):
        self.round_trip({"m": MinKey()})

    def test_timestamp(self):
        res = json.dumps({"ts": Timestamp(4, 13)}, cls=JSONEncoder)
        dct = json.loads(res)
        self.assertEqual(dct['ts']['t'], 4)
        self.assertEqual(dct['ts']['i'], 13)

    def test_binary(self):
        self.round_trip({"bin": Binary("\x00\x01\x02\x03\x04")})
        self.round_trip({
            "md5": Binary(' n7\x18\xaf\t/\xd1\xd1/\x80\xca\xe7q\xcc\xac',
                MD5_SUBTYPE)})

    def test_code(self):
        self.round_trip({"code": Code("function x() { return 1; }")})
        self.round_trip({"code": Code("function y() { return z; }", z=2)})

    def test_uuid(self):
        if not should_test_uuid:
            raise SkipTest()
        self.round_trip(
                {'uuid': uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')})

if __name__ == "__main__":
    unittest.main()

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
json_lib = True
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        json_lib = False
try:
    import uuid
    should_test_uuid = True
except ImportError:
    should_test_uuid = False

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from bson.dbref import DBRef
from bson.json_util import default, object_hook
from bson.min_key import MinKey
from bson.max_key import MaxKey
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from bson.tz_util import utc

PY3 = sys.version_info[0] == 3


class TestJsonUtil(unittest.TestCase):

    def setUp(self):
        if not json_lib:
            raise SkipTest()

    def round_tripped(self, doc):
        return json.loads(json.dumps(doc, default=default),
                          object_hook=object_hook)

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
        if PY3:
            # re.UNICODE is a default in python 3.
            self.assertEqual(re.IGNORECASE|re.UNICODE, res.flags)
        else:
            self.assertEqual(re.IGNORECASE, res.flags)

    def test_minkey(self):
        self.round_trip({"m": MinKey()})

    def test_maxkey(self):
        self.round_trip({"m": MinKey()})

    def test_timestamp(self):
        res = json.dumps({"ts": Timestamp(4, 13)}, default=default)
        dct = json.loads(res)
        self.assertEqual(dct['ts']['t'], 4)
        self.assertEqual(dct['ts']['i'], 13)

    def test_uuid(self):
        if not should_test_uuid:
            raise SkipTest()
        self.round_trip(
                {'uuid': uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')})

if __name__ == "__main__":
    unittest.main()

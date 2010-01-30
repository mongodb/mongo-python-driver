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
json_lib = True
try:
    import json
except ImportError:
    try:
        import simplejson as json
    except ImportError:
        json_lib = False

from nose.plugins.skip import SkipTest

sys.path[0:0] = [""]

from pymongo.json_util import default, object_hook
from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef

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

    def test_datetime(self):
        # only millis, not micros
        self.round_trip({"date": datetime.datetime(2009, 12, 9, 15,
                                                   49, 45, 191000)})

    def test_regex(self):
        res = self.round_tripped({"r": re.compile("a*b", re.IGNORECASE)})["r"]
        self.assertEqual("a*b", res.pattern)
        self.assertEqual(re.IGNORECASE, res.flags)


if __name__ == "__main__":
    unittest.main()

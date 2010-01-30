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

"""Tests for the son module."""

import unittest
import sys
sys.path[0:0] = [""]

from pymongo.son import SON


class TestSON(unittest.TestCase):

    def setUp(self):
        pass

    def test_ordered_dict(self):
        a = SON()
        a["hello"] = "world"
        a["mike"] = "awesome"
        a["hello_"] = "mike"
        self.assertEqual(a.items(), [("hello", "world"),
                                     ("mike", "awesome"),
                                     ("hello_", "mike")])

        b = SON({"hello": "world"})
        self.assertEqual(b["hello"], "world")
        self.assertRaises(KeyError, lambda: b["goodbye"])

    def test_to_dict(self):
        a = SON()
        b = SON([("blah", SON())])
        c = SON([("blah", [SON()])])
        d = SON([("blah", {"foo": SON()})])
        self.assertEqual({}, a.to_dict())
        self.assertEqual({"blah": {}}, b.to_dict())
        self.assertEqual({"blah": [{}]}, c.to_dict())
        self.assertEqual({"blah": {"foo": {}}}, d.to_dict())
        self.assertEqual(dict, a.to_dict().__class__)
        self.assertEqual(dict, b.to_dict()["blah"].__class__)
        self.assertEqual(dict, c.to_dict()["blah"][0].__class__)
        self.assertEqual(dict, d.to_dict()["blah"]["foo"].__class__)


if __name__ == "__main__":
    unittest.main()

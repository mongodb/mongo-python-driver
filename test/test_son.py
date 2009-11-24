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

"""Tests for the son module."""

import unittest
import datetime
import re
import sys
sys.path[0:0] = [""]

from pymongo.objectid import ObjectId
from pymongo.dbref import DBRef
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

    def test_from_xml(self):
        smorgasbord = """
<twonk>
  <meta/>
  <doc>
    <oid name="_id">285a664923b5fcd8ec000000</oid>
    <int name="the_answer">42</int>
    <string name="b">foo</string>
    <boolean name="c">true</boolean>
    <number name="pi">3.14159265358979</number>
    <array name="an_array">
      <string name="0">x</string>
      <string name="1">y</string>
      <string name="2">z</string>
      <doc name="3">
        <string name="subobject">yup</string>
      </doc>
    </array>
    <date name="now">123144452057</date>
    <ref name="dbref">
      <ns>namespace</ns>
      <oid>ca5c67496c01d896f7010000</oid>
    </ref>
    <code name="$where">this is code</code>
    <null name="mynull"/>
  </doc>
</twonk>
"""
        self.assertEqual(SON.from_xml(smorgasbord),
                         SON([(u"_id", ObjectId("\x28\x5A\x66\x49\x23\xB5\xFC"
                                                "\xD8\xEC\x00\x00\x00")),
                              (u"the_answer", 42),
                              (u"b", u"foo"),
                              (u"c", True),
                              (u"pi", 3.14159265358979),
                              (u"an_array", [u"x", u"y", u"z",
                                             SON([(u"subobject", u"yup")])]),
                              (u"now", datetime.datetime(1973, 11, 26, 6,
                                                         47, 32, 57000)),
                              (u"dbref",
                               DBRef("namespace",
                                     ObjectId("\xCA\x5C\x67\x49\x6C\x01"
                                              "\xD8\x96\xF7\x01\x00\x00"))),
                              (u"$where", "this is code"),
                              (u"mynull", None),
                              ]))

if __name__ == "__main__":
    unittest.main()

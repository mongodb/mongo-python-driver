"""Tests for the son module."""

import unittest
import datetime
import re

from objectid import ObjectId
from dbref import DBRef
from son import SON

class TestSON(unittest.TestCase):
    def setUp(self):
        pass

    def test_ordered_dict(self):
        a = SON()
        a["hello"] = "world"
        a["mike"] = "awesome"
        a["hello_"] = "mike"
        self.assertEqual(a.items(), [("hello", "world"), ("mike", "awesome"), ("hello_", "mike")])

        b = SON({"hello": "world"})
        self.assertEqual(b["hello"], "world")
        self.assertRaises(KeyError, lambda: b["goodbye"])

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
    <regex name="regex">
      <pattern>foobar</pattern>
      <options>i</options>
    </regex>
    <code name="$where">this is code</code>
    <null name="mynull"/>
  </doc>
</twonk>
"""
        self.assertEqual(SON.from_xml(smorgasbord),
                         SON([(u"_id", ObjectId("\x28\x5A\x66\x49\x23\xB5\xFC\xD8\xEC\x00\x00\x00")),
                              (u"the_answer", 42),
                              (u"b", u"foo"),
                              (u"c", True),
                              (u"pi", 3.14159265358979),
                              (u"an_array", [u"x", u"y", u"z", SON([(u"subobject", u"yup")])]),
                              (u"now", datetime.datetime(1973, 11, 26, 6, 47, 32, 57000)),
                              (u"dbref",
                               DBRef("namespace",
                                     ObjectId("\xCA\x5C\x67\x49\x6C\x01\xD8\x96\xF7\x01\x00\x00"))),
                              (u"regex", re.compile(u"foobar", re.IGNORECASE)),
                              (u"$where", "this is code"),
                              (u"mynull", None),
                              ]))

if __name__ == "__main__":
    unittest.main()

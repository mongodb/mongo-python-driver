"""Tools for creating and manipulating SON, the Serialized Ocument Notation.

Regular dictionaries can be used instead of SON objects, but not when the order
of keys is important."""

import unittest
from UserDict import DictMixin
from xml.parsers import expat
from objectid import ObjectId
from dbref import DBRef
import datetime
import re
import binascii

class SON(DictMixin):
    """SON data.

    A subclass of dict that maintains ordering of keys and provides a few extra
    niceties for dealing with SON.
    """
    def __init__(self, data=None, **kwargs):
        self.__keys = []
        self.__data = {}
        if data is not None:
            if hasattr(data, 'items'):
                items = data.iteritems()
            else:
                items = list(data)
            for item in items:
                if len(item) != 2:
                    raise ValueError("sequence elements must have length 2")
                self.__keys.append(item[0])
                self.__data[item[0]] = item[1]
        if kwargs:
            self.__merge_keys(kwargs.iterkeys())
            self.update(kwargs)

    def __merge_keys(self, keys):
        self._keys.extend(keys)
        newkeys = {}
        self.__keys = [newkeys.setdefault(x, x) for x in self.__keys if x not in newkeys]

    def __repr__(self):
        result = []
        for key in self.__keys:
            result.append("(%r, %r)" % (key, self.__data[key]))
        return "SON([%s])" % ", ".join(result)

    def update(self, data):
        if data is not None:
            if hasattr(data, "iterkeys"):
                self.__merge_keys(data.iterkeys())
            else:
                self.__merge_keys(data.keys())
            self._data.update(data)

    def __getitem__(self, key):
        return self.__data[key]

    def __setitem__(self, key, value):
        if key not in self.__data:
            self.__keys.append(key)
        self.__data[key] = value

    def __delitem__(self, key):
        del self.__data[key]
        self.__keys.remove(key)

    def keys(self):
        return list(self.__keys)

    def copy(self):
        other = SON()
        other.__data = self.__data.copy()
        other.__keys = self.__keys[:]
        return other

    @classmethod
    def from_xml(cls, xml):
        """Create an instance of SON from an xml document.
        """
        parser = expat.ParserCreate()
        son_stack = []
        list_stack = []
        append_to = []
        info = {}
        extra = {}

        def pad(list, index):
            while index >= len(list):
                list.append(None)

        def set(key, value):
            if append_to[-1] == "array":
                index = int(key)
                pad(list_stack[-1], index)
                list_stack[-1][int(key)] = value
            else:
                son_stack[-1][key] = value

        def start_element(name, attributes):
            info["current"] = name
            if name == "doc":
                a = SON()
                if attributes.has_key("name"):
                    set(attributes["name"], a)
                son_stack.append(a)
                append_to.append("doc")
            elif name == "array":
                a = []
                set(attributes["name"], a)
                list_stack.append(a)
                append_to.append("array")
            elif name in ["int", "string", "boolean", "number", "date", "code", "regex"]:
                info["key"] = attributes["name"]
            elif name == "ref":
                info["key"] = attributes["name"]
                info["ref"] = True
            elif name == "oid":
                if not info.get("ref", False):
                    info["key"] = attributes["name"]
            elif name == "null":
                set(attributes["name"], None)

            if name in ["string", "code", "ns", "pattern", "options"]:
                info["empty"] = True

        def end_element(name):
            if name == "doc":
                append_to.pop()
                info["last"] = son_stack.pop()
            elif name == "array":
                append_to.pop()
                info["array"] = False
                list_stack.pop()
            elif name == "ref":
                set(info["key"], DBRef(extra["ns"], extra["oid"]))
                info["ref"] = False
            elif name == "regex":
                set(info["key"], re.compile(extra["pattern"], extra["options"]))

            if info.get("empty", False):
                if info["current"] == "string":
                    set(info["key"], u"")
                elif info["current"] == "code":
                    set(info["key"], "")
                elif info["current"] in ["ns", "pattern"]:
                    extra[info["current"]] = u""
                elif info["current"] == "options":
                    extra["options"] = 0
                info["empty"] = False

        def char_data(data):
            data = data.strip()
            if not data:
                return

            info["empty"] = False

            if info["current"] == "oid":
                oid = ObjectId(binascii.unhexlify(data))
                if info.get("ref", False):
                    extra["oid"] = oid
                else:
                    set(info["key"], oid)
            elif info["current"] == "int":
                set(info["key"], int(data))
            elif info["current"] == "string":
                set(info["key"], data)
            elif info["current"] == "code":
                set(info["key"], data.encode("utf-8"))
            elif info["current"] == "boolean":
                set(info["key"], data == "true")
            elif info["current"] == "number":
                set(info["key"], float(data))
            elif info["current"] == "date":
                set(info["key"], datetime.datetime.fromtimestamp(float(data) / 1000.0))
            elif info["current"] == "ns":
                extra["ns"] = data
            elif info["current"] == "pattern":
                extra["pattern"] = data
            elif info["current"] == "options":
                options = 0
                if "i" in data:
                    options |= re.IGNORECASE
                if "m" in data:
                    options |= re.MULTILINE
                extra["options"] = options

        parser.StartElementHandler = start_element
        parser.EndElementHandler = end_element
        parser.CharacterDataHandler = char_data

        parser.Parse(xml)
        return info["last"]

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
                              (u"now", datetime.datetime(1973, 11, 26, 1, 47, 32, 57000)),
                              (u"dbref",
                               DBRef("namespace",
                                     ObjectId("\xCA\x5C\x67\x49\x6C\x01\xD8\x96\xF7\x01\x00\x00"))),
                              (u"regex", re.compile(u"foobar", re.IGNORECASE)),
                              (u"$where", "this is code"),
                              (u"mynull", None),
                              ]))

if __name__ == "__main__":
    unittest.main()

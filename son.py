"""Tools for creating and manipulating SON, the Serialized Ocument Notation.

Regular dictionaries can be used instead of SON objects, but not when the order
of keys is important."""

import datetime
import re
import binascii
import base64
from UserDict import DictMixin

import ElementTree as ET
from objectid import ObjectId
from dbref import DBRef
from errors import UnsupportedTag

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
        self.__keys.extend(keys)
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
            self.__data.update(data)

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
        def pad(list, index):
            while index >= len(list):
                list.append(None)

        def make_array(array):
            doc = make_doc(array)
            array = []
            for (key, value) in doc.items():
                index = int(key)
                pad(array, index)
                array[index] = value
            return array

        def make_string(string):
            return string.text is not None and unicode(string.text) or u""

        def make_binary(binary):
            return binary.text is not None and base64.b64decode(binary.text) or ""

        def make_boolean(bool):
            return bool.text == "true"

        def make_date(date):
            return datetime.datetime.fromtimestamp(float(date.text) / 1000.0)

        def make_ref(dbref):
            return DBRef(make_elem(dbref[0]), make_elem(dbref[1]))

        def make_oid(oid):
            return ObjectId(binascii.unhexlify(oid.text))

        def make_int(data):
            return int(data.text)

        def make_null(null):
            return None

        def make_number(number):
            return float(number.text)

        def make_regex(regex):
            return re.compile(make_elem(regex[0]), make_elem(regex[1]))

        def make_options(data):
            options = 0
            if not data.text:
                return options
            if "i" in data.text:
                options |= re.IGNORECASE
            if "l" in data.text:
                options |= re.LOCALE
            if "m" in data.text:
                options |= re.MULTILINE
            if "s" in data.text:
                options |= re.DOTALL
            if "u" in data.text:
                options |= re.UNICODE
            if "x" in data.text:
                options |= re.VERBOSE
            return options

        def make_elem(elem):
            try:
                return {"array": make_array,
                        "doc": make_doc,
                        "string": make_string,
                        "binary": make_binary,
                        "boolean": make_boolean,
                        "code": make_string,
                        "date": make_date,
                        "ref": make_ref,
                        "ns": make_string,
                        "oid": make_oid,
                        "int": make_int,
                        "null": make_null,
                        "number": make_number,
                        "regex": make_regex,
                        "pattern": make_string,
                        "options": make_options,
                        }[elem.tag](elem)
            except KeyError:
                raise UnsupportedTag("cannot parse tag: %s" % elem.tag)

        def make_doc(doc):
            son = SON()
            for elem in doc:
                son[elem.attrib["name"]] = make_elem(elem)
            return son

        tree = ET.XML(xml)
        doc = tree[1]

        return make_doc(doc)

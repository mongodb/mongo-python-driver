"""Tools for creating and manipulating SON, the Serialized Ocument Notation.

Regular dictionaries can be used instead of SON objects, but not when the order
of keys is important."""

import unittest
from UserDict import DictMixin

class SON(DictMixin):
    """SON data.

    A subclass of dict that maintains ordering of keys and provides a few extra
    niceties.
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
                self.__keys.append(items[0])
                self.__data[items[0]] = items[1]
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
            result.append("(%s, %s)" % (repr(key), repr(self.__data[key])))
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

class TestSON(unittest.TestCase):
    def setUp(self):
        pass

    def test_ordered_dict(self):
        a = SON()
        a["hello"] = "world"
        a["mike"] = "awesome"
        a["hello_"] = "mike"
        self.assertEqual(a.items(), [("hello", "world"), ("mike", "awesome"), ("hello_", "mike")])

if __name__ == "__main__":
    unittest.main()

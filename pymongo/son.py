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

"""Tools for creating and manipulating SON, the Serialized Ocument Notation.

Regular dictionaries can be used instead of SON objects, but not when the order
of keys is important. A SON object can be used just like a normal Python
dictionary."""


class SON(dict):
    """SON data.

    A subclass of dict that maintains ordering of keys and provides a few extra
    niceties for dealing with SON. SON objects can be saved and retrieved from
    Mongo.

    The mapping from Python types to Mongo types is as follows:

    ===================================  =============  ===================
    Python Type                          Mongo Type     Supported Direction
    ===================================  =============  ===================
    None                                 null           both
    bool                                 boolean        both
    int                                  number (int)   both
    float                                number (real)  both
    string                               string         py -> mongo
    unicode                              string         both
    list                                 array          both
    dict / `SON`                         object         both
    datetime.datetime [#dt]_ [#dt2]_     date           both
    compiled re                          regex          both
    `pymongo.binary.Binary`              binary         both
    `pymongo.objectid.ObjectId`          oid            both
    `pymongo.dbref.DBRef`                dbref          both
    None                                 undefined      mongo -> py
    unicode                              code           mongo -> py
    `pymongo.code.Code`                  code           py -> mongo
    unicode                              symbol         mongo -> py
    ===================================  =============  ===================

    Note that to save binary data it must be wrapped as an instance of
    `pymongo.binary.Binary`. Otherwise it will be saved as a Mongo string and
    retrieved as unicode.

    .. [#dt] datetime.datetime instances will be rounded to the nearest
       millisecond when saved
    .. [#dt2] all datetime.datetime instances are treated as *naive*. clients
       should always use UTC.
    """

    def __init__(self, data=None, **kwargs):
        self.__keys = []
        dict.__init__(self)
        self.update(data)
        self.update(kwargs)

    def __repr__(self):
        result = []
        for key in self.__keys:
            result.append("(%r, %r)" % (key, self[key]))
        return "SON([%s])" % ", ".join(result)

    def __setitem__(self, key, value):
        if key not in self:
            self.__keys.append(key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        self.__keys.remove(key)
        dict.__delitem__(self, key)

    def keys(self):
        return list(self.__keys)

    def copy(self):
        other = SON()
        other.update(self)
        return other

    # TODO this is all from UserDict.DictMixin. it could probably be made more
    # efficient.
    # second level definitions support higher levels
    def __iter__(self):
        for k in self.keys():
            yield k

    def has_key(self, key):
        return key in self.keys()

    def __contains__(self, key):
        return key in self.keys()

    # third level takes advantage of second level definitions
    def iteritems(self):
        for k in self:
            yield (k, self[k])

    def iterkeys(self):
        return self.__iter__()

    # fourth level uses definitions from lower levels
    def itervalues(self):
        for _, v in self.iteritems():
            yield v

    def values(self):
        return [v for _, v in self.iteritems()]

    def items(self):
        return list(self.iteritems())

    def clear(self):
        for key in self.keys():
            del self[key]

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
        return default

    def pop(self, key, *args):
        if len(args) > 1:
            raise TypeError("pop expected at most 2 arguments, got "\
                                + repr(1 + len(args)))
        try:
            value = self[key]
        except KeyError:
            if args:
                return args[0]
            raise
        del self[key]
        return value

    def popitem(self):
        try:
            k, v = self.iteritems().next()
        except StopIteration:
            raise KeyError('container is empty')
        del self[k]
        return (k, v)

    def update(self, other=None, **kwargs):
        # Make progressively weaker assumptions about "other"
        if other is None:
            pass
        elif hasattr(other, 'iteritems'):  # iteritems saves memory and lookups
            for k, v in other.iteritems():
                self[k] = v
        elif hasattr(other, 'keys'):
            for k in other.keys():
                self[k] = other[k]
        else:
            for k, v in other:
                self[k] = v
        if kwargs:
            self.update(kwargs)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __cmp__(self, other):
        if isinstance(other, SON):
            return cmp((dict(self.iteritems()), self.keys()),
                       (dict(other.iteritems()), other.keys()))
        return cmp(dict(self.iteritems()), other)

    def __len__(self):
        return len(self.keys())

    def to_dict(self):
        """Convert a SON document to a normal Python dictionary instance.

        This is trickier than just *dict(...)* because it needs to be
        recursive.
        """

        def transform_value(value):
            if isinstance(value, list):
                return [transform_value(v) for v in value]
            if isinstance(value, SON):
                value = dict(value)
            if isinstance(value, dict):
                for k, v in value.iteritems():
                    value[k] = transform_value(v)
            return value

        return transform_value(dict(self))

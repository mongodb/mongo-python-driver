# Copyright 2009-2015 MongoDB, Inc.
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

"""Manipulators that can edit SON objects as they enter and exit a database.

New manipulators should be defined as subclasses of SONManipulator and can be
installed on a database by calling
`pymongo.database.Database.add_son_manipulator`."""

import collections
from abc import ABCMeta, abstractmethod as abstract_method
from codecs import decode
from collections import Mapping as MappingType
from hashlib import md5
from sys import getdefaultencoding as get_default_encoding

from six import binary_type, iteritems, text_type, with_metaclass

from bson.dbref import DBRef
from bson.errors import InvalidDocument
from bson.objectid import ObjectId
from bson.son import SON


class SONManipulator(object):
    """A base son manipulator.

    This manipulator just saves and restores objects without changing them.
    """

    def will_copy(self):
        """Will this SON manipulator make a copy of the incoming document?

        Derived classes that do need to make a copy should override this
        method, returning True instead of False. All non-copying manipulators
        will be applied first (so that the user's document will be updated
        appropriately), followed by copying manipulators.
        """
        return False

    def transform_incoming(self, son, collection):
        """Manipulate an incoming SON object.

        :Parameters:
          - `son`: the SON object to be inserted into the database
          - `collection`: the collection the object is being inserted into
        """
        if self.will_copy():
            return SON(son)
        return son

    def transform_outgoing(self, son, collection):
        """Manipulate an outgoing SON object.

        :Parameters:
          - `son`: the SON object being retrieved from the database
          - `collection`: the collection this object was stored in
        """
        if self.will_copy():
            return SON(son)
        return son


class ObjectIdInjector(SONManipulator):
    """A son manipulator that adds the _id field if it is missing.

    .. versionchanged:: 2.7
       ObjectIdInjector is no longer used by PyMongo, but remains in this
       module for backwards compatibility.
    """

    def transform_incoming(self, son, collection):
        """Add an _id field if it is missing.
        """
        if not "_id" in son:
            son["_id"] = ObjectId()
        return son


# This is now handled during BSON encoding (for performance reasons),
# but I'm keeping this here as a reference for those implementing new
# SONManipulators.
class ObjectIdShuffler(SONManipulator):
    """A son manipulator that moves _id to the first position.
    """

    def will_copy(self):
        """We need to copy to be sure that we are dealing with SON, not a dict.
        """
        return True

    def transform_incoming(self, son, collection):
        """Move _id to the front if it's there.
        """
        if not "_id" in son:
            return son
        transformed = SON({"_id": son["_id"]})
        transformed.update(son)
        return transformed


class NamespaceInjector(SONManipulator):
    """A son manipulator that adds the _ns field.
    """

    def transform_incoming(self, son, collection):
        """Add the _ns field to the incoming object
        """
        son["_ns"] = collection.name
        return son


class AutoReference(SONManipulator):
    """Transparently reference and de-reference already saved embedded objects.

    This manipulator should probably only be used when the NamespaceInjector is
    also being used, otherwise it doesn't make too much sense - documents can
    only be auto-referenced if they have an *_ns* field.

    NOTE: this will behave poorly if you have a circular reference.

    TODO: this only works for documents that are in the same database. To fix
    this we'll need to add a DatabaseInjector that adds *_db* and then make
    use of the optional *database* support for DBRefs.
    """

    def __init__(self, db):
        self.database = db

    def will_copy(self):
        """We need to copy so the user's document doesn't get transformed refs.
        """
        return True

    def transform_incoming(self, son, collection):
        """Replace embedded documents with DBRefs.
        """

        def transform_value(value):
            if isinstance(value, collections.MutableMapping):
                if "_id" in value and "_ns" in value:
                    return DBRef(value["_ns"], transform_value(value["_id"]))
                else:
                    return transform_dict(SON(value))
            elif isinstance(value, list):
                return [transform_value(v) for v in value]
            return value

        def transform_dict(object):
            for (key, value) in object.items():
                object[key] = transform_value(value)
            return object

        return transform_dict(SON(son))

    def transform_outgoing(self, son, collection):
        """Replace DBRefs with embedded documents.
        """

        def transform_value(value):
            if isinstance(value, DBRef):
                return self.database.dereference(value)
            elif isinstance(value, list):
                return [transform_value(v) for v in value]
            elif isinstance(value, collections.MutableMapping):
                return transform_dict(SON(value))
            return value

        def transform_dict(object):
            for (key, value) in object.items():
                object[key] = transform_value(value)
            return object

        return transform_dict(SON(son))


class BaseKeyEscaper(with_metaclass(ABCMeta, SONManipulator)):
    """
    Escapes illegal keys, ensuring that the original values can be
        recovered later.

    Note that the escaped keys will be virtually impossible to query
        for, but that's infinitely preferable to MongoDB refusing to
        store the document in the first place.
    """
    magic_prefix = '__escaped__'
    """Used to identify escaped keys."""

    def __init__(self):
        super(BaseKeyEscaper, self).__init__()

        ##
        # These attributes are only used when escaping keys.
        ##

        self.current_path = None
        """
        Keeps track of where we are in the document so that we can
            populate the `__escapedKeys` dict correctly.
        """

        self.escaped_keys = None
        """
        Keeps track of any keys that we've escaped so that a
            KeyEscaper can later unescape them.
        """

    @abstract_method
    def escape_key(self, key):
        """Escapes a single key."""
        raise NotImplementedError(
            'Not implemented in {cls}.'.format(cls=type(self).__name__),
        )

    def will_copy(self):
        """Does this manipulator create a copy of the SON?"""
        #
        # `transform_incoming` does create a copy, but
        #   `transform_outgoing` does not.  Well, we have to pick one!
        #
        # We'll go with `False` because it is not safe to assume that
        #   this manipulator will create a copy of the SON.
        #
        return False

    def transform_incoming(self, son, collection):
        """
        Transforms a document before it is stored to the database.
        """
        self.current_path = []
        self.escaped_keys = {}

        transformed = self._escape(son)
        transformed[self.magic_prefix] = self.escaped_keys
        return transformed

    def transform_outgoing(self, son, collection):
        """
        Transforms a document after it is retrieved from the database.

        Note that this method will directly modify the document!
        """
        escaped_keys = son.pop(self.magic_prefix, None)
        return self._unescape(son, escaped_keys) if escaped_keys else son

    def _escape(self, son):
        """
        Recursively crawls the document, transforming keys as it goes.
        """
        copy = SON()

        for (key, value) in iteritems(son):
            # Python 2 compatibility:  Binary strings are allowed, so long as
            #   they can be converted to unicode strings.
            if isinstance(key, binary_type):
                # For consistent behavior, ensure same default encoding for
                #   Py2k and Py3k.
                encoding = get_default_encoding()
                if encoding == 'ascii':
                    encoding = 'utf-8'

                try:
                    key = decode(key, encoding)
                except UnicodeDecodeError:
                    pass

            if not isinstance(key, text_type):
                raise InvalidDocument(
                    'documents must have only string keys, '
                    'key was {path}[{actual!r}]'.format(
                        actual=key,
                        path='.'.join(self.current_path),
                    ),
                )

            if (
                        key.startswith('$')
                    or  key.startswith(self.magic_prefix)
                    or  ('.' in key)
                    or  ('\x00' in key)
            ):
                key = self._escape_key(key)

            if isinstance(value, MappingType):
                self.current_path.append(key)
                value = self._escape(value)
                self.current_path.pop()

            copy[key] = value

        return copy

    def _escape_key(self, key):
        """
        Transforms an illegal key into something that MongoDB will
            approve of.
        """
        new_key = self.escape_key(key)

        # Insert the escaped key into the correct location in
        #   self.escaped_keys so that it can be unescaped later.
        crawler = self.escaped_keys
        for x in self.current_path:
            crawler.setdefault(x, [None, {}])
            crawler = crawler[x][1]

        crawler[new_key] = [key, {}]

        return new_key

    def _unescape(self, son, escaped_keys):
        """
        Recursively crawls the document, restoring keys as it goes.
        """
        copy = SON()

        for key, value in iteritems(son):
            if key in escaped_keys:
                r_key, r_children = escaped_keys[key]

                if r_key is None:
                    r_key = key

                if r_children:
                    copy[r_key] = self._unescape(son[key], r_children)
                else:
                    copy[r_key] = son[key]
            else:
                copy[key] = value

        return copy


class NonDeterministicKeyEscaper(BaseKeyEscaper):
    """
    A KeyEscaper that uses an internal counter to generate escaped keys.

    This method is a bit faster and tends to yield smaller escaped keys
      than DeterministicKeyEscaper, but the result is more difficult to
      query.
    """

    def __init__(self):
        super(NonDeterministicKeyEscaper, self).__init__()

        self.escaped_key_count = None
        """Used to ensure each escaped key is unique."""

    def transform_incoming(self, son, collection):
        self.escaped_key_count = 0

        return \
            super(NonDeterministicKeyEscaper, self) \
                .transform_incoming(son, collection)

    def escape_key(self, key):
        escaped = self.magic_prefix + text_type(self.escaped_key_count)
        self.escaped_key_count += 1
        return escaped


class DeterministicKeyEscaper(BaseKeyEscaper):
    """
    A KeyEscaper that uses hashes to escape unsafe keys.

    This method is a little slower and tends to yield larger escaped keys
      than NonDeterministicKeyEscaper, but you can execute queries
      against the escaped keys more easily.
    """

    def escape_key(self, key):
        # Note:  In Python 3, hashlib requires a byte string.
        return self.magic_prefix + md5(key.encode('utf-8')).hexdigest()



# TODO make a generic translator for custom types. Take encode, decode,
# should_encode and should_decode functions and just encode and decode where
# necessary. See examples/custom_type.py for where this would be useful.
# Alternatively it could take a should_encode, to_binary, from_binary and
# binary subtype.

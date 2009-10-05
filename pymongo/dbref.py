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

"""Tools for manipulating DBRefs (references to Mongo objects)."""

import types


class DBRef(object):
    """A reference to an object stored in a Mongo database.
    """

    def __init__(self, collection, id):
        """Initialize a new DBRef.

        Raises TypeError if collection is not an instance of (str, unicode).

        :Parameters:
          - `collection`: the collection the object is stored in
          - `id`: the value of the object's _id field
        """
        if not isinstance(collection, types.StringTypes):
            raise TypeError("collection must be an instance of (str, unicode)")

        if isinstance(collection, types.StringType):
            collection = unicode(collection, "utf-8")

        self.__collection = collection
        self.__id = id

    def collection(self):
        """Get this DBRef's collection as unicode.
        """
        return self.__collection
    collection = property(collection)

    def id(self):
        """Get this DBRef's _id.
        """
        return self.__id
    id = property(id)

    def __repr__(self):
        return "DBRef(" + repr(self.collection) + ", " + repr(self.id) + ")"

    def __cmp__(self, other):
        if isinstance(other, DBRef):
            return cmp([self.__collection, self.__id],
                       [other.__collection, other.__id])
        return NotImplemented
        
    def __hash__(self):
        return hash((self.__collection, self.__id))

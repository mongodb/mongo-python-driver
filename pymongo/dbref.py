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

"""Tools for manipulating DBRefs (references to MongoDB documents)."""

import types

from son import SON


class DBRef(object):
    """A reference to a document stored in a Mongo database.
    """

    def __init__(self, collection, id, database=None):
        """Initialize a new DBRef.

        Raises TypeError if collection or database is not an instance of
        (str, unicode). `database` is optional and allows references to
        documents to work across databases.

        :Parameters:
          - `collection`: name of the collection the document is stored in
          - `id`: the value of the document's _id field
          - `database` (optional): name of the database to reference
        """
        if not isinstance(collection, types.StringTypes):
            raise TypeError("collection must be an instance of (str, unicode)")
        if not isinstance(database, (types.StringTypes, types.NoneType)):
            raise TypeError("database must be an instance of (str, unicode)")

        self.__collection = collection
        self.__id = id
        self.__database = database

    def collection(self):
        """Get the name of this DBRef's collection as unicode.
        """
        return self.__collection
    collection = property(collection)

    def id(self):
        """Get this DBRef's _id.
        """
        return self.__id
    id = property(id)

    def database(self):
        """Get the name of this DBRef's database.

        Returns None if this DBRef doesn't specify a database.
        """
        return self.__database
    database = property(database)

    def as_doc(self):
        """Get the SON document representation of this DBRef.

        Generally not needed by application developers
        """
        doc = SON([("$ref", self.collection),
                         ("$id", self.id)])
        if self.database is not None:
            doc["$db"] = self.database
        return doc

    def __repr__(self):
        if self.database is None:
            return "DBRef(%r, %r)" % (self.collection, self.id)
        return "DBRef(%r, %r, %r)" % (self.collection, self.id, self.database)

    def __cmp__(self, other):
        if isinstance(other, DBRef):
            return cmp([self.__database, self.__collection, self.__id],
                       [other.__database, other.__collection, other.__id])
        return NotImplemented

    def __hash__(self):
        return hash((self.__collection, self.__id, self.__database))

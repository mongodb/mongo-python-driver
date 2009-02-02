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

"""Manipulators that can edit SON objects as they enter and exit a database.

New manipulators should be defined as subclasses of SONManipulator and can be
installed on a database by calling `pymongo.database.Database.add_son_manipulator`."""

from objectid import ObjectId
from son import SON

class SONManipulator(object):
    """A base son manipulator.

    This manipulator just saves and restores objects without changing them.
    """
    def __init__(self, database):
        """Instantiate the manager.

        :Parameters:
          - `database`: a Mongo Database
        """
        self.__database = database

    def transform_incoming(self, son, collection):
        """Manipulate an incoming son object.

        :Parameters:
          - `son`: the son object to be inserted into the database
          - `collection`: the collection the object is being inserted into
        """
        return son

    def transform_outgoing(self, son, collection):
        """Manipulate an outgoing son object.

        :Parameters:
          - `son`: the son object being retrieved from the database
          - `collection`: the collection this object was stored in
        """
        return son

class ObjectIdInjector(SONManipulator):
    """A son manipulator that adds the _id field if it is missing.
    """
    def transform_incoming(self, son, collection):
        """Add an _id field if it is missing.
        """
        if not "_id" in son:
            son["_id"] = ObjectId()
        return son

class ObjectIdShuffler(SONManipulator):
    """A son manipulator that moves _id to the first position.
    """
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
        son["_ns"] = collection.name()
        return son

# TODO make a generic translator for custom types. Take encode, decode,
# should_encode and should_decode functions and just encode and decode where
# necessary. See examples/custom_type.py for where this would be useful.
# Alternatively it could take a should_encode, to_binary, from_binary and
# binary subtype.

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

    def will_copy(self):
        """Will this SON manipulator make a copy of the incoming document?

        Derived classes that do need to make a copy should override this method,
        returning True instead of False. All non-copying manipulators will be
        applied first (so that the user's document will be updated
        appropriately), followed by copying manipulators.
        """
        return False

    def transform_incoming(self, son, collection):
        """Manipulate an incoming SON object.

        :Parameters:
          - `son`: the SON object to be inserted into the database
          - `collection`: the collection the object is being inserted into
        """
        return son

    def transform_outgoing(self, son, collection):
        """Manipulate an outgoing SON object.

        :Parameters:
          - `son`: the SON object being retrieved from the database
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
        son["_ns"] = collection.name()
        return son

class DBRefTransformer(SONManipulator):
    """A son manipulator for handling DBRefs.

    Transforms a DBRef instance into the appropriate embedded object before a
    document is saved. Transforms embedded objects that are references to DBRef
    instances after a document is retrieved.

    See the MongoDB wiki_ for more information.

    .. _wiki: http://www.mongodb.org/display/DOCS/DB+Ref+Specification
    """
    def will_copy(self):
        """We need to copy so the user's document doesn't get transformed refs.
        """
        return True

    def transform_incoming(self, son, collection):
        """Replace DBRef instances with the appropriate embedded objects.
        """
        return son

    def transform_outgoing(self, son, collection):
        """Replace embedded DBRef objects with DBRef instances.
        """
        return son

# TODO make a generic translator for custom types. Take encode, decode,
# should_encode and should_decode functions and just encode and decode where
# necessary. See examples/custom_type.py for where this would be useful.
# Alternatively it could take a should_encode, to_binary, from_binary and
# binary subtype.

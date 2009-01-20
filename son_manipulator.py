"""Manipulators that can edit SON objects as the enter and exit a database.

New manipulators should be defined as subclasses of SONManipulator and can be
installed on a database by calling `Database.add_son_manipulator`."""

from objectid import ObjectId

class SONManipulator(object):
    """A base son manipulator.

    This manipulator just saves and restores objects without changing them.
    """
    def __init__(self, database):
        """Instantiate the manager.

        Arguments:
        - `database`: a Mongo Database
        """
        self.__database = database

    def transform_incoming(self, son, collection):
        """Manipulate an incoming son object.

        Arguments:
        - `son`: the son object to be inserted into the database
        - `collection`: the collection the object is being inserted into
        """
        return son

    def transform_outgoing(self, son, collection):
        """Manipulate an outgoing son object.

        Arguments:
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
        if "_id" in son:
            assert isinstance(son["_id"], ObjectId), "'_id' must be an ObjectId"
        else:
            son["_id"] = ObjectId()
        return son

class NamespaceInjector(SONManipulator):
    """A son manipulator that adds the _ns field.
    """
    def transform_incoming(self, son, collection):
        """Add the _ns field to the incoming object
        """
        son["_ns"] = collection.name()
        return son

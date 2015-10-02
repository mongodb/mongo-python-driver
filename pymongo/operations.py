# Copyright 2015 MongoDB, Inc.
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

"""Operation class definitions."""

from pymongo.common import validate_boolean, validate_is_mapping
from pymongo.helpers import _gen_index_name, _index_document, _index_list


class _WriteOp(object):
    """Private base class for all write operations."""

    __slots__ = ("_filter", "_doc", "_upsert")

    def __init__(self, filter=None, doc=None, upsert=None):
        if filter is not None:
            validate_is_mapping("filter", filter)
        if upsert is not None:
            validate_boolean("upsert", upsert)
        self._filter = filter
        self._doc = doc
        self._upsert = upsert

    def __eq__(self, other):
        if type(other) == type(self):
            return (other._filter, other._doc, other._upsert) == \
                   (self._filter, self._doc, self._upsert)
        return NotImplemented

    def __ne__(self, other):
        return not self == other


class InsertOne(_WriteOp):
    """Represents an insert_one operation."""

    def __init__(self, document):
        """Create an InsertOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `document`: The document to insert. If the document is missing an
            _id field one will be added.
        """
        super(InsertOne, self).__init__(doc=document)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_insert(self._doc)

    def __repr__(self):
        return "InsertOne(%r)" % (self._doc,)


class DeleteOne(_WriteOp):
    """Represents a delete_one operation."""

    def __init__(self, filter):
        """Create a DeleteOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to delete.
        """
        super(DeleteOne, self).__init__(filter)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_delete(self._filter, 1)

    def __repr__(self):
        return "DeleteOne(%r)" % (self._filter,)


class DeleteMany(_WriteOp):
    """Represents a delete_many operation."""

    def __init__(self, filter):
        """Create a DeleteMany instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the documents to delete.
        """
        super(DeleteMany, self).__init__(filter)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_delete(self._filter, 0)

    def __repr__(self):
        return "DeleteMany(%r)" % (self._filter,)


class ReplaceOne(_WriteOp):
    """Represents a replace_one operation."""

    def __init__(self, filter, replacement, upsert=False):
        """Create a ReplaceOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
        """
        super(ReplaceOne, self).__init__(filter, replacement, upsert)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_replace(self._filter, self._doc, self._upsert)

    def __repr__(self):
        return "ReplaceOne(%r, %r, %r)" % (self._filter,
                                           self._doc,
                                           self._upsert)


class UpdateOne(_WriteOp):
    """Represents an update_one operation."""

    def __init__(self, filter, update, upsert=False):
        """Represents an update_one operation.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
        """
        super(UpdateOne, self).__init__(filter, update, upsert)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_update(self._filter, self._doc, False, self._upsert)

    def __repr__(self):
        return "UpdateOne(%r, %r, %r)" % (self._filter,
                                          self._doc,
                                          self._upsert)


class UpdateMany(_WriteOp):
    """Represents an update_many operation."""

    def __init__(self, filter, update, upsert=False):
        """Create an UpdateMany instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
        """
        super(UpdateMany, self).__init__(filter, update, upsert)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_update(self._filter, self._doc, True, self._upsert)

    def __repr__(self):
        return "UpdateMany(%r, %r, %r)" % (self._filter,
                                           self._doc,
                                           self._upsert)


class IndexModel(object):
    """Represents an index to create."""

    __slots__ = ("__document",)

    def __init__(self, keys, **kwargs):
        """Create an Index instance.

        For use with :meth:`~pymongo.collection.Collection.create_indexes`.

        Takes either a single key or a list of (key, direction) pairs.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOHAYSTACK`,
        :data:`~pymongo.GEOSPHERE`, :data:`~pymongo.HASHED`,
        :data:`~pymongo.TEXT`).

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated.
          - `unique`: if ``True`` creates a uniqueness constraint on the index.
          - `background`: if ``True`` this index should be created in the
            background.
          - `sparse`: if ``True``, omit from the index any documents that lack
            the indexed field.
          - `bucketSize`: for use with geoHaystack indexes.
            Number of documents to group together within a certain proximity
            to a given longitude and latitude.
          - `min`: minimum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `max`: maximum value for keys in a :data:`~pymongo.GEO2D`
            index.
          - `expireAfterSeconds`: <int> Used to create an expiring (TTL)
            collection. MongoDB will automatically delete documents from
            this collection after <int> seconds. The indexed field must
            be a UTC datetime or the data will not expire.
          - `partialFilterExpression`: A document that specifies a filter for
            a partial index.

        See the MongoDB documentation for a full list of supported options by
        server version.

        .. note:: `partialFilterExpression` requires server version **>= 3.2**

        :Parameters:
          - `keys`: a single key or a list of (key, direction)
            pairs specifying the index to create
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments

        .. versionchanged:: 3.2
            Added partialFilterExpression to support partial indexes.
        """
        keys = _index_list(keys)
        if "name" not in kwargs:
            kwargs["name"] = _gen_index_name(keys)
        kwargs["key"] = _index_document(keys)
        self.__document = kwargs

    @property
    def document(self):
        """An index document suitable for passing to the createIndexes
        command.
        """
        return self.__document

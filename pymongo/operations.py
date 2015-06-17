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

from pymongo.common import validate_boolean, validate_is_dict


class _WriteOp(object):
    """Private base class for all write operations."""

    __slots__ = ("_filter", "_doc", "_upsert")

    def __init__(self, filter=None, doc=None, upsert=None):
        if filter is not None:
            validate_is_dict("filter", filter)
        if upsert is not None:
            validate_boolean("upsert", upsert)
        self._filter = filter
        self._doc = doc
        self._upsert = upsert


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


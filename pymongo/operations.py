# Copyright 2015-present MongoDB, Inc.
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
from typing import Any, Dict, Generic, List, Mapping, Optional, Sequence, Tuple, Union

from bson.raw_bson import RawBSONDocument
from pymongo import helpers
from pymongo.collation import validate_collation_or_none
from pymongo.common import validate_boolean, validate_is_mapping, validate_list
from pymongo.helpers import _gen_index_name, _index_document, _index_list
from pymongo.typings import _CollationIn, _DocumentType, _Pipeline

# Hint supports index name, "myIndex", or list of either strings or index pairs: [('x', 1), ('y', -1), 'z'']
_IndexList = Sequence[Union[str, Tuple[str, Union[int, str, Mapping[str, Any]]]]]
_IndexKeyHint = Union[str, _IndexList]


class InsertOne(Generic[_DocumentType]):
    """Represents an insert_one operation."""

    __slots__ = ("_doc",)

    def __init__(self, document: Union[_DocumentType, RawBSONDocument]) -> None:
        """Create an InsertOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `document`: The document to insert. If the document is missing an
            _id field one will be added.
        """
        self._doc = document

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_insert(self._doc)

    def __repr__(self):
        return f"InsertOne({self._doc!r})"

    def __eq__(self, other: Any) -> bool:
        if type(other) == type(self):
            return other._doc == self._doc
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other


class DeleteOne:
    """Represents a delete_one operation."""

    __slots__ = ("_filter", "_collation", "_hint")

    def __init__(
        self,
        filter: Mapping[str, Any],
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
    ) -> None:
        """Create a DeleteOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.

        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        if filter is not None:
            validate_is_mapping("filter", filter)
        if hint is not None:
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)
        self._filter = filter
        self._collation = collation
        self._hint = hint

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_delete(self._filter, 1, collation=self._collation, hint=self._hint)

    def __repr__(self):
        return f"DeleteOne({self._filter!r}, {self._collation!r})"

    def __eq__(self, other: Any) -> bool:
        if type(other) == type(self):
            return (other._filter, other._collation) == (self._filter, self._collation)
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other


class DeleteMany:
    """Represents a delete_many operation."""

    __slots__ = ("_filter", "_collation", "_hint")

    def __init__(
        self,
        filter: Mapping[str, Any],
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
    ) -> None:
        """Create a DeleteMany instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the documents to delete.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.4 and above.

        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        if filter is not None:
            validate_is_mapping("filter", filter)
        if hint is not None:
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)
        self._filter = filter
        self._collation = collation
        self._hint = hint

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_delete(self._filter, 0, collation=self._collation, hint=self._hint)

    def __repr__(self):
        return f"DeleteMany({self._filter!r}, {self._collation!r})"

    def __eq__(self, other: Any) -> bool:
        if type(other) == type(self):
            return (other._filter, other._collation) == (self._filter, self._collation)
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other


class ReplaceOne(Generic[_DocumentType]):
    """Represents a replace_one operation."""

    __slots__ = ("_filter", "_doc", "_upsert", "_collation", "_hint")

    def __init__(
        self,
        filter: Mapping[str, Any],
        replacement: Union[_DocumentType, RawBSONDocument],
        upsert: bool = False,
        collation: Optional[_CollationIn] = None,
        hint: Optional[_IndexKeyHint] = None,
    ) -> None:
        """Create a ReplaceOne instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to replace.
          - `replacement`: The new document.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.

        .. versionchanged:: 3.11
           Added the ``hint`` option.
        .. versionchanged:: 3.5
           Added the ``collation`` option.
        """
        if filter is not None:
            validate_is_mapping("filter", filter)
        if upsert is not None:
            validate_boolean("upsert", upsert)
        if hint is not None:
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)

        self._filter = filter
        self._doc = replacement
        self._upsert = upsert
        self._collation = collation
        self._hint = hint

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_replace(
            self._filter, self._doc, self._upsert, collation=self._collation, hint=self._hint
        )

    def __eq__(self, other: Any) -> bool:
        if type(other) == type(self):
            return (other._filter, other._doc, other._upsert, other._collation, other._hint) == (
                self._filter,
                self._doc,
                self._upsert,
                self._collation,
                other._hint,
            )
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __repr__(self):
        return "{}({!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__,
            self._filter,
            self._doc,
            self._upsert,
            self._collation,
            self._hint,
        )


class _UpdateOp:
    """Private base class for update operations."""

    __slots__ = ("_filter", "_doc", "_upsert", "_collation", "_array_filters", "_hint")

    def __init__(self, filter, doc, upsert, collation, array_filters, hint):
        if filter is not None:
            validate_is_mapping("filter", filter)
        if upsert is not None:
            validate_boolean("upsert", upsert)
        if array_filters is not None:
            validate_list("array_filters", array_filters)
        if hint is not None:
            if not isinstance(hint, str):
                hint = helpers._index_document(hint)

        self._filter = filter
        self._doc = doc
        self._upsert = upsert
        self._collation = collation
        self._array_filters = array_filters
        self._hint = hint

    def __eq__(self, other):
        if type(other) == type(self):
            return (
                other._filter,
                other._doc,
                other._upsert,
                other._collation,
                other._array_filters,
                other._hint,
            ) == (
                self._filter,
                self._doc,
                self._upsert,
                self._collation,
                self._array_filters,
                self._hint,
            )
        return NotImplemented

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "{}({!r}, {!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__,
            self._filter,
            self._doc,
            self._upsert,
            self._collation,
            self._array_filters,
            self._hint,
        )


class UpdateOne(_UpdateOp):
    """Represents an update_one operation."""

    __slots__ = ()

    def __init__(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        collation: Optional[_CollationIn] = None,
        array_filters: Optional[List[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
    ) -> None:
        """Represents an update_one operation.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the document to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.

        .. versionchanged:: 3.11
           Added the `hint` option.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added the `array_filters` option.
        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        super().__init__(filter, update, upsert, collation, array_filters, hint)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_update(
            self._filter,
            self._doc,
            False,
            self._upsert,
            collation=self._collation,
            array_filters=self._array_filters,
            hint=self._hint,
        )


class UpdateMany(_UpdateOp):
    """Represents an update_many operation."""

    __slots__ = ()

    def __init__(
        self,
        filter: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        upsert: bool = False,
        collation: Optional[_CollationIn] = None,
        array_filters: Optional[List[Mapping[str, Any]]] = None,
        hint: Optional[_IndexKeyHint] = None,
    ) -> None:
        """Create an UpdateMany instance.

        For use with :meth:`~pymongo.collection.Collection.bulk_write`.

        :Parameters:
          - `filter`: A query that matches the documents to update.
          - `update`: The modifications to apply.
          - `upsert` (optional): If ``True``, perform an insert if no documents
            match the filter.
          - `collation` (optional): An instance of
            :class:`~pymongo.collation.Collation`.
          - `array_filters` (optional): A list of filters specifying which
            array elements an update should apply.
          - `hint` (optional): An index to use to support the query
            predicate specified either by its string name, or in the same
            format as passed to
            :meth:`~pymongo.collection.Collection.create_index` (e.g.
            ``[('field', ASCENDING)]``). This option is only supported on
            MongoDB 4.2 and above.

        .. versionchanged:: 3.11
           Added the `hint` option.
        .. versionchanged:: 3.9
           Added the ability to accept a pipeline as the `update`.
        .. versionchanged:: 3.6
           Added the `array_filters` option.
        .. versionchanged:: 3.5
           Added the `collation` option.
        """
        super().__init__(filter, update, upsert, collation, array_filters, hint)

    def _add_to_bulk(self, bulkobj):
        """Add this operation to the _Bulk instance `bulkobj`."""
        bulkobj.add_update(
            self._filter,
            self._doc,
            True,
            self._upsert,
            collation=self._collation,
            array_filters=self._array_filters,
            hint=self._hint,
        )


class IndexModel:
    """Represents an index to create."""

    __slots__ = ("__document",)

    def __init__(self, keys: _IndexKeyHint, **kwargs: Any) -> None:
        """Create an Index instance.

        For use with :meth:`~pymongo.collection.Collection.create_indexes`.

        Takes either a single key or a list containing (key, direction) pairs
        or keys.  If no direction is given, :data:`~pymongo.ASCENDING` will
        be assumed.
        The key(s) must be an instance of :class:`basestring`
        (:class:`str` in python 3), and the direction(s) must be one of
        (:data:`~pymongo.ASCENDING`, :data:`~pymongo.DESCENDING`,
        :data:`~pymongo.GEO2D`, :data:`~pymongo.GEOSPHERE`,
        :data:`~pymongo.HASHED`, :data:`~pymongo.TEXT`).

        Valid options include, but are not limited to:

          - `name`: custom name to use for this index - if none is
            given, a name will be generated.
          - `unique`: if ``True``, creates a uniqueness constraint on the index.
          - `background`: if ``True``, this index should be created in the
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
          - `collation`: An instance of :class:`~pymongo.collation.Collation`
            that specifies the collation to use.
          - `wildcardProjection`: Allows users to include or exclude specific
            field paths from a `wildcard index`_ using the { "$**" : 1} key
            pattern. Requires MongoDB >= 4.2.
          - `hidden`: if ``True``, this index will be hidden from the query
            planner and will not be evaluated as part of query plan
            selection. Requires MongoDB >= 4.4.

        See the MongoDB documentation for a full list of supported options by
        server version.

        :Parameters:
          - `keys`: a single key or a list containing (key, direction) pairs
             or keys specifying the index to create.
          - `**kwargs` (optional): any additional index creation
            options (see the above list) should be passed as keyword
            arguments.

        .. versionchanged:: 3.11
           Added the ``hidden`` option.
        .. versionchanged:: 3.2
           Added the ``partialFilterExpression`` option to support partial
           indexes.

        .. _wildcard index: https://mongodb.com/docs/master/core/index-wildcard/
        """
        keys = _index_list(keys)
        if kwargs.get("name") is None:
            kwargs["name"] = _gen_index_name(keys)
        kwargs["key"] = _index_document(keys)
        collation = validate_collation_or_none(kwargs.pop("collation", None))
        self.__document = kwargs
        if collation is not None:
            self.__document["collation"] = collation

    @property
    def document(self) -> Dict[str, Any]:
        """An index document suitable for passing to the createIndexes
        command.
        """
        return self.__document

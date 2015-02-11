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

"""Result class definitions."""

from pymongo.errors import InvalidOperation


class InsertOneResult(object):
    """The return type for :meth:`Collection.insert_one`."""

    __slots__ = ("__inserted_id", "__acknowledged")

    def __init__(self, inserted_id, acknowledged):
        self.__inserted_id = inserted_id
        self.__acknowledged = acknowledged

    @property
    def inserted_id(self):
        """The inserted document's _id."""
        return self.__inserted_id

    @property
    def acknowledged(self):
        """Is this the result of an acknowledged write operation?"""
        return self.__acknowledged


class BulkWriteResult(object):
    """An object wrapper for bulk API write results."""

    __slots__ = ("__bulk_api_result", "__acknowledged")

    def __init__(self, bulk_api_result, acknowledged):
        """Create a BulkWriteResult instance.

        :Parameters:
          - `bulk_api_result`: A result dict from the bulk API
          - `acknowledged`: Was this write result acknowledged? If ``False``
            then all properties of this object will raise
            :exc:`~pymongo.errors.InvalidOperation`.
        """
        self.__bulk_api_result = bulk_api_result
        self.__acknowledged = acknowledged

    def __raise_if_unacknowledged(self, property_name):
        """Raise an exception on property access if unacknowledged."""
        if not self.__acknowledged:
            raise InvalidOperation("A value for %s is not available when "
                                   "the write is unacknowledged. Check the "
                                   "acknowledged attribute to avoid this "
                                   "error." % (property_name,))

    @property
    def bulk_api_result(self):
        """The raw bulk API result."""
        return self.__bulk_api_result

    @property
    def acknowledged(self):
        """Is this the result of an acknowledged bulk write operation?"""
        return self.__acknowledged

    @property
    def inserted_count(self):
        """The number of documents inserted."""
        self.__raise_if_unacknowledged("inserted_count")
        return self.__bulk_api_result.get("nInserted")

    @property
    def matched_count(self):
        """The number of documents matched for an update."""
        self.__raise_if_unacknowledged("matched_count")
        return self.__bulk_api_result.get("nMatched")

    @property
    def modified_count(self):
        """The number of documents modified.

        .. note:: modified_count is only reported by MongoDB 2.6 and later.
          When connected to an earlier server version, or in certain mixed
          version sharding configurations, this attribute will be set to
          ``None``.
        """
        self.__raise_if_unacknowledged("modified_count")
        return self.__bulk_api_result.get("nModified")

    @property
    def deleted_count(self):
        """The number of documents deleted."""
        self.__raise_if_unacknowledged("deleted_count")
        return self.__bulk_api_result.get("nRemoved")

    @property
    def upserted_count(self):
        """The number of documents upserted."""
        self.__raise_if_unacknowledged("upserted_count")
        return self.__bulk_api_result.get("nUpserted")

    @property
    def upserted_ids(self):
        """A map of operation index to the _id of the upserted document."""
        self.__raise_if_unacknowledged("upserted_ids")
        if self.__bulk_api_result:
            return dict((upsert["index"], upsert["_id"])
                        for upsert in self.bulk_api_result["upserted"])

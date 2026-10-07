# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.


"""Constants, types, and classes shared across Client Bulk Write API implementations."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import TYPE_CHECKING, Any, NoReturn, Optional, Union

from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from pymongo.common import (
    validate_is_document_type,
    validate_ok_for_replace,
    validate_ok_for_update,
)
from pymongo.errors import ClientBulkWriteException, OperationFailure
from pymongo.helpers_shared import _get_wce_doc
from pymongo.message import _ClientBulkWriteContext
from pymongo.write_concern import WriteConcern

if TYPE_CHECKING:
    from pymongo.typings import _AgnosticMongoClient, _DocumentOut, _Pipeline


def _merge_command(
    ops: list[tuple[str, Mapping[str, Any]]],
    offset: int,
    full_result: MutableMapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    """Merge result of a single bulk write batch into the full result."""
    if result.get("error"):
        full_result["error"] = result["error"]

    full_result["nInserted"] += result.get("nInserted", 0)
    full_result["nDeleted"] += result.get("nDeleted", 0)
    full_result["nMatched"] += result.get("nMatched", 0)
    full_result["nModified"] += result.get("nModified", 0)
    full_result["nUpserted"] += result.get("nUpserted", 0)

    write_errors = result.get("writeErrors")
    if write_errors:
        for doc in write_errors:
            # Leave the server response intact for APM.
            replacement = doc.copy()
            original_index = doc["idx"] + offset
            replacement["idx"] = original_index
            # Add the failed operation to the error document.
            replacement["op"] = ops[original_index][1]
            full_result["writeErrors"].append(replacement)

    wce = _get_wce_doc(result)
    if wce:
        full_result["writeConcernErrors"].append(wce)


def _throw_client_bulk_write_exception(
    full_result: _DocumentOut, verbose_results: bool
) -> NoReturn:
    """Raise a ClientBulkWriteException from the full result."""
    # retryWrites on MMAPv1 should raise an actionable error.
    if full_result["writeErrors"]:
        full_result["writeErrors"].sort(key=lambda error: error["idx"])
        err = full_result["writeErrors"][0]
        code = err["code"]
        msg = err["errmsg"]
        if code == 20 and msg.startswith("Transaction numbers"):
            errmsg = (
                "This MongoDB deployment does not support "
                "retryable writes. Please add retryWrites=false "
                "to your connection string."
            )
            raise OperationFailure(errmsg, code, full_result)
    if isinstance(full_result["error"], BaseException):
        raise ClientBulkWriteException(full_result, verbose_results) from full_result["error"]
    raise ClientBulkWriteException(full_result, verbose_results)


class _AgnosticClientBulkBase:
    """The private guts of the client-level bulk write API."""

    def __init__(
        self,
        client: _AgnosticMongoClient,
        write_concern: WriteConcern,
        ordered: bool = True,
        bypass_document_validation: Optional[bool] = None,
        comment: Optional[str] = None,
        let: Optional[Any] = None,
        verbose_results: bool = False,
    ) -> None:
        """Initialize a _AgnosticClientBulkBase instance."""
        self.client = client
        self.write_concern = write_concern
        self.let = let
        if self.let is not None:
            validate_is_document_type("let", self.let)
        self.ordered = ordered
        self.bypass_doc_val = bypass_document_validation
        self.comment = comment
        self.verbose_results = verbose_results
        self.ops: list[tuple[str, Mapping[str, Any]]] = []
        self.namespaces: list[str] = []
        self.idx_offset: int = 0
        self.total_ops: int = 0
        self.executed = False
        self.uses_collation = False
        self.uses_array_filters = False
        self.is_retryable = self.client.options.retry_writes
        self.retrying = False
        self.started_retryable_write = False

    @property
    def bulk_ctx_class(self) -> type[_ClientBulkWriteContext]:
        return _ClientBulkWriteContext

    def add_insert(self, namespace: str, document: _DocumentOut) -> None:
        """Add an insert document to the list of ops."""
        validate_is_document_type("document", document)
        # Generate ObjectId client side.
        if not (isinstance(document, RawBSONDocument) or "_id" in document):
            document["_id"] = ObjectId()
        cmd = {"insert": -1, "document": document}
        self.ops.append(("insert", cmd))
        self.namespaces.append(namespace)
        self.total_ops += 1

    def add_update(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        multi: bool,
        upsert: Optional[bool] = None,
        collation: Optional[Mapping[str, Any]] = None,
        array_filters: Optional[list[Mapping[str, Any]]] = None,
        hint: Union[str, dict[str, Any], None] = None,
        sort: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Create an update document and add it to the list of ops."""
        validate_ok_for_update(update)
        cmd = {
            "update": -1,
            "filter": selector,
            "updateMods": update,
            "multi": multi,
        }
        if upsert is not None:
            cmd["upsert"] = upsert
        if array_filters is not None:
            self.uses_array_filters = True
            cmd["arrayFilters"] = array_filters
        if hint is not None:
            cmd["hint"] = hint
        if collation is not None:
            self.uses_collation = True
            cmd["collation"] = collation
        if sort is not None:
            cmd["sort"] = sort
        if multi:
            # A bulk_write containing an update_many is not retryable.
            self.is_retryable = False
        self.ops.append(("update", cmd))
        self.namespaces.append(namespace)
        self.total_ops += 1

    def add_replace(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: Optional[bool] = None,
        collation: Optional[Mapping[str, Any]] = None,
        hint: Union[str, dict[str, Any], None] = None,
        sort: Optional[Mapping[str, Any]] = None,
    ) -> None:
        """Create a replace document and add it to the list of ops."""
        validate_ok_for_replace(replacement)
        cmd = {
            "update": -1,
            "filter": selector,
            "updateMods": replacement,
            "multi": False,
        }
        if upsert is not None:
            cmd["upsert"] = upsert
        if hint is not None:
            cmd["hint"] = hint
        if collation is not None:
            self.uses_collation = True
            cmd["collation"] = collation
        if sort is not None:
            cmd["sort"] = sort
        self.ops.append(("replace", cmd))
        self.namespaces.append(namespace)
        self.total_ops += 1

    def add_delete(
        self,
        namespace: str,
        selector: Mapping[str, Any],
        multi: bool,
        collation: Optional[Mapping[str, Any]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        """Create a delete document and add it to the list of ops."""
        cmd = {"delete": -1, "filter": selector, "multi": multi}
        if hint is not None:
            cmd["hint"] = hint
        if collation is not None:
            self.uses_collation = True
            cmd["collation"] = collation
        if multi:
            # A bulk_write containing a delete_many is not retryable.
            self.is_retryable = False
        self.ops.append(("delete", cmd))
        self.namespaces.append(namespace)
        self.total_ops += 1

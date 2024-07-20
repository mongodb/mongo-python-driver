# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.


"""Constants, types, and classes shared across Client Bulk Write API implementations."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, NoReturn, Optional

from pymongo.errors import OperationFailure, WriteConcernError, WriteError
from pymongo.helpers_shared import _get_wce_doc
from pymongo.results import ClientBulkWriteResult

if TYPE_CHECKING:
    from pymongo.typings import _DocumentOut


_DELETE_ALL: int = 0
_DELETE_ONE: int = 1

# For backwards compatibility. See MongoDB src/mongo/base/error_codes.err
_BAD_VALUE: int = 2
_UNKNOWN_ERROR: int = 8
_WRITE_CONCERN_ERROR: int = 64

_COMMANDS: tuple[str, str, str] = ("insert", "update", "delete")


class _Run:
    """Represents a batch of bulk write operations."""

    def __init__(self) -> None:
        """Initialize a new Run object."""
        self.index_map: list[int] = []
        self.ops: list[tuple[str, Mapping[str, Any]]] = []
        self.idx_offset: int = 0
        self.total_ops: int = 0

    def index(self, idx: int) -> int:
        """Get the original index of an operation in this run.

        :param idx: The Run index that maps to the original index.
        """
        return self.index_map[idx]

    def add(self, original_index: int, operation: tuple[str, Any]) -> None:
        """Add an operation and its namespace to this Run instance.

        :param original_index: The original index of this operation
            within a larger bulk operation.
        :param operation: A tuple containing the operation type and
            the operation document.
        """
        self.index_map.append(original_index)
        self.ops.append(operation)
        self.total_ops += 1


def _merge_command(
    run: _Run,
    full_result: MutableMapping[str, Any],
    offset: int,
    result: Mapping[str, Any],
) -> None:
    """Merge result of a single bulk write batch into the full result."""
    if result.get("error"):
        full_result["error"] = result["error"]

    full_result["nInserted"] += result.get("nInserted")
    full_result["nDeleted"] += result.get("nDeleted")
    full_result["nMatched"] += result.get("nMatched")
    full_result["nModified"] += result.get("nModified")
    full_result["nUpserted"] += result.get("nUpserted")
    upserted = result.get("upserted")
    if upserted:
        full_result["upserted"].extend(upserted)

    write_errors = result.get("writeErrors")
    if write_errors:
        for doc in write_errors:
            # Leave the server response intact for APM.
            replacement = doc.copy()
            idx = doc["idx"] + offset
            replacement["idx"] = run.index(idx)
            # Add the failed operation to the error document.
            replacement["op"] = run.ops[idx][1]
            full_result["writeErrors"].append(replacement)

    wce = _get_wce_doc(result)
    if wce:
        full_result["writeConcernErrors"].append(wce)


class ClientBulkWriteException(OperationFailure):
    """Exception class for client-level bulk write errors.

    .. versionadded:: TODO
    """

    details: _DocumentOut
    verbose: bool

    def __init__(self, results: _DocumentOut, verbose: bool) -> None:
        super().__init__("batch op errors occurred", 65, results)
        self.verbose = verbose

    def __reduce__(self) -> tuple[Any, Any]:
        return self.__class__, (self.details,)

    @property
    def error(self) -> Optional[Any]:
        return self.details.get("error", None)

    @property
    def write_concern_errors(self) -> Optional[list[WriteConcernError]]:
        return self.details.get("writeConcernErrors", [])

    @property
    def write_errors(self) -> Optional[Mapping[int, WriteError]]:
        return self.details.get("writeErrors", {})

    @property
    def partial_result(self) -> Optional[ClientBulkWriteResult]:
        if self.details.get("anySuccessful"):
            return ClientBulkWriteResult(
                self.details,
                acknowledged=True,
                has_verbose_results=self.verbose,
            )
        return None


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
    raise ClientBulkWriteException(full_result, verbose_results)

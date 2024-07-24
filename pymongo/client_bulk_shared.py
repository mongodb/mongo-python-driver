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


class ClientBulkWriteException(OperationFailure):
    """Exception class for client-level bulk write errors."""

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
                self.details,  # type: ignore[arg-type]
                acknowledged=True,
                has_verbose_results=self.verbose,
            )
        return None


def _merge_command(
    ops: list[tuple[str, Mapping[str, Any]]],
    offset: int,
    full_result: MutableMapping[str, Any],
    result: Mapping[str, Any],
) -> None:
    """Merge result of a single bulk write batch into the full result."""
    full_result["nInserted"] += result.get("nInserted")
    full_result["nDeleted"] += result.get("nDeleted")
    full_result["nMatched"] += result.get("nMatched")
    full_result["nModified"] += result.get("nModified")
    full_result["nUpserted"] += result.get("nUpserted")

    if result.get("error"):
        full_result["error"] = result["error"]

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
    raise ClientBulkWriteException(full_result, verbose_results)

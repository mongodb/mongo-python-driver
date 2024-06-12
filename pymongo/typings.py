# Copyright 2022-Present MongoDB, Inc.
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

"""Type aliases used by PyMongo"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from bson import Int64, Timestamp
from bson.typings import _DocumentOut, _DocumentType, _DocumentTypeArg
from pymongo.read_preferences import _ServerMode

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient, MongoClient
    from pymongo.asynchronous.bulk import _AsyncBulk
    from pymongo.asynchronous.client_session import AsyncClientSession, SessionOptions
    from pymongo.asynchronous.pool import AsyncConnection
    from pymongo.bulk_shared import _Run
    from pymongo.collation import Collation
    from pymongo.message import _BulkWriteContext
    from pymongo.synchronous.bulk import _Bulk
    from pymongo.synchronous.client_session import ClientSession
    from pymongo.synchronous.pool import Connection

_IS_SYNC = False

# Common Shared Types.
_Address = Tuple[str, Optional[int]]
_CollationIn = Union[Mapping[str, Any], "Collation"]
_Pipeline = Sequence[Mapping[str, Any]]
ClusterTime = Mapping[str, Any]

_T = TypeVar("_T")

# Type hinting types for compatibility between async and sync classes


class _BaseBulk(ABC):
    @property
    @abstractmethod
    def bulk_ctx_class(self) -> Type[_BulkWriteContext]:
        ...

    @abstractmethod
    def add_insert(self, document: _DocumentOut) -> None:
        ...

    @abstractmethod
    def add_update(
        self,
        selector: Mapping[str, Any],
        update: Union[Mapping[str, Any], _Pipeline],
        multi: bool = False,
        upsert: bool = False,
        collation: Optional[Mapping[str, Any]] = None,
        array_filters: Optional[list[Mapping[str, Any]]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        ...

    @abstractmethod
    def add_replace(
        self,
        selector: Mapping[str, Any],
        replacement: Mapping[str, Any],
        upsert: bool = False,
        collation: Optional[Mapping[str, Any]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        ...

    @abstractmethod
    def add_delete(
        self,
        selector: Mapping[str, Any],
        limit: int,
        collation: Optional[Mapping[str, Any]] = None,
        hint: Union[str, dict[str, Any], None] = None,
    ) -> None:
        ...

    @abstractmethod
    def gen_ordered(self) -> Iterator[Optional[_Run]]:
        ...

    @abstractmethod
    def gen_unordered(self) -> Iterator[_Run]:
        ...


class _BaseClientSession(ABC):
    @abstractmethod
    def _check_ended(self) -> None:
        ...

    @property
    @abstractmethod
    def client(self) -> AsyncMongoClient:
        ...

    @property
    @abstractmethod
    def options(self) -> SessionOptions:
        ...

    @property
    @abstractmethod
    def session_id(self) -> Mapping[str, Any]:
        ...

    @property
    @abstractmethod
    def _transaction_id(self) -> Int64:
        ...

    @property
    @abstractmethod
    def cluster_time(self) -> Optional[ClusterTime]:
        ...

    @property
    @abstractmethod
    def operation_time(self) -> Optional[Timestamp]:
        ...

    @abstractmethod
    def _inherit_option(self, name: str, val: _T) -> _T:
        ...

    @abstractmethod
    def _advance_cluster_time(self, cluster_time: Optional[Mapping[str, Any]]) -> None:
        ...

    @abstractmethod
    def advance_cluster_time(self, cluster_time: Mapping[str, Any]) -> None:
        ...

    @abstractmethod
    def _advance_operation_time(self, operation_time: Optional[Timestamp]) -> None:
        ...

    @abstractmethod
    def advance_operation_time(self, operation_time: Timestamp) -> None:
        ...

    @abstractmethod
    def _process_response(self, reply: Mapping[str, Any]) -> None:
        ...

    @property
    @abstractmethod
    def has_ended(self) -> bool:
        ...

    @property
    @abstractmethod
    def in_transaction(self) -> bool:
        ...

    @property
    @abstractmethod
    def _starting_transaction(self) -> bool:
        ...

    @property
    @abstractmethod
    def _pinned_address(self) -> Optional[_Address]:
        ...

    @property
    @abstractmethod
    def _pinned_connection(self) -> Optional[Any]:
        ...

    @abstractmethod
    def _pin(self, server: Any, conn: Any) -> None:
        ...

    @abstractmethod
    def _txn_read_preference(self) -> Optional[_ServerMode]:
        ...

    @abstractmethod
    def _materialize(self, logical_session_timeout_minutes: Optional[int] = None) -> None:
        ...

    @abstractmethod
    def _apply_to(
        self,
        command: MutableMapping[str, Any],
        is_retryable: bool,
        read_preference: _ServerMode,
        conn: Any,
    ) -> None:
        ...

    @abstractmethod
    def _start_retryable_write(self) -> None:
        ...

    @abstractmethod
    def _update_read_concern(self, cmd: MutableMapping[str, Any], conn: Any) -> None:
        ...

    @abstractmethod
    def __copy__(self) -> NoReturn:
        ...


_AgnosticMongoClient = Union["AsyncMongoClient", "MongoClient"]
_AgnosticConnection = Union["AsyncConnection", "Connection"]
_AgnosticClientSession = Union["AsyncClientSession", "ClientSession"]
_AgnosticBulk = Union["_AsyncBulk", "_Bulk"]


def strip_optional(elem: Optional[_T]) -> _T:
    """This function is to allow us to cast all the elements of an iterator from Optional[_T] to _T
    while inside a list comprehension.
    """
    assert elem is not None
    return elem


__all__ = [
    "_DocumentOut",
    "_DocumentType",
    "_DocumentTypeArg",
    "_Address",
    "_CollationIn",
    "_Pipeline",
    "strip_optional",
    "_AgnosticMongoClient",
]

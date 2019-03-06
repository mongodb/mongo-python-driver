import datetime
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

from bson import CodecOptions
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.collation import Collation
from pymongo.monitoring import _EventListeners
from pymongo.pool import SocketInfo
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode

MAX_INT32: int = ...
MIN_INT32: int = ...

class _Query(object):
    def __init__(
        self,
        flags: bool,
        db: Database,
        coll: Collection,
        ntoskip: int,
        spec: Mapping[str, Any],
        fields: Union[Mapping[str, bool], Sequence[str]],
        codec_options: CodecOptions,
        read_preference: _ServerMode,
        limit: int,
        batch_size: int,
        read_concern: ReadConcern,
        collation: Collation) -> None: ...
    def as_command(self) -> Dict[str, Any]: ...
    def get_message(self, set_slave_ok: bool, is_mongos: bool, use_cmd: bool = ...) -> Tuple[int, bytes, int]: ...

class _GetMore(object):
    def __init__(
        self,
        db: Database,
        coll: Collection,
        ntoreturn: int,
        cursor_id: int,
        codec_options: CodecOptions,
        max_await_time_ms: Optional[int] = ...) -> None: ...
    def as_command(self) -> Mapping[str, Any]: ...
    def get_message(self, dummy0: Any, dummy1: Any, use_cmd: bool = ...) -> Tuple[Any]: ...

class _CursorAddress(Tuple[str, int]):
    def __new__(cls, address: Tuple[str, int], namespace: Any) -> '_CursorAddress': ...
    @property
    def namespace(self) -> Any: ...
    def __hash__(self) -> int: ...
    def __eq__(self, other: Any) -> bool: ...
    def __ne__(self, other: Any) -> bool: ...

def __last_error(namespace: str, args: Dict[str, Any]) -> Tuple[int, bytes, int]: ...
def __pack_message(operation: Union[_Query, _GetMore], data: bytes) -> Tuple[int, bytes]: ...
def insert(
    collection_name: str,
    docs: Mapping[str, Any],
    check_keys: bool,
    safe: bool,
    last_error_args: Mapping[str, Any],
    continue_on_error: bool,
    opts: CodecOptions) -> Tuple[int, bytes, int]: ...
def update(
    collection_name: str,
    upsert: bool,
    multi: bool,
    spec: Mapping[str, Any],
    doc: Mapping[str, Any],
    safe: bool,
    last_error_args: Mapping[str, Any],
    check_keys: bool,
    opts: CodecOptions) -> Tuple[int, bytes, int]: ...
def query(
    options: int,
    collection_name: str,
    num_to_skip: int,
    num_to_return: int,
    query: Mapping[str, Any],
    field_selector: Mapping[str, Any],
    opts: CodecOptions,
    check_keys: bool = ...) -> Tuple[int, bytes, int]: ...
def get_more(collection_name: str, num_to_return: int, cursor_id: int) -> Tuple[int, bytes]: ...
def delete(
    collection_name: str,
    spec: Mapping[str, Any],
    safe: bool,
    last_error_args: Mapping[str, Any],
    opts: CodecOptions, flags: int = ...) -> Tuple[int, bytes, int]: ...
def kill_cursors(cursor_ids: Sequence[int]) -> Tuple[int, bytes]: ...
_FIELD_MAP: Dict[str, str] = ...

class _BulkWriteContext(object):
    def __init__(
        self,
        database_name: str,
        command: Mapping[str, Any],
        sock_info: SocketInfo,
        operation_id: int,
        listeners: _EventListeners) -> None: ...
    @property
    def max_bson_size(self) -> int: ...
    @property
    def max_message_size(self) -> int: ...
    @property
    def max_write_batch_size(self) -> int: ...
    def legacy_write(
        self,
        request_id: int,
        msg: bytes,
        max_doc_size: int,
        acknowledged: bool,
        docs: Sequence[Mapping[str, Any]]) -> Dict[str, Any]: ...
    def write_command(self, request_id: int, msg: bytes, docs: Sequence[Mapping[str, Any]]) -> Dict[str, Any]: ...

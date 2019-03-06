from socket import socket
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple, Union

from bson import CodecOptions
from pymongo.collation import Collation
from pymongo.monitoring import _EventListeners
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ServerMode


def command(
    sock: socket,
    dbname: str,
    spec: Mapping[str, Any],
    slave_ok: bool,
    is_mongos: bool,
    read_preference: _ServerMode,
    codec_options: CodecOptions,
    check: bool = ...,
    allowable_errors: Optional[Sequence[str]] = ...,
    address: Optional[Tuple[str, int]] = ...,
    check_keys: bool = ...,
    listeners: Optional[_EventListeners] = ...,
    max_bson_size: Optional[int] = ...,
    read_concern: ReadConcern = ...,
    parse_write_concern_error: bool = ...,
    collation: Optional[Collation] = ...) -> Dict[str, Any]: ...
def receive_message(sock: socket, operation: int, request_id: int, max_message_size: int = ...) -> bytes: ...

class SocketChecker(object):
    def __init__(self) -> None: ...
    def socket_closed(self, sock: socket) -> bool: ...

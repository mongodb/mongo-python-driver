import contextlib
import sys

import sys
if sys.version_info >= (3,):
    from queue import Queue
else:
    from Queue import Queue
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence, Tuple, Union

from bson import ObjectId
from pymongo.auth import MongoCredential
from pymongo.message import _GetMore, _Query
from pymongo.monitor import Monitor
from pymongo.monitoring import _EventListeners
from pymongo.pool import Pool, SocketInfo
from pymongo.response import Response
from pymongo.server_description import ServerDescription


class Server(object):
    def __init__(
        self,
        server_description: ServerDescription,
        pool: Pool,
        monitor: Monitor,
        topology_id: Optional[ObjectId] = ...,
        listeners: Optional[_EventListeners] = ...,
        events: Optional[Queue[Tuple[Callable[..., Any], Sequence[Any]]]] = ...) -> None: ...
    def open(self) -> None: ...
    def reset(self) -> None: ...
    def close(self) -> None: ...
    def request_check(self) -> None: ...
    def send_message(self, message: Tuple[int, bytes], all_credentials: Mapping[str, MongoCredential]) -> None: ...
    def send_message_with_response(
        self,
        operation: Union[_Query, _GetMore],
        set_slave_okay: bool,
        all_credentials: Mapping[str, MongoCredential],
        listeners: _EventListeners,
        exhaust: bool = ...) -> Response: ...
    @contextlib.contextmanager
    def get_socket(self, all_credentials: Mapping[str, MongoCredential], checkout: bool = ...) -> Iterator[SocketInfo]: ...
    @property
    def description(self) -> ServerDescription: ...
    @description.setter
    def description(self, server_description: ServerDescription) -> None: ...
    @property
    def pool(self) -> Pool: ...
    def __str__(self) -> str: ...

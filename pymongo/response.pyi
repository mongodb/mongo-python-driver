import datetime

from typing import Tuple

from pymongo.pool import Pool, SocketInfo

class Response(object):
    def __init__(
        self,
        data: bytes,
        address: Tuple[str, int],
        request_id: int,
        duration: datetime.timedelta,
        from_command: bool) -> None: ...
    @property
    def data(self) -> bytes: ...
    @property
    def address(self) -> Tuple[str, int]: ...
    @property
    def request_id(self) -> int: ...
    @property
    def duration(self) -> datetime.timedelta: ...
    @property
    def from_command(self) -> bool: ...

class ExhaustResponse(Response):
    def __init__(
        self,
        data: bytes,
        address: Tuple[str, int],
        socket_info: SocketInfo,
        pool: Pool,
        request_id: int,
        duration: datetime.timedelta,
        from_command: bool) -> None: ...
    @property
    def socket_info(self) -> SocketInfo: ...
    @property
    def pool(self) -> Pool: ...

from typing import Any, Optional, Tuple

from pymongo.ismaster import IsMaster
from pymongo.pool import Pool, SocketInfo
from pymongo.server_description import ServerDescription
from pymongo.settings import TopologySettings
from pymongo.topology import Topology


class Monitor(object):
    def __init__(
        self,
        server_description: ServerDescription,
        topology: Topology,
        pool: Pool,
        topology_settings: TopologySettings) -> None: ...
    def open(self) -> None: ...
    def close(self) -> None: ...
    def join(self, timeout: Optional[int] = ...) -> None: ...
    def request_check(self) -> None: ...

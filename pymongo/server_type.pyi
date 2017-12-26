from typing import NamedTuple

class ServerType(NamedTuple):
    Unknown: int
    Mongos: int
    RSPrimary: int
    RSSecondary: int
    RSArbiter: int
    RSOther: int
    RSGhost: int
    Standalone: int

SERVER_TYPE: ServerType = ...

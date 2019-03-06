from typing import Any, Dict, FrozenSet, NamedTuple

from pymongo.pool import SocketInfo

HAVE_KERBEROS: bool = True
MECHANISMS: FrozenSet[str] = ...

MongoCredential = NamedTuple('MongoCredential', [
    ('mechanism', str),
    ('source', str),
    ('username', str),
    ('password', str),
    ('props', Any),
])
GSSAPIProperties = NamedTuple('GSSAPIProperties', [
    ('service_name', str),
    ('canonicalize_host_name', bool),
    ('service_realm', Any),
])
def authenticate(credentials: MongoCredential, sock_info: SocketInfo) -> None: ...
def logout(source: str, sock_info: SocketInfo) -> None: ...

from typing import NamedTuple

ServerType = NamedTuple('ServerType', [
    ('Unknown', int),
    ('Mongos', int),
    ('RSPrimary', int),
    ('RSSecondary', int),
    ('RSArbiter', int),
    ('RSOther', int),
    ('RSGhost', int),
    ('Standalone', int),
])

SERVER_TYPE: ServerType = ...

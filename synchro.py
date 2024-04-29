from __future__ import annotations

from os import listdir
from pathlib import Path

from unasync import Rule, unasync_files

replacements = {
    "AsyncCollection": "Collection",
    "AsyncDatabase": "Database",
    "AsyncCursor": "Cursor",
    "AsyncMongoClient": "MongoClient",
    "AsyncCommandCursor": "CommandCursor",
    "AsyncRawBatchCursor": "RawBatchCursor",
    "AsyncRawBatchCommandCursor": "RawBatchCommandCursor",
    "async_command": "command",
    "async_receive_message": "receive_message",
    "async_sendall": "sendall",
    "a_wrap_socket": "wrap_socket",
    "asynchronous": "synchronous",
    "anext": "next",
    "_ALock": "_Lock",
    "_ACondition": "_Condition",
    "AsyncGridFS": "GridFS",
    "AsyncGridFSBucket": "GridFSBucket",
    "AsyncGridIn": "GridIn",
    "AsyncGridOut": "GridOut",
    "AsyncGridOutCursor": "GridOutCursor",
    "AsyncGridOutIterator": "GridOutIterator",
    "_AsyncGridOutChunkIterator": "GridOutChunkIterator",
    "_a_grid_in_property": "_grid_in_property",
    "_a_grid_out_property": "_grid_out_property",
    "AsyncMongoCryptCallback": "MongoCryptCallback",
    "AsyncExplicitEncrypter": "ExplicitEncrypter",
    "AsyncAutoEncrypter": "AutoEncrypter",
}

_pymongo_base = "pymongo/asynchronous"
_gridfs_base = "gridfs/asynchronous/"

_pymongo_dest_base = "pymongo/synchronous"
_gridfs_dest_base = "gridfs/synchronous/"


async_files = [
    _pymongo_base + f
    for f in listdir(_pymongo_base)
    if (Path(_pymongo_base) / "asynchronous" / f).is_file()
]

gridfs_files = [
    _gridfs_base + f
    for f in listdir(_gridfs_base)
    if (Path(_gridfs_base) / "asynchronous" / f).is_file()
]

unasync_files(
    async_files,
    [
        Rule(
            fromdir="/pymongo/asynchronous/",
            todir="/pymongo/synchronous/",
            additional_replacements=replacements,
        )
    ],
)

unasync_files(
    gridfs_files,
    [
        Rule(
            fromdir="/gridfs/asynchronous/",
            todir="/gridfs/synchronous/",
            additional_replacements=replacements,
        )
    ],
)

sync_files = [
    _pymongo_dest_base + f
    for f in listdir(_pymongo_dest_base)
    if (Path(_pymongo_dest_base) / "asynchronous" / f).is_file()
]

sync_gridfs_files = [
    _gridfs_dest_base + f
    for f in listdir(_gridfs_dest_base)
    if (Path(_gridfs_dest_base) / "asynchronous" / f).is_file()
]


for file in sync_files + sync_gridfs_files:
    with open(file, "r+") as f:
        lines = f.readlines()
        is_sync = next([line for line in lines if line.startswith("IS_SYNC = ")])
        index = lines.index(is_sync)
        is_sync = is_sync.replace("False", "True")
        lines[index] = is_sync
        f.seek(0)
        f.writelines(lines)
        f.truncate()

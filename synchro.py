from __future__ import annotations

from os import listdir
from os.path import isfile, join

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
}

async_files = [
    "./pymongo/asynchronous/" + f
    for f in listdir("pymongo/asynchronous")
    if isfile(join("pymongo/asynchronous", f))
]

gridfs_files = [
    "./gridfs/asynchronous/" + f
    for f in listdir("gridfs/asynchronous")
    if isfile(join("gridfs/asynchronous", f))
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

with open("gridfs/synchronous/grid_file.py", "r+") as f:
    lines = f.readlines()
    is_sync = [line for line in lines if line.startswith("IS_SYNC = ")][0]
    index = lines.index(is_sync)
    is_sync = is_sync.replace("False", "True")
    lines[index] = is_sync
    f.seek(0)
    f.writelines(lines)
    f.truncate()

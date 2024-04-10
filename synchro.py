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
    "_async": "_sync",
    "anext": "next",
    "_ALock": "_Lock",
    "_ACondition": "_Condition",
}

async_files = [
    "./pymongo/_async/" + f
    for f in listdir("./pymongo/_async")
    if isfile(join("./pymongo/_async", f))
]

unasync_files(
    async_files,
    [
        Rule(
            fromdir="/pymongo/_async/",
            todir="/pymongo/_sync/",
            additional_replacements=replacements,
        )
    ],
)

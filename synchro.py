from os.path import isfile, join

from unasync import unasync_files, Rule
import os

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
    "_async": "_sync",
    "anext": "next",
    "_ALock": "_Lock",
    "_ACondition": "_Condition"
}

async_files = ["./pymongo/_async/" + f for f in os.listdir("./pymongo/_async") if isfile(join("./pymongo/_async", f))]

unasync_files(async_files, [Rule(fromdir="/pymongo/_async/", todir="/pymongo/_sync/", additional_replacements=replacements)])
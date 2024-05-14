from __future__ import annotations

import re
from os import listdir
from pathlib import Path

from unasync import Rule, unasync_files  # type: ignore[import]

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
    "AsyncContextManager": "ContextManager",
}

_pymongo_base = "./pymongo/asynchronous/"
_gridfs_base = "./gridfs/asynchronous/"

_pymongo_dest_base = "./pymongo/synchronous/"
_gridfs_dest_base = "./gridfs/synchronous/"


async_files = [
    _pymongo_base + f for f in listdir(_pymongo_base) if (Path(_pymongo_base) / f).is_file()
]

gridfs_files = [
    _gridfs_base + f for f in listdir(_gridfs_base) if (Path(_gridfs_base) / f).is_file()
]

sync_files = [
    _pymongo_dest_base + f
    for f in listdir(_pymongo_dest_base)
    if (Path(_pymongo_dest_base) / f).is_file()
]

sync_gridfs_files = [
    _gridfs_dest_base + f
    for f in listdir(_gridfs_dest_base)
    if (Path(_gridfs_dest_base) / f).is_file()
]

docstring_translate_files = [
    _pymongo_dest_base + f
    for f in [
        "aggregation.py",
        "change_stream.py",
        "collection.py",
        "command_cursor.py",
        "cursor.py",
        "client_options.py",
        "client_session.py",
        "database.py",
        "encryption.py",
        "encryption_options.py",
        "mongo_client.py",
        "network.py",
        "operations.py",
        "pool.py",
        "topology.py",
    ]
]


def process_files(files: list[str]) -> None:
    for file in files:
        if "__init__" not in file:
            with open(file, "r+") as f:
                lines = f.readlines()
                lines = apply_is_sync(lines)
                lines = translate_coroutine_types(lines)
                lines = remove_async_sleeps(lines)
                if file in docstring_translate_files:
                    lines = translate_docstrings(lines)
                f.seek(0)
                f.writelines(lines)
                f.truncate()


def apply_is_sync(lines: list[str]) -> list[str]:
    is_sync = next(iter([line for line in lines if line.startswith("IS_SYNC = ")]))
    index = lines.index(is_sync)
    is_sync = is_sync.replace("False", "True")
    lines[index] = is_sync
    return lines


def translate_coroutine_types(lines: list[str]) -> list[str]:
    coroutine_types = [line for line in lines if "Coroutine[" in line]
    for type in coroutine_types:
        res = re.search(r"Coroutine\[([A-z]+), ([A-z]+), ([A-z]+)\]", type)
        if res:
            old = res[0]
            index = lines.index(type)
            new = type.replace(old, res.group(3))
            lines[index] = new
    return lines


def remove_async_sleeps(lines: list[str]) -> list[str]:
    sleeps = [line for line in lines if "asyncio.sleep(0)" in line]
    return [line for line in lines if line not in sleeps]


def translate_docstrings(lines: list[str]) -> list[str]:
    for i in range(len(lines)):
        for k in replacements:
            if k in lines[i]:
                # This sequence of replacements fixes the grammar issues caused by translating async -> sync
                if "an Async" in lines[i]:
                    lines[i] = lines[i].replace("an Async", "a Async")
                if "An Async" in lines[i]:
                    lines[i] = lines[i].replace("An Async", "A Async")
                if "an asynchronous" in lines[i]:
                    lines[i] = lines[i].replace("an asynchronous", "a")
                if "An asynchronous" in lines[i]:
                    lines[i] = lines[i].replace("An asynchronous", "A")
                lines[i] = lines[i].replace(k, replacements[k])
            if "Sync" in lines[i] and replacements[k] in lines[i]:
                lines[i] = lines[i].replace("Sync", "")

    return lines


def unasync_directory(files: list[str], src: str, dest: str, replacements: dict[str, str]) -> None:
    unasync_files(
        files,
        [
            Rule(
                fromdir=src,
                todir=dest,
                additional_replacements=replacements,
            )
        ],
    )


def main() -> None:
    unasync_directory(async_files, _pymongo_base, _pymongo_dest_base, replacements)
    unasync_directory(gridfs_files, _gridfs_base, _gridfs_dest_base, replacements)
    process_files(sync_files + sync_gridfs_files)


if __name__ == "__main__":
    main()
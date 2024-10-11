# Copyright 2024-Present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Synchronization of asynchronous modules.

Used as part of our build system to generate synchronous code.
"""

from __future__ import annotations

import re
from os import listdir
from pathlib import Path

from unasync import Rule, unasync_files  # type: ignore[import-not-found]

replacements = {
    "AsyncCollection": "Collection",
    "AsyncDatabase": "Database",
    "AsyncCursor": "Cursor",
    "AsyncMongoClient": "MongoClient",
    "AsyncCommandCursor": "CommandCursor",
    "AsyncRawBatchCursor": "RawBatchCursor",
    "AsyncRawBatchCommandCursor": "RawBatchCommandCursor",
    "AsyncClientSession": "ClientSession",
    "AsyncChangeStream": "ChangeStream",
    "AsyncCollectionChangeStream": "CollectionChangeStream",
    "AsyncDatabaseChangeStream": "DatabaseChangeStream",
    "AsyncClusterChangeStream": "ClusterChangeStream",
    "_AsyncBulk": "_Bulk",
    "_AsyncClientBulk": "_ClientBulk",
    "AsyncConnection": "Connection",
    "async_command": "command",
    "async_receive_message": "receive_message",
    "async_receive_data": "receive_data",
    "async_sendall": "sendall",
    "asynchronous": "synchronous",
    "Asynchronous": "Synchronous",
    "AsyncBulkTestBase": "BulkTestBase",
    "AsyncBulkAuthorizationTestBase": "BulkAuthorizationTestBase",
    "anext": "next",
    "aiter": "iter",
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
    "AsyncClientEncryption": "ClientEncryption",
    "AsyncMongoCryptCallback": "MongoCryptCallback",
    "AsyncExplicitEncrypter": "ExplicitEncrypter",
    "AsyncAutoEncrypter": "AutoEncrypter",
    "AsyncContextManager": "ContextManager",
    "AsyncClientContext": "ClientContext",
    "AsyncTestCollection": "TestCollection",
    "AsyncIntegrationTest": "IntegrationTest",
    "AsyncPyMongoTestCase": "PyMongoTestCase",
    "AsyncMockClientTest": "MockClientTest",
    "async_client_context": "client_context",
    "async_setup": "setup",
    "asyncSetUp": "setUp",
    "asyncTearDown": "tearDown",
    "async_teardown": "teardown",
    "pytest_asyncio": "pytest",
    "async_wait_until": "wait_until",
    "addAsyncCleanup": "addCleanup",
    "async_setup_class": "setup_class",
    "IsolatedAsyncioTestCase": "TestCase",
    "AsyncUnitTest": "UnitTest",
    "AsyncMockClient": "MockClient",
    "AsyncSpecRunner": "SpecRunner",
    "AsyncTransactionsBase": "TransactionsBase",
    "async_get_pool": "get_pool",
    "async_is_mongos": "is_mongos",
    "async_rs_or_single_client": "rs_or_single_client",
    "async_rs_or_single_client_noauth": "rs_or_single_client_noauth",
    "async_rs_client": "rs_client",
    "async_single_client": "single_client",
    "async_from_client": "from_client",
    "aclosing": "closing",
    "asyncAssertRaisesExactly": "assertRaisesExactly",
    "get_async_mock_client": "get_mock_client",
    "aconnect": "_connect",
    "async-transactions-ref": "transactions-ref",
    "async-snapshot-reads-ref": "snapshot-reads-ref",
    "default_async": "default",
    "aclose": "close",
    "PyMongo|async": "PyMongo",
    "PyMongo|c|async": "PyMongo|c",
    "AsyncTestGridFile": "TestGridFile",
    "AsyncTestGridFileNoConnect": "TestGridFileNoConnect",
    "AsyncTestSpec": "TestSpec",
    "AsyncSpecTestCreator": "SpecTestCreator",
    "async_set_fail_point": "set_fail_point",
    "async_ensure_all_connected": "ensure_all_connected",
    "async_repl_set_step_down": "repl_set_step_down",
}

docstring_replacements: dict[tuple[str, str], str] = {
    ("MongoClient", "connect"): """If ``True`` (the default), immediately
            begin connecting to MongoDB in the background. Otherwise connect
            on the first operation.  The default value is ``False`` when
            running in a Function-as-a-service environment.""",
    ("Collection", "create"): """If ``True``, force collection
            creation even without options being set.""",
    ("Collection", "session"): """A
            :class:`~pymongo.client_session.ClientSession` that is used with
            the create collection command.""",
    ("Collection", "kwargs"): """Additional keyword arguments will
            be passed as options for the create collection command.""",
}

docstring_removals: set[str] = {
    ".. warning:: This API is currently in beta, meaning the classes, methods, and behaviors described within may change before the full release."
}

type_replacements = {"_Condition": "threading.Condition"}

import_replacements = {"test.synchronous": "test"}

_pymongo_base = "./pymongo/asynchronous/"
_gridfs_base = "./gridfs/asynchronous/"
_test_base = "./test/asynchronous/"

_pymongo_dest_base = "./pymongo/synchronous/"
_gridfs_dest_base = "./gridfs/synchronous/"
_test_dest_base = "./test/"


async_files = [
    _pymongo_base + f for f in listdir(_pymongo_base) if (Path(_pymongo_base) / f).is_file()
]

gridfs_files = [
    _gridfs_base + f for f in listdir(_gridfs_base) if (Path(_gridfs_base) / f).is_file()
]


def async_only_test(f: str) -> bool:
    """Return True for async tests that should not be converted to sync."""
    return f in ["test_locks.py", "test_concurrency.py"]


test_files = [
    _test_base + f
    for f in listdir(_test_base)
    if (Path(_test_base) / f).is_file() and not async_only_test(f)
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

# Add each asynchronized test here as part of the converting PR
converted_tests = [
    "__init__.py",
    "conftest.py",
    "helpers.py",
    "pymongo_mocks.py",
    "utils_spec_runner.py",
    "qcheck.py",
    "test_auth.py",
    "test_auth_spec.py",
    "test_bulk.py",
    "test_change_stream.py",
    "test_client.py",
    "test_client_bulk_write.py",
    "test_client_context.py",
    "test_collation.py",
    "test_collection.py",
    "test_common.py",
    "test_connections_survive_primary_stepdown_spec.py",
    "test_crud_unified.py",
    "test_cursor.py",
    "test_database.py",
    "test_encryption.py",
    "test_grid_file.py",
    "test_logger.py",
    "test_monitoring.py",
    "test_raw_bson.py",
    "test_retryable_reads.py",
    "test_retryable_writes.py",
    "test_session.py",
    "test_transactions.py",
    "unified_format.py",
]

sync_test_files = [
    _test_dest_base + f for f in converted_tests if (Path(_test_dest_base) / f).is_file()
]


docstring_translate_files = sync_files + sync_gridfs_files + sync_test_files


def process_files(files: list[str]) -> None:
    for file in files:
        if "__init__" not in file or "__init__" and "test" in file:
            with open(file, "r+") as f:
                lines = f.readlines()
                lines = apply_is_sync(lines, file)
                lines = translate_coroutine_types(lines)
                lines = translate_async_sleeps(lines)
                if file in docstring_translate_files:
                    lines = translate_docstrings(lines)
                translate_locks(lines)
                translate_types(lines)
                if file in sync_test_files:
                    translate_imports(lines)
                f.seek(0)
                f.writelines(lines)
                f.truncate()


def apply_is_sync(lines: list[str], file: str) -> list[str]:
    try:
        is_sync = next(iter([line for line in lines if line.startswith("_IS_SYNC = ")]))
        index = lines.index(is_sync)
        is_sync = is_sync.replace("False", "True")
        lines[index] = is_sync
    except StopIteration as e:
        print(
            f"Missing _IS_SYNC at top of async file {file.replace('synchronous', 'asynchronous')}"
        )
        raise e
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


def translate_locks(lines: list[str]) -> list[str]:
    lock_lines = [line for line in lines if "_Lock(" in line]
    cond_lines = [line for line in lines if "_Condition(" in line]
    for line in lock_lines:
        res = re.search(r"_Lock\(([^()]*\([^()]*\))\)", line)
        if res:
            old = res[0]
            index = lines.index(line)
            lines[index] = line.replace(old, res[1])
    for line in cond_lines:
        res = re.search(r"_Condition\(([^()]*\([^()]*\))\)", line)
        if res:
            old = res[0]
            index = lines.index(line)
            lines[index] = line.replace(old, res[1])

    return lines


def translate_types(lines: list[str]) -> list[str]:
    for k, v in type_replacements.items():
        matches = [line for line in lines if k in line and "import" not in line]
        for line in matches:
            index = lines.index(line)
            lines[index] = line.replace(k, v)
    return lines


def translate_imports(lines: list[str]) -> list[str]:
    for k, v in import_replacements.items():
        matches = [line for line in lines if k in line and "import" in line]
        for line in matches:
            index = lines.index(line)
            lines[index] = line.replace(k, v)
    return lines


def translate_async_sleeps(lines: list[str]) -> list[str]:
    blocking_sleeps = [line for line in lines if "asyncio.sleep(0)" in line]
    lines = [line for line in lines if line not in blocking_sleeps]
    sleeps = [line for line in lines if "asyncio.sleep" in line]

    for line in sleeps:
        res = re.search(r"asyncio.sleep\(([^()]*)\)", line)
        if res:
            old = res[0]
            index = lines.index(line)
            new = f"time.sleep({res[1]})"
            lines[index] = line.replace(old, new)

    return lines


def translate_docstrings(lines: list[str]) -> list[str]:
    for i in range(len(lines)):
        for k in replacements:
            if k in lines[i]:
                # This sequence of replacements fixes the grammar issues caused by translating async -> sync
                if "an Async" in lines[i]:
                    lines[i] = lines[i].replace("an Async", "a Async")
                if "an 'Async" in lines[i]:
                    lines[i] = lines[i].replace("an 'Async", "a 'Async")
                if "An Async" in lines[i]:
                    lines[i] = lines[i].replace("An Async", "A Async")
                if "An 'Async" in lines[i]:
                    lines[i] = lines[i].replace("An 'Async", "A 'Async")
                if "an asynchronous" in lines[i]:
                    lines[i] = lines[i].replace("an asynchronous", "a")
                if "An asynchronous" in lines[i]:
                    lines[i] = lines[i].replace("An asynchronous", "A")
                # This ensures docstring links are for `pymongo.X` instead of `pymongo.synchronous.X`
                if "pymongo.asynchronous" in lines[i] and "import" not in lines[i]:
                    lines[i] = lines[i].replace("pymongo.asynchronous", "pymongo")
                lines[i] = lines[i].replace(k, replacements[k])
            if "Sync" in lines[i] and "Synchronous" not in lines[i] and replacements[k] in lines[i]:
                lines[i] = lines[i].replace("Sync", "")
                if "rsApplyStop" in lines[i]:
                    lines[i] = lines[i].replace("rsApplyStop", "rsSyncApplyStop")
        if "async for" in lines[i] or "async with" in lines[i] or "async def" in lines[i]:
            lines[i] = lines[i].replace("async ", "")
        if "await " in lines[i] and "tailable" not in lines[i]:
            lines[i] = lines[i].replace("await ", "")
    for i in range(len(lines)):
        for k in docstring_replacements:  # type: ignore[assignment]
            if f":param {k[1]}: **Not supported by {k[0]}**." in lines[i]:
                lines[i] = lines[i].replace(
                    f"**Not supported by {k[0]}**.",
                    docstring_replacements[k],  # type: ignore[index]
                )

        for line in docstring_removals:
            if line in lines[i]:
                lines[i] = "DOCSTRING_REMOVED"
                lines[i + 1] = "DOCSTRING_REMOVED"

    return [line for line in lines if line != "DOCSTRING_REMOVED"]


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
    unasync_directory(test_files, _test_base, _test_dest_base, replacements)
    process_files(sync_files + sync_gridfs_files + sync_test_files)


if __name__ == "__main__":
    main()

from __future__ import annotations

import inspect
import sys

from pymongo import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.cursor import AsyncCursor
from pymongo.asynchronous.database import AsyncDatabase

replacements = {
    "Collection": "AsyncCollection",
    "Database": "AsyncDatabase",
    "Cursor": "AsyncCursor",
    "MongoClient": "AsyncMongoClient",
    "CommandCursor": "AsyncCommandCursor",
    "RawBatchCursor": "AsyncRawBatchCursor",
    "RawBatchCommandCursor": "AsyncRawBatchCommandCursor",
    "ClientSession": "AsyncClientSession",
    "ChangeStream": "AsyncChangeStream",
    "CollectionChangeStream": "AsyncCollectionChangeStream",
    "DatabaseChangeStream": "AsyncDatabaseChangeStream",
    "ClusterChangeStream": "AsyncClusterChangeStream",
    "_Bulk": "_AsyncBulk",
    "_ClientBulk": "_AsyncClientBulk",
    "Connection": "AsyncConnection",
    "synchronous": "asynchronous",
    "Synchronous": "Asynchronous",
    "next": "await anext",
    "_Lock": "_ALock",
    "_Condition": "_ACondition",
    "GridFS": "AsyncGridFS",
    "GridFSBucket": "AsyncGridFSBucket",
    "GridIn": "AsyncGridIn",
    "GridOut": "AsyncGridOut",
    "GridOutCursor": "AsyncGridOutCursor",
    "GridOutIterator": "AsyncGridOutIterator",
    "GridOutChunkIterator": "_AsyncGridOutChunkIterator",
    "_grid_in_property": "_a_grid_in_property",
    "_grid_out_property": "_a_grid_out_property",
    "ClientEncryption": "AsyncClientEncryption",
    "MongoCryptCallback": "AsyncMongoCryptCallback",
    "ExplicitEncrypter": "AsyncExplicitEncrypter",
    "AutoEncrypter": "AsyncAutoEncrypter",
    "ContextManager": "AsyncContextManager",
    "ClientContext": "AsyncClientContext",
    "TestCollection": "AsyncTestCollection",
    "IntegrationTest": "AsyncIntegrationTest",
    "PyMongoTestCase": "AsyncPyMongoTestCase",
    "MockClientTest": "AsyncMockClientTest",
    "client_context": "async_client_context",
    "setUp": "asyncSetUp",
    "tearDown": "asyncTearDown",
    "wait_until": "await async_wait_until",
    "addCleanup": "addAsyncCleanup",
    "TestCase": "IsolatedAsyncioTestCase",
    "UnitTest": "AsyncUnitTest",
    "MockClient": "AsyncMockClient",
    "SpecRunner": "AsyncSpecRunner",
    "TransactionsBase": "AsyncTransactionsBase",
    "get_pool": "await async_get_pool",
    "is_mongos": "await async_is_mongos",
    "rs_or_single_client": "await async_rs_or_single_client",
    "rs_or_single_client_noauth": "await async_rs_or_single_client_noauth",
    "rs_client": "await async_rs_client",
    "single_client": "await async_single_client",
    "from_client": "await async_from_client",
    "closing": "aclosing",
    "assertRaisesExactly": "asyncAssertRaisesExactly",
    "get_mock_client": "await get_async_mock_client",
    "close": "await aclose",
}

async_classes = [AsyncMongoClient, AsyncDatabase, AsyncCollection, AsyncCursor, AsyncCommandCursor]


def get_async_methods() -> set[str]:
    result: set[str] = set()
    for x in async_classes:
        methods = {
            k
            for k, v in vars(x).items()
            if callable(v)
            and not isinstance(v, classmethod)
            and inspect.iscoroutinefunction(v)
            and v.__name__[0] != "_"
        }
        result = result | methods
    return result


async_methods = get_async_methods()


def apply_replacements(lines: list[str]) -> list[str]:
    for i in range(len(lines)):
        if "_IS_SYNC = True" in lines[i]:
            lines[i] = "_IS_SYNC = False"
        if "def test" in lines[i]:
            lines[i] = lines[i].replace("def test", "async def test")
        for k in replacements:
            if k in lines[i]:
                lines[i] = lines[i].replace(k, replacements[k])
        for k in async_methods:
            if k + "(" in lines[i]:
                tokens = lines[i].split(" ")
                for j in range(len(tokens)):
                    if k + "(" in tokens[j]:
                        if j < 2:
                            tokens.insert(0, "await")
                        else:
                            tokens.insert(j, "await")
                        break
                new_line = " ".join(tokens)

                lines[i] = new_line

    return lines


def process_file(input_file: str, output_file: str) -> None:
    with open(input_file, "r+") as f:
        lines = f.readlines()
        lines = apply_replacements(lines)

        with open(output_file, "w+") as f2:
            f2.seek(0)
            f2.writelines(lines)
            f2.truncate()


def main() -> None:
    args = sys.argv[1:]
    sync_file = "./test/" + args[0]
    async_file = "./" + args[0]

    process_file(sync_file, async_file)


main()

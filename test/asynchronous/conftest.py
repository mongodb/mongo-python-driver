from __future__ import annotations

import asyncio
import sys

import pymongo
from typing_extensions import Any

from pymongo import AsyncMongoClient
from pymongo.uri_parser import parse_uri

from test import pytest_conf, db_user, db_pwd
from test.asynchronous import async_setup, async_teardown, _connection_string, AsyncClientContext

import pytest
import pytest_asyncio

from test.utils import FunctionCallRecorder

_IS_SYNC = False


@pytest.fixture(scope="session")
def event_loop_policy():
    # The default asyncio loop implementation on Windows
    # has issues with sharing sockets across loops (https://github.com/python/cpython/issues/122240)
    # We explicitly use a different loop implementation here to prevent that issue
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]

    return asyncio.get_event_loop_policy()

@pytest_asyncio.fixture(loop_scope="session")
async def async_client_context_fixture():
    client = AsyncClientContext()
    await client.init()
    yield client
    await client.client.close()

@pytest_asyncio.fixture(loop_scope="session", autouse=True)
async def test_setup_and_teardown():
    await async_setup()
    yield
    await async_teardown()

async def _async_mongo_client(
        async_client_context, host, port, authenticate=True, directConnection=None, **kwargs
    ):
        """Create a new client over SSL/TLS if necessary."""
        host = host or await async_client_context.host
        port = port or await async_client_context.port
        client_options: dict = async_client_context.default_client_options.copy()
        if async_client_context.replica_set_name and not directConnection:
            client_options["replicaSet"] = async_client_context.replica_set_name
        if directConnection is not None:
            client_options["directConnection"] = directConnection
        client_options.update(kwargs)

        uri = _connection_string(host)
        auth_mech = kwargs.get("authMechanism", "")
        if async_client_context.auth_enabled and authenticate and auth_mech != "MONGODB-OIDC":
            # Only add the default username or password if one is not provided.
            res = parse_uri(uri)
            if (
                not res["username"]
                and not res["password"]
                and "username" not in client_options
                and "password" not in client_options
            ):
                client_options["username"] = db_user
                client_options["password"] = db_pwd
        client = AsyncMongoClient(uri, port, **client_options)
        if client._options.connect:
            await client.aconnect()
        return client


async def async_single_client_noauth(
    async_client_context, h: Any = None, p: Any = None, **kwargs: Any
) -> AsyncMongoClient[dict]:
    """Make a direct connection. Don't authenticate."""
    return await _async_mongo_client(async_client_context, h, p, authenticate=False, directConnection=True, **kwargs)
#
async def async_single_client(
    async_client_context, h: Any = None, p: Any = None, **kwargs: Any
) -> AsyncMongoClient[dict]:
    """Make a direct connection, and authenticate if necessary."""
    return await _async_mongo_client(async_client_context, h, p, directConnection=True, **kwargs)

# @pytest_asyncio.fixture(loop_scope="function")
# async def async_rs_client_noauth(
#     async_client_context, h: Any = None, p: Any = None, **kwargs: Any
# ) -> AsyncMongoClient[dict]:
#     """Connect to the replica set. Don't authenticate."""
#     return await _async_mongo_client(async_client_context, h, p, authenticate=False, **kwargs)
#
# @pytest_asyncio.fixture(loop_scope="function")
# async def async_rs_client(
#     async_client_context, h: Any = None, p: Any = None, **kwargs: Any
# ) -> AsyncMongoClient[dict]:
#     """Connect to the replica set and authenticate if necessary."""
#     return await _async_mongo_client(async_client_context, h, p, **kwargs)
#
# @pytest_asyncio.fixture(loop_scope="function")
# async def async_rs_or_single_client_noauth(
#     async_client_context, h: Any = None, p: Any = None, **kwargs: Any
# ) -> AsyncMongoClient[dict]:
#     """Connect to the replica set if there is one, otherwise the standalone.
#
#     Like rs_or_single_client, but does not authenticate.
#     """
#     return await _async_mongo_client(async_client_context, h, p, authenticate=False, **kwargs)

async def async_rs_or_single_client(
    async_client_context, h: Any = None, p: Any = None, **kwargs: Any
) -> AsyncMongoClient[Any]:
    """Connect to the replica set if there is one, otherwise the standalone.

    Authenticates if necessary.
    """
    return await _async_mongo_client(async_client_context, h, p, **kwargs)

def simple_client(h: Any = None, p: Any = None, **kwargs: Any) -> AsyncMongoClient:
    if not h and not p:
        client = AsyncMongoClient(**kwargs)
    else:
        client = AsyncMongoClient(h, p, **kwargs)
    return client

@pytest.fixture(scope="function")
def patch_resolver():
    from pymongo.srv_resolver import _resolve

    patched_resolver = FunctionCallRecorder(_resolve)
    pymongo.srv_resolver._resolve = patched_resolver
    yield patched_resolver
    pymongo.srv_resolver._resolve = _resolve



pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

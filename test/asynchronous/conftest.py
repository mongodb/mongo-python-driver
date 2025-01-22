from __future__ import annotations

import asyncio
import sys
from typing import Callable

import pymongo
from typing_extensions import Any

from pymongo import AsyncMongoClient
from pymongo.uri_parser import parse_uri

from test import pytest_conf, db_user, db_pwd, MONGODB_API_VERSION
from test.asynchronous import async_setup, async_teardown, _connection_string, AsyncClientContext

import pytest
import pytest_asyncio

from test.asynchronous.pymongo_mocks import AsyncMockClient
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

@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def async_client_context_fixture():
    client = AsyncClientContext()
    await client.init()
    yield client
    await client.client.close()

@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def test_environment(async_client_context_fixture):
    requirements = {}
    requirements["SUPPORT_TRANSACTIONS"] = async_client_context_fixture.supports_transactions()
    requirements["IS_DATA_LAKE"] = async_client_context_fixture.is_data_lake
    requirements["IS_SYNC"] = _IS_SYNC
    requirements["IS_SYNC"] = _IS_SYNC
    requirements["REQUIRE_API_VERSION"] = MONGODB_API_VERSION
    requirements["SUPPORTS_FAILCOMMAND_FAIL_POINT"] = async_client_context_fixture.supports_failCommand_fail_point
    requirements["IS_NOT_MMAP"] = async_client_context_fixture.is_not_mmap
    requirements["SERVER_VERSION"] = async_client_context_fixture.version
    requirements["AUTH_ENABLED"] = async_client_context_fixture.auth_enabled
    requirements["FIPS_ENABLED"] = async_client_context_fixture.fips_enabled
    requirements["IS_RS"] = async_client_context_fixture.is_rs
    requirements["MONGOSES"] = len(async_client_context_fixture.mongoses)
    requirements["SECONDARIES_COUNT"] = await async_client_context_fixture.secondaries_count
    requirements["SECONDARY_READ_PREF"] = await async_client_context_fixture.supports_secondary_read_pref
    requirements["HAS_IPV6"] = async_client_context_fixture.has_ipv6
    requirements["IS_SERVERLESS"] = async_client_context_fixture.serverless
    requirements["IS_LOAD_BALANCER"] = async_client_context_fixture.load_balancer
    requirements["TEST_COMMANDS_ENABLED"] = async_client_context_fixture.test_commands_enabled
    requirements["IS_TLS"] = async_client_context_fixture.tls
    requirements["IS_TLS_CERT"] = async_client_context_fixture.tlsCertificateKeyFile
    requirements["SERVER_IS_RESOLVEABLE"] = async_client_context_fixture.server_is_resolvable
    requirements["SESSIONS_ENABLED"] = async_client_context_fixture.sessions_enabled
    requirements["SUPPORTS_RETRYABLE_WRITES"] = async_client_context_fixture.supports_retryable_writes()
    yield requirements


@pytest_asyncio.fixture
async def require_auth(test_environment):
    if not test_environment["AUTH_ENABLED"]:
        pytest.skip("Authentication is not enabled on the server")

@pytest_asyncio.fixture
async def require_no_fips(test_environment):
    if test_environment["FIPS_ENABLED"]:
        pytest.skip("Test cannot run on a FIPS-enabled host")

@pytest_asyncio.fixture
async def require_no_tls(test_environment):
    if test_environment["IS_TLS"]:
        pytest.skip("Must be able to connect without TLS")

@pytest_asyncio.fixture
async def require_ipv6(test_environment):
    if not test_environment["HAS_IPV6"]:
        pytest.skip("No IPv6")

@pytest_asyncio.fixture
async def require_sync(test_environment):
    if not _IS_SYNC:
        pytest.skip("This test only works with the synchronous API")

@pytest_asyncio.fixture
async def require_no_mongos(test_environment):
    if test_environment["MONGOSES"]:
        pytest.skip("Must be connected to a mongod, not a mongos")

@pytest_asyncio.fixture
async def require_no_replica_set(test_environment):
    if test_environment["IS_RS"]:
        pytest.skip("Connected to a replica set, not a standalone mongod")

@pytest_asyncio.fixture
async def require_replica_set(test_environment):
    if not test_environment["IS_RS"]:
        pytest.skip("Not connected to a replica set")

@pytest_asyncio.fixture
async def require_sdam(test_environment):
    if test_environment["IS_SERVERLESS"] or test_environment["IS_LOAD_BALANCER"]:
        pytest.skip("loadBalanced and serverless clients do not run SDAM")

@pytest_asyncio.fixture
async def require_failCommand_fail_point(test_environment):
    if not test_environment["SUPPORTS_FAILCOMMAND_FAIL_POINT"]:
        pytest.skip("failCommand fail point must be supported")


@pytest_asyncio.fixture(loop_scope="session", scope="session", autouse=True)
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


@pytest_asyncio.fixture(loop_scope="session")
async def async_single_client_noauth(async_client_context_fixture) -> Callable[..., AsyncMongoClient]:
    """Make a direct connection. Don't authenticate."""
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = await _async_mongo_client(async_client_context_fixture, h, p, authenticate=False, directConnection=True, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()

@pytest_asyncio.fixture(loop_scope="session")
async def async_single_client(async_client_context_fixture) -> Callable[..., AsyncMongoClient]:
    """Make a direct connection, and authenticate if necessary."""
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = await _async_mongo_client(async_client_context_fixture, h, p, directConnection=True, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()

@pytest_asyncio.fixture(loop_scope="session")
async def async_rs_client_noauth(async_client_context_fixture) -> Callable[..., AsyncMongoClient]:
    """Connect to the replica set. Don't authenticate."""
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = await _async_mongo_client(async_client_context_fixture, h, p, authenticate=False, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()


@pytest_asyncio.fixture(loop_scope="session")
async def async_rs_client(async_client_context_fixture) -> Callable[..., AsyncMongoClient]:
    """Connect to the replica set and authenticate if necessary."""
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = await _async_mongo_client(async_client_context_fixture, h, p, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()


@pytest_asyncio.fixture(loop_scope="session")
async def async_rs_or_single_client_noauth(async_client_context_fixture) -> Callable[..., AsyncMongoClient]:
    """Connect to the replica set if there is one, otherwise the standalone.

    Like rs_or_single_client, but does not authenticate.
    """
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = await _async_mongo_client(async_client_context_fixture, h, p, authenticate=False, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()

@pytest_asyncio.fixture(loop_scope="session")
async def async_rs_or_single_client(async_client_context_fixture) -> Callable[..., AsyncMongoClient]:
    """Connect to the replica set if there is one, otherwise the standalone.

    Authenticates if necessary.
    """
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = await _async_mongo_client(async_client_context_fixture, h, p, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()


@pytest_asyncio.fixture(loop_scope="session")
async def simple_client() -> Callable[..., AsyncMongoClient]:
    clients = []
    async def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        if not h and not p:
            client = AsyncMongoClient(**kwargs)
        else:
            client = AsyncMongoClient(h, p, **kwargs)
        clients.append(client)
        return client
    yield _make_client
    for client in clients:
        await client.close()

@pytest.fixture(scope="function")
def patch_resolver():
    from pymongo.srv_resolver import _resolve

    patched_resolver = FunctionCallRecorder(_resolve)
    pymongo.srv_resolver._resolve = patched_resolver
    yield patched_resolver
    pymongo.srv_resolver._resolve = _resolve

@pytest_asyncio.fixture(loop_scope="session")
async def async_mock_client():
        clients = []

        async def _make_client(standalones,
        members,
        mongoses,
        hello_hosts=None,
        arbiters=None,
        down_hosts=None,
        *args,
        **kwargs):
            client = await AsyncMockClient.get_async_mock_client(standalones, members, mongoses, hello_hosts, arbiters, down_hosts, *args, **kwargs)
            clients.append(client)
            return client
        yield _make_client
        for client in clients:
            await client.close()

pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

from __future__ import annotations

import asyncio
import sys
from test import (
    MONGODB_API_VERSION,
    ClientContext,
    _connection_string,
    db_pwd,
    db_user,
    pytest_conf,
    setup,
    teardown,
)
from test.pymongo_mocks import MockClient
from test.utils import FunctionCallRecorder
from typing import Any, Callable

import pytest

import pymongo
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

_IS_SYNC = True


@pytest.fixture(scope="session")
def event_loop_policy():
    # The default asyncio loop implementation on Windows
    # has issues with sharing sockets across loops (https://github.com/python/cpython/issues/122240)
    # We explicitly use a different loop implementation here to prevent that issue
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]

    return asyncio.get_event_loop_policy()


@pytest.fixture(scope="session")
def client_context_fixture():
    client = ClientContext()
    client.init()
    yield client
    if client.client is not None:
        if not client.is_data_lake:
            client.client.drop_database("pymongo-pooling-tests")
            client.client.drop_database("pymongo_test")
            client.client.drop_database("pymongo_test1")
            client.client.drop_database("pymongo_test2")
            client.client.drop_database("pymongo_test_mike")
            client.client.drop_database("pymongo_test_bernie")
        client.client.close()


@pytest.fixture
def require_integration(client_context_fixture):
    if not client_context_fixture.connected:
        pytest.fail("Integration tests require a MongoDB server")


@pytest.fixture(scope="session")
def test_environment(client_context_fixture):
    requirements = {}
    requirements["SUPPORT_TRANSACTIONS"] = client_context_fixture.supports_transactions()
    requirements["IS_DATA_LAKE"] = client_context_fixture.is_data_lake
    requirements["IS_SYNC"] = _IS_SYNC
    requirements["IS_SYNC"] = _IS_SYNC
    requirements["REQUIRE_API_VERSION"] = MONGODB_API_VERSION
    requirements[
        "SUPPORTS_FAILCOMMAND_FAIL_POINT"
    ] = client_context_fixture.supports_failCommand_fail_point
    requirements["IS_NOT_MMAP"] = client_context_fixture.is_not_mmap
    requirements["SERVER_VERSION"] = client_context_fixture.version
    requirements["AUTH_ENABLED"] = client_context_fixture.auth_enabled
    requirements["FIPS_ENABLED"] = client_context_fixture.fips_enabled
    requirements["IS_RS"] = client_context_fixture.is_rs
    requirements["MONGOSES"] = len(client_context_fixture.mongoses)
    requirements["SECONDARIES_COUNT"] = client_context_fixture.secondaries_count
    requirements["SECONDARY_READ_PREF"] = client_context_fixture.supports_secondary_read_pref
    requirements["HAS_IPV6"] = client_context_fixture.has_ipv6
    requirements["IS_SERVERLESS"] = client_context_fixture.serverless
    requirements["IS_LOAD_BALANCER"] = client_context_fixture.load_balancer
    requirements["TEST_COMMANDS_ENABLED"] = client_context_fixture.test_commands_enabled
    requirements["IS_TLS"] = client_context_fixture.tls
    requirements["IS_TLS_CERT"] = client_context_fixture.tlsCertificateKeyFile
    requirements["SERVER_IS_RESOLVEABLE"] = client_context_fixture.server_is_resolvable
    requirements["SESSIONS_ENABLED"] = client_context_fixture.sessions_enabled
    requirements["SUPPORTS_RETRYABLE_WRITES"] = client_context_fixture.supports_retryable_writes()
    yield requirements


@pytest.fixture
def require_auth(test_environment):
    if not test_environment["AUTH_ENABLED"]:
        pytest.skip("Authentication is not enabled on the server")


@pytest.fixture
def require_no_fips(test_environment):
    if test_environment["FIPS_ENABLED"]:
        pytest.skip("Test cannot run on a FIPS-enabled host")


@pytest.fixture
def require_no_tls(test_environment):
    if test_environment["IS_TLS"]:
        pytest.skip("Must be able to connect without TLS")


@pytest.fixture
def require_ipv6(test_environment):
    if not test_environment["HAS_IPV6"]:
        pytest.skip("No IPv6")


@pytest.fixture
def require_sync(test_environment):
    if not _IS_SYNC:
        pytest.skip("This test only works with the synchronous API")


@pytest.fixture
def require_no_mongos(test_environment):
    if test_environment["MONGOSES"]:
        pytest.skip("Must be connected to a mongod, not a mongos")


@pytest.fixture
def require_no_replica_set(test_environment):
    if test_environment["IS_RS"]:
        pytest.skip("Connected to a replica set, not a standalone mongod")


@pytest.fixture
def require_replica_set(test_environment):
    if not test_environment["IS_RS"]:
        pytest.skip("Not connected to a replica set")


@pytest.fixture
def require_sdam(test_environment):
    if test_environment["IS_SERVERLESS"] or test_environment["IS_LOAD_BALANCER"]:
        pytest.skip("loadBalanced and serverless clients do not run SDAM")


@pytest.fixture
def require_no_load_balancer(test_environment):
    if test_environment["IS_LOAD_BALANCER"]:
        pytest.skip("Must not be connected to a load balancer")


@pytest.fixture
def require_failCommand_fail_point(test_environment):
    if not test_environment["SUPPORTS_FAILCOMMAND_FAIL_POINT"]:
        pytest.skip("failCommand fail point must be supported")


@pytest.fixture(scope="session", autouse=True)
def test_setup_and_teardown():
    setup()
    yield
    teardown()


def _async_mongo_client(
    client_context_fixture, host, port, authenticate=True, directConnection=None, **kwargs
):
    """Create a new client over SSL/TLS if necessary."""
    host = host or client_context_fixture.host
    port = port or client_context_fixture.port
    client_options: dict = client_context_fixture.default_client_options.copy()
    if client_context_fixture.replica_set_name and not directConnection:
        client_options["replicaSet"] = client_context_fixture.replica_set_name
    if directConnection is not None:
        client_options["directConnection"] = directConnection
    client_options.update(kwargs)

    uri = _connection_string(host)
    auth_mech = kwargs.get("authMechanism", "")
    if client_context_fixture.auth_enabled and authenticate and auth_mech != "MONGODB-OIDC":
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
    client = MongoClient(uri, port, **client_options)
    if client._options.connect:
        client._connect()
    return client


@pytest.fixture()
def single_client_noauth(client_context_fixture) -> Callable[..., MongoClient]:
    """Make a direct connection. Don't authenticate."""
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = _async_mongo_client(
            client_context_fixture, h, p, authenticate=False, directConnection=True, **kwargs
        )
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def single_client(client_context_fixture) -> Callable[..., MongoClient]:
    """Make a direct connection, and authenticate if necessary."""
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = _async_mongo_client(client_context_fixture, h, p, directConnection=True, **kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def rs_client_noauth(client_context_fixture) -> Callable[..., MongoClient]:
    """Connect to the replica set. Don't authenticate."""
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = _async_mongo_client(client_context_fixture, h, p, authenticate=False, **kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def rs_client(client_context_fixture) -> Callable[..., MongoClient]:
    """Connect to the replica set and authenticate if necessary."""
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = _async_mongo_client(client_context_fixture, h, p, **kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def rs_or_single_client_noauth(client_context_fixture) -> Callable[..., MongoClient]:
    """Connect to the replica set if there is one, otherwise the standalone.

    Like rs_or_single_client, but does not authenticate.
    """
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = _async_mongo_client(client_context_fixture, h, p, authenticate=False, **kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def rs_or_single_client(client_context_fixture) -> Callable[..., MongoClient]:
    """Connect to the replica set if there is one, otherwise the standalone.

    Authenticates if necessary.
    """
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        client = _async_mongo_client(client_context_fixture, h, p, **kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def simple_client() -> Callable[..., MongoClient]:
    clients = []

    def _make_client(h: Any = None, p: Any = None, **kwargs: Any):
        if not h and not p:
            client = MongoClient(**kwargs)
        else:
            client = MongoClient(h, p, **kwargs)
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture(scope="function")
def patch_resolver():
    from pymongo.srv_resolver import _resolve

    patched_resolver = FunctionCallRecorder(_resolve)
    pymongo.srv_resolver._resolve = patched_resolver
    yield patched_resolver
    pymongo.srv_resolver._resolve = _resolve


@pytest.fixture()
def mock_client():
    clients = []

    def _make_client(
        standalones,
        members,
        mongoses,
        hello_hosts=None,
        arbiters=None,
        down_hosts=None,
        *args,
        **kwargs,
    ):
        client = MockClient.get_mock_client(
            standalones, members, mongoses, hello_hosts, arbiters, down_hosts, *args, **kwargs
        )
        clients.append(client)
        return client

    yield _make_client
    for client in clients:
        client.close()


@pytest.fixture()
def remove_all_users_fixture(client_context_fixture, request):
    db_name = request.param
    yield
    client_context_fixture.client[db_name].command(
        "dropAllUsersFromDatabase", 1, writeConcern={"w": client_context_fixture.w}
    )


@pytest.fixture()
def drop_user_fixture(client_context_fixture, request):
    db, user = request.param
    yield
    client_context_fixture.drop_user(db, user)


@pytest.fixture()
def drop_database_fixture(client_context_fixture, request):
    db = request.param
    yield
    client_context_fixture.client.drop_database(db)


pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

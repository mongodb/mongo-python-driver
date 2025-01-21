# Copyright 2013-present MongoDB, Inc.
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

"""Test the mongo_client module."""
from __future__ import annotations

import _thread as thread
import asyncio
import base64
import contextlib
import copy
import datetime
import gc
import logging
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import uuid
from typing import Any, Iterable, Type, no_type_check
from unittest import mock
from unittest.mock import patch

import pytest
import pytest_asyncio
from pymongo.lock import _async_create_lock

from bson.binary import CSHARP_LEGACY, JAVA_LEGACY, PYTHON_LEGACY, Binary, UuidRepresentation
from pymongo.operations import _Op
from test.asynchronous.conftest import async_rs_or_single_client, simple_client, async_single_client

sys.path[0:0] = [""]

from test.asynchronous import (
    HAVE_IPADDRESS,
    AsyncIntegrationTest,
    AsyncMockClientTest,
    AsyncUnitTest,
    SkipTest,
    client_knobs,
    connected,
    db_pwd,
    db_user,
    remove_all_users,
    unittest, AsyncClientContext,
)
from test.asynchronous.pymongo_mocks import AsyncMockClient
from test.test_binary import BinaryData
from test.utils import (
    NTHREADS,
    CMAPListener,
    FunctionCallRecorder,
    async_get_pool,
    async_wait_until,
    asyncAssertRaisesExactly,
    delay,
    gevent_monkey_patched,
    is_greenthread_patched,
    lazy_client_trial,
    one,
)

import bson
import pymongo
from bson import encode
from bson.codec_options import (
    CodecOptions,
    DatetimeConversion,
    TypeEncoder,
    TypeRegistry,
)
from bson.son import SON
from bson.tz_util import utc
from pymongo import event_loggers, message, monitoring
from pymongo.asynchronous.command_cursor import AsyncCommandCursor
from pymongo.asynchronous.cursor import AsyncCursor, CursorType
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.asynchronous.helpers import anext
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.asynchronous.pool import (
    AsyncConnection,
)
from pymongo.asynchronous.settings import TOPOLOGY_TYPE
from pymongo.asynchronous.topology import _ErrorContext
from pymongo.client_options import ClientOptions
from pymongo.common import _UUID_REPRESENTATIONS, CONNECT_TIMEOUT, MIN_SUPPORTED_WIRE_VERSION, has_c
from pymongo.compression_support import _have_snappy, _have_zstd
from pymongo.driver_info import DriverInfo
from pymongo.errors import (
    AutoReconnect,
    ConfigurationError,
    ConnectionFailure,
    InvalidName,
    InvalidOperation,
    InvalidURI,
    NetworkTimeout,
    OperationFailure,
    ServerSelectionTimeoutError,
    WriteConcernError,
)
from pymongo.monitoring import ServerHeartbeatListener, ServerHeartbeatStartedEvent
from pymongo.pool_options import _MAX_METADATA_SIZE, _METADATA, ENV_VAR_K8S, PoolOptions
from pymongo.read_preferences import ReadPreference
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import readable_server_selector, writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.topology_description import TopologyDescription
from pymongo.write_concern import WriteConcern

_IS_SYNC = False

pytestmark = pytest.mark.asyncio(loop_scope="session")

@pytest_asyncio.fixture(loop_scope="session")
async def async_client(async_client_context_fixture) -> AsyncMongoClient:
    client = await async_rs_or_single_client(async_client_context_fixture,
        connect=False, serverSelectionTimeoutMS=100
    )
    yield client
    await client.close()


async def test_keyword_arg_defaults():
    client = simple_client(
        socketTimeoutMS=None,
        connectTimeoutMS=20000,
        waitQueueTimeoutMS=None,
        replicaSet=None,
        read_preference=ReadPreference.PRIMARY,
        ssl=False,
        tlsCertificateKeyFile=None,
        tlsAllowInvalidCertificates=True,
        tlsCAFile=None,
        connect=False,
        serverSelectionTimeoutMS=12000,
    )

    options = client.options
    pool_opts = options.pool_options
    assert pool_opts.socket_timeout is None
    # socket.Socket.settimeout takes a float in seconds
    assert 20.0 == pool_opts.connect_timeout
    assert pool_opts.wait_queue_timeout is None
    assert pool_opts._ssl_context is None
    assert options.replica_set_name is None
    assert client.read_preference == ReadPreference.PRIMARY
    assert pytest.approx(client.options.server_selection_timeout, rel=1e-9) == 12

async def test_connect_timeout():
    client = simple_client(connect=False, connectTimeoutMS=None, socketTimeoutMS=None)
    pool_opts = client.options.pool_options
    assert pool_opts.socket_timeout is None
    assert pool_opts.connect_timeout is None

    client = simple_client(connect=False, connectTimeoutMS=0, socketTimeoutMS=0)
    pool_opts = client.options.pool_options
    assert pool_opts.socket_timeout is None
    assert pool_opts.connect_timeout is None

    client = simple_client(
        "mongodb://localhost/?connectTimeoutMS=0&socketTimeoutMS=0", connect=False
    )
    pool_opts = client.options.pool_options
    assert pool_opts.socket_timeout is None
    assert pool_opts.connect_timeout is None

async def test_types():
    with pytest.raises(TypeError):
        AsyncMongoClient(1)
    with pytest.raises(TypeError):
        AsyncMongoClient(1.14)
    with pytest.raises(TypeError):
        AsyncMongoClient("localhost", "27017")
    with pytest.raises(TypeError):
        AsyncMongoClient("localhost", 1.14)
    with pytest.raises(TypeError):
        AsyncMongoClient("localhost", [])

    with pytest.raises(ConfigurationError):
        AsyncMongoClient([])

async def test_max_pool_size_zero():
    simple_client(maxPoolSize=0)

async def test_uri_detection():
    with pytest.raises(ConfigurationError):
        AsyncMongoClient("/foo")
    with pytest.raises(ConfigurationError):
        AsyncMongoClient("://")
    with pytest.raises(ConfigurationError):
        AsyncMongoClient("foo/")


async def test_get_db(async_client):
    def make_db(base, name):
        return base[name]

    with pytest.raises(InvalidName):
        make_db(async_client, "")
    with pytest.raises(InvalidName):
        make_db(async_client, "te$t")
    with pytest.raises(InvalidName):
        make_db(async_client, "te.t")
    with pytest.raises(InvalidName):
        make_db(async_client, "te\\t")
    with pytest.raises(InvalidName):
        make_db(async_client, "te/t")
    with pytest.raises(InvalidName):
        make_db(async_client, "te st")
    # Type and equality assertions
    assert isinstance(async_client.test, AsyncDatabase)
    assert async_client.test == async_client["test"]
    assert async_client.test == AsyncDatabase(async_client, "test")

async def test_get_database(async_client):
    codec_options = CodecOptions(tz_aware=True)
    write_concern = WriteConcern(w=2, j=True)
    db = async_client.get_database("foo", codec_options, ReadPreference.SECONDARY, write_concern)
    assert db.name == "foo"
    assert db.codec_options == codec_options
    assert db.read_preference == ReadPreference.SECONDARY
    assert db.write_concern == write_concern

async def test_getattr(async_client):
    assert isinstance(async_client["_does_not_exist"], AsyncDatabase)

    with pytest.raises(AttributeError) as context:
        async_client.client._does_not_exist

    # Message should be:
    # "AttributeError: AsyncMongoClient has no attribute '_does_not_exist'. To
    # access the _does_not_exist database, use client['_does_not_exist']".
    assert "has no attribute '_does_not_exist'" in str(context.value)


async def test_iteration(async_client):
    if _IS_SYNC:
        msg = "'AsyncMongoClient' object is not iterable"
    else:
        msg = "'AsyncMongoClient' object is not an async iterator"

    with pytest.raises(TypeError, match="'AsyncMongoClient' object is not iterable"):
        for _ in async_client:
            break

    # Index fails
    with pytest.raises(TypeError):
        _ = async_client[0]

    # 'next' function fails
    with pytest.raises(TypeError, match=msg):
        _ = await anext(async_client)

    # 'next()' method fails
    with pytest.raises(TypeError, match="'AsyncMongoClient' object is not iterable"):
        _ = await async_client.anext()

    # Do not implement typing.Iterable
    assert not isinstance(async_client, Iterable)



async def test_get_default_database(async_client_context_fixture):
    c = await async_rs_or_single_client(async_client_context_fixture,
        "mongodb://%s:%d/foo"
        % (await async_client_context_fixture.host, await async_client_context_fixture.port),
        connect=False,
    )
    assert AsyncDatabase(c, "foo") == c.get_default_database()
    # Test that default doesn't override the URI value.
    assert AsyncDatabase(c, "foo") == c.get_default_database("bar")
    codec_options = CodecOptions(tz_aware=True)
    write_concern = WriteConcern(w=2, j=True)
    db = c.get_default_database(None, codec_options, ReadPreference.SECONDARY, write_concern)
    assert "foo" == db.name
    assert codec_options == db.codec_options
    assert ReadPreference.SECONDARY == db.read_preference
    assert write_concern == db.write_concern

    c = await async_rs_or_single_client(async_client_context_fixture,
        "mongodb://%s:%d/" % (await async_client_context_fixture.host, await async_client_context_fixture.port),
        connect=False,
    )
    assert AsyncDatabase(c, "foo") == c.get_default_database("foo")


async def test_get_default_database_error(async_client_context_fixture):
    # URI with no database.
    c = await async_rs_or_single_client(async_client_context_fixture,
        "mongodb://%s:%d/" % (await async_client_context_fixture.host, await async_client_context_fixture.port),
        connect=False,
    )
    with pytest.raises(ConfigurationError):
        c.get_default_database()

async def test_get_default_database_with_authsource(async_client_context_fixture):
    # Ensure we distinguish database name from authSource.
    uri = "mongodb://%s:%d/foo?authSource=src" % (
        await async_client_context_fixture.host,
        await async_client_context_fixture.port,
    )
    c = await async_rs_or_single_client(async_client_context_fixture, uri, connect=False)
    assert (AsyncDatabase(c, "foo") == c.get_default_database())

async def test_get_database_default(async_client_context_fixture):
    c = await async_rs_or_single_client(async_client_context_fixture,
        "mongodb://%s:%d/foo"
        % (await async_client_context_fixture.host, await async_client_context_fixture.port),
        connect=False,
    )
    assert AsyncDatabase(c, "foo") == c.get_database()

async def test_get_database_default_error(async_client_context_fixture):
    # URI with no database.
    c = await async_rs_or_single_client(async_client_context_fixture,
        "mongodb://%s:%d/" % (await async_client_context_fixture.host, await async_client_context_fixture.port),
        connect=False,
    )
    with pytest.raises(ConfigurationError):
        c.get_database()

async def test_get_database_default_with_authsource(async_client_context_fixture):
    # Ensure we distinguish database name from authSource.
    uri = "mongodb://%s:%d/foo?authSource=src" % (
        await async_client_context_fixture.host,
        await async_client_context_fixture.port,
    )
    c = await async_rs_or_single_client(async_client_context_fixture, uri, connect=False)
    assert AsyncDatabase(c, "foo") == c.get_database()

async def test_primary_read_pref_with_tags(async_client_context_fixture):
    # No tags allowed with "primary".
    with pytest.raises(ConfigurationError):
        async with await async_single_client(async_client_context_fixture, "mongodb://host/?readpreferencetags=dc:east"):
            pass
    with pytest.raises(ConfigurationError):
        async with await async_single_client(async_client_context_fixture,
            "mongodb://host/?readpreference=primary&readpreferencetags=dc:east"
        ):
            pass

async def test_read_preference(async_client_context_fixture):
    c = await async_rs_or_single_client(async_client_context_fixture,
        "mongodb://host", connect=False, readpreference=ReadPreference.NEAREST.mongos_mode
    )
    assert c.read_preference == ReadPreference.NEAREST

# async def test_metadata():
#     metadata = copy.deepcopy(_METADATA)
#     if has_c():
#         metadata["driver"]["name"] = "PyMongo|c|async"
#     else:
#         metadata["driver"]["name"] = "PyMongo|async"
#     metadata["application"] = {"name": "foobar"}
#     client = self.simple_client("mongodb://foo:27017/?appname=foobar&connect=false")
#     options = client.options
#     self.assertEqual(options.pool_options.metadata, metadata)
#     client = self.simple_client("foo", 27017, appname="foobar", connect=False)
#     options = client.options
#     self.assertEqual(options.pool_options.metadata, metadata)
#     # No error
#     self.simple_client(appname="x" * 128)
#     with self.assertRaises(ValueError):
#         self.simple_client(appname="x" * 129)
#     # Bad "driver" options.
#     self.assertRaises(TypeError, DriverInfo, "Foo", 1, "a")
#     self.assertRaises(TypeError, DriverInfo, version="1", platform="a")
#     self.assertRaises(TypeError, DriverInfo)
#     with self.assertRaises(TypeError):
#         self.simple_client(driver=1)
#     with self.assertRaises(TypeError):
#         self.simple_client(driver="abc")
#     with self.assertRaises(TypeError):
#         self.simple_client(driver=("Foo", "1", "a"))
#     # Test appending to driver info.
#     if has_c():
#         metadata["driver"]["name"] = "PyMongo|c|async|FooDriver"
#     else:
#         metadata["driver"]["name"] = "PyMongo|async|FooDriver"
#     metadata["driver"]["version"] = "{}|1.2.3".format(_METADATA["driver"]["version"])
#     client = self.simple_client(
#         "foo",
#         27017,
#         appname="foobar",
#         driver=DriverInfo("FooDriver", "1.2.3", None),
#         connect=False,
#     )
#     options = client.options
#     self.assertEqual(options.pool_options.metadata, metadata)
#     metadata["platform"] = "{}|FooPlatform".format(_METADATA["platform"])
#     client = self.simple_client(
#         "foo",
#         27017,
#         appname="foobar",
#         driver=DriverInfo("FooDriver", "1.2.3", "FooPlatform"),
#         connect=False,
#     )
#     options = client.options
#     self.assertEqual(options.pool_options.metadata, metadata)
#     # Test truncating driver info metadata.
#     client = self.simple_client(
#         driver=DriverInfo(name="s" * _MAX_METADATA_SIZE),
#         connect=False,
#     )
#     options = client.options
#     self.assertLessEqual(
#         len(bson.encode(options.pool_options.metadata)),
#         _MAX_METADATA_SIZE,
#     )
#     client = self.simple_client(
#         driver=DriverInfo(name="s" * _MAX_METADATA_SIZE, version="s" * _MAX_METADATA_SIZE),
#         connect=False,
#     )
#     options = client.options
#     self.assertLessEqual(
#         len(bson.encode(options.pool_options.metadata)),
#         _MAX_METADATA_SIZE,
#     )
#
# @mock.patch.dict("os.environ", {ENV_VAR_K8S: "1"})
# def test_container_metadata(self):
#     metadata = copy.deepcopy(_METADATA)
#     metadata["driver"]["name"] = "PyMongo|async"
#     metadata["env"] = {}
#     metadata["env"]["container"] = {"orchestrator": "kubernetes"}
#     client = self.simple_client("mongodb://foo:27017/?appname=foobar&connect=false")
#     options = client.options
#     self.assertEqual(options.pool_options.metadata["env"], metadata["env"])
    #
    # async def test_kwargs_codec_options(self):
    #     class MyFloatType:
    #         def __init__(self, x):
    #             self.__x = x
    #
    #         @property
    #         def x(self):
    #             return self.__x
    #
    #     class MyFloatAsIntEncoder(TypeEncoder):
    #         python_type = MyFloatType
    #
    #         def transform_python(self, value):
    #             return int(value)
    #
    #     # Ensure codec options are passed in correctly
    #     document_class: Type[SON] = SON
    #     type_registry = TypeRegistry([MyFloatAsIntEncoder()])
    #     tz_aware = True
    #     uuid_representation_label = "javaLegacy"
    #     unicode_decode_error_handler = "ignore"
    #     tzinfo = utc
    #     c = self.simple_client(
    #         document_class=document_class,
    #         type_registry=type_registry,
    #         tz_aware=tz_aware,
    #         uuidrepresentation=uuid_representation_label,
    #         unicode_decode_error_handler=unicode_decode_error_handler,
    #         tzinfo=tzinfo,
    #         connect=False,
    #     )
    #     self.assertEqual(c.codec_options.document_class, document_class)
    #     self.assertEqual(c.codec_options.type_registry, type_registry)
    #     self.assertEqual(c.codec_options.tz_aware, tz_aware)
    #     self.assertEqual(
    #         c.codec_options.uuid_representation,
    #         _UUID_REPRESENTATIONS[uuid_representation_label],
    #     )
    #     self.assertEqual(c.codec_options.unicode_decode_error_handler, unicode_decode_error_handler)
    #     self.assertEqual(c.codec_options.tzinfo, tzinfo)
    #
    # async def test_uri_codec_options(self):
    #     # Ensure codec options are passed in correctly
    #     uuid_representation_label = "javaLegacy"
    #     unicode_decode_error_handler = "ignore"
    #     datetime_conversion = "DATETIME_CLAMP"
    #     uri = (
    #         "mongodb://%s:%d/foo?tz_aware=true&uuidrepresentation="
    #         "%s&unicode_decode_error_handler=%s"
    #         "&datetime_conversion=%s"
    #         % (
    #             await async_client_context.host,
    #             await async_client_context.port,
    #             uuid_representation_label,
    #             unicode_decode_error_handler,
    #             datetime_conversion,
    #         )
    #     )
    #     c = self.simple_client(uri, connect=False)
    #     self.assertEqual(c.codec_options.tz_aware, True)
    #     self.assertEqual(
    #         c.codec_options.uuid_representation,
    #         _UUID_REPRESENTATIONS[uuid_representation_label],
    #     )
    #     self.assertEqual(c.codec_options.unicode_decode_error_handler, unicode_decode_error_handler)
    #     self.assertEqual(
    #         c.codec_options.datetime_conversion, DatetimeConversion[datetime_conversion]
    #     )
    #
    #     # Change the passed datetime_conversion to a number and re-assert.
    #     uri = uri.replace(datetime_conversion, f"{int(DatetimeConversion[datetime_conversion])}")
    #     c = self.simple_client(uri, connect=False)
    #     self.assertEqual(
    #         c.codec_options.datetime_conversion, DatetimeConversion[datetime_conversion]
    #     )
    #
    # async def test_uri_option_precedence(self):
    #     # Ensure kwarg options override connection string options.
    #     uri = "mongodb://localhost/?ssl=true&replicaSet=name&readPreference=primary"
    #     c = self.simple_client(
    #         uri, ssl=False, replicaSet="newname", readPreference="secondaryPreferred"
    #     )
    #     clopts = c.options
    #     opts = clopts._options
    #
    #     self.assertEqual(opts["tls"], False)
    #     self.assertEqual(clopts.replica_set_name, "newname")
    #     self.assertEqual(clopts.read_preference, ReadPreference.SECONDARY_PREFERRED)
    #
    # async def test_connection_timeout_ms_propagates_to_DNS_resolver(self):
    #     # Patch the resolver.
    #     from pymongo.srv_resolver import _resolve
    #
    #     patched_resolver = FunctionCallRecorder(_resolve)
    #     pymongo.srv_resolver._resolve = patched_resolver
    #
    #     def reset_resolver():
    #         pymongo.srv_resolver._resolve = _resolve
    #
    #     self.addCleanup(reset_resolver)
    #
    #     # Setup.
    #     base_uri = "mongodb+srv://test5.test.build.10gen.cc"
    #     connectTimeoutMS = 5000
    #     expected_kw_value = 5.0
    #     uri_with_timeout = base_uri + "/?connectTimeoutMS=6000"
    #     expected_uri_value = 6.0
    #
    #     async def test_scenario(args, kwargs, expected_value):
    #         patched_resolver.reset()
    #         self.simple_client(*args, **kwargs)
    #         for _, kw in patched_resolver.call_list():
    #             self.assertAlmostEqual(kw["lifetime"], expected_value)
    #
    #     # No timeout specified.
    #     await test_scenario((base_uri,), {}, CONNECT_TIMEOUT)
    #
    #     # Timeout only specified in connection string.
    #     await test_scenario((uri_with_timeout,), {}, expected_uri_value)
    #
    #     # Timeout only specified in keyword arguments.
    #     kwarg = {"connectTimeoutMS": connectTimeoutMS}
    #     await test_scenario((base_uri,), kwarg, expected_kw_value)
    #
    #     # Timeout specified in both kwargs and connection string.
    #     await test_scenario((uri_with_timeout,), kwarg, expected_kw_value)
    #
    # async def test_uri_security_options(self):
    #     # Ensure that we don't silently override security-related options.
    #     with self.assertRaises(InvalidURI):
    #         self.simple_client("mongodb://localhost/?ssl=true", tls=False, connect=False)
    #
    #     # Matching SSL and TLS options should not cause errors.
    #     c = self.simple_client("mongodb://localhost/?ssl=false", tls=False, connect=False)
    #     self.assertEqual(c.options._options["tls"], False)
    #
    #     # Conflicting tlsInsecure options should raise an error.
    #     with self.assertRaises(InvalidURI):
    #         self.simple_client(
    #             "mongodb://localhost/?tlsInsecure=true",
    #             connect=False,
    #             tlsAllowInvalidHostnames=True,
    #         )
    #
    #     # Conflicting legacy tlsInsecure options should also raise an error.
    #     with self.assertRaises(InvalidURI):
    #         self.simple_client(
    #             "mongodb://localhost/?tlsInsecure=true",
    #             connect=False,
    #             tlsAllowInvalidCertificates=False,
    #         )
    #
    #     # Conflicting kwargs should raise InvalidURI
    #     with self.assertRaises(InvalidURI):
    #         self.simple_client(ssl=True, tls=False)
    #
    # async def test_event_listeners(self):
    #     c = self.simple_client(event_listeners=[], connect=False)
    #     self.assertEqual(c.options.event_listeners, [])
    #     listeners = [
    #         event_loggers.CommandLogger(),
    #         event_loggers.HeartbeatLogger(),
    #         event_loggers.ServerLogger(),
    #         event_loggers.TopologyLogger(),
    #         event_loggers.ConnectionPoolLogger(),
    #     ]
    #     c = self.simple_client(event_listeners=listeners, connect=False)
    #     self.assertEqual(c.options.event_listeners, listeners)
    #
    # async def test_client_options(self):
    #     c = self.simple_client(connect=False)
    #     self.assertIsInstance(c.options, ClientOptions)
    #     self.assertIsInstance(c.options.pool_options, PoolOptions)
    #     self.assertEqual(c.options.server_selection_timeout, 30)
    #     self.assertEqual(c.options.pool_options.max_idle_time_seconds, None)
    #     self.assertIsInstance(c.options.retry_writes, bool)
    #     self.assertIsInstance(c.options.retry_reads, bool)
    #
    # def test_validate_suggestion(self):
    #     """Validate kwargs in constructor."""
    #     for typo in ["auth", "Auth", "AUTH"]:
    #         expected = f"Unknown option: {typo}. Did you mean one of (authsource, authmechanism, authoidcallowedhosts) or maybe a camelCase version of one? Refer to docstring."
    #         expected = re.escape(expected)
    #         with self.assertRaisesRegex(ConfigurationError, expected):
    #             AsyncMongoClient(**{typo: "standard"})  # type: ignore[arg-type]
    #
    # @patch("pymongo.srv_resolver._SrvResolver.get_hosts")
    # def test_detected_environment_logging(self, mock_get_hosts):
    #     normal_hosts = [
    #         "normal.host.com",
    #         "host.cosmos.azure.com",
    #         "host.docdb.amazonaws.com",
    #         "host.docdb-elastic.amazonaws.com",
    #     ]
    #     srv_hosts = ["mongodb+srv://<test>:<test>@" + s for s in normal_hosts]
    #     multi_host = (
    #         "host.cosmos.azure.com,host.docdb.amazonaws.com,host.docdb-elastic.amazonaws.com"
    #     )
    #     with self.assertLogs("pymongo", level="INFO") as cm:
    #         for host in normal_hosts:
    #             AsyncMongoClient(host, connect=False)
    #         for host in srv_hosts:
    #             mock_get_hosts.return_value = [(host, 1)]
    #             AsyncMongoClient(host, connect=False)
    #         AsyncMongoClient(multi_host, connect=False)
    #         logs = [record.getMessage() for record in cm.records if record.name == "pymongo.client"]
    #         self.assertEqual(len(logs), 7)
    #
    # @patch("pymongo.srv_resolver._SrvResolver.get_hosts")
    # async def test_detected_environment_warning(self, mock_get_hosts):
    #     with self._caplog.at_level(logging.WARN):
    #         normal_hosts = [
    #             "host.cosmos.azure.com",
    #             "host.docdb.amazonaws.com",
    #             "host.docdb-elastic.amazonaws.com",
    #         ]
    #         srv_hosts = ["mongodb+srv://<test>:<test>@" + s for s in normal_hosts]
    #         multi_host = (
    #             "host.cosmos.azure.com,host.docdb.amazonaws.com,host.docdb-elastic.amazonaws.com"
    #         )
    #         for host in normal_hosts:
    #             with self.assertWarns(UserWarning):
    #                 self.simple_client(host)
    #         for host in srv_hosts:
    #             mock_get_hosts.return_value = [(host, 1)]
    #             with self.assertWarns(UserWarning):
    #                 self.simple_client(host)
    #         with self.assertWarns(UserWarning):
    #             self.simple_client(multi_host)

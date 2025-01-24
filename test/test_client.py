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

from bson.binary import CSHARP_LEGACY, JAVA_LEGACY, PYTHON_LEGACY, Binary, UuidRepresentation
from pymongo.operations import _Op

sys.path[0:0] = [""]

from test import (
    HAVE_IPADDRESS,
    PyMongoTestCasePyTest,
    SkipTest,
    client_knobs,
    connected,
    db_pwd,
    db_user,
)
from test.test_binary import BinaryData
from test.utils import (
    NTHREADS,
    CMAPListener,
    _default_pytest_mark,
    assertRaisesExactly,
    delay,
    get_pool,
    gevent_monkey_patched,
    is_greenthread_patched,
    lazy_client_trial,
    one,
    wait_until,
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
from pymongo.synchronous.command_cursor import CommandCursor
from pymongo.synchronous.cursor import Cursor, CursorType
from pymongo.synchronous.database import Database
from pymongo.synchronous.helpers import next
from pymongo.synchronous.mongo_client import MongoClient
from pymongo.synchronous.pool import (
    Connection,
)
from pymongo.synchronous.settings import TOPOLOGY_TYPE
from pymongo.synchronous.topology import _ErrorContext
from pymongo.topology_description import TopologyDescription
from pymongo.write_concern import WriteConcern

_IS_SYNC = True


pytestmark = _default_pytest_mark(_IS_SYNC)


@pytest.mark.unit
class TestClientUnitTest:
    @pytest.fixture()
    def client(self, rs_or_single_client) -> MongoClient:
        client = rs_or_single_client(connect=False, serverSelectionTimeoutMS=100)
        yield client
        client.close()

    def test_keyword_arg_defaults(self, simple_client):
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

    def test_connect_timeout(self, simple_client):
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

    def test_types(self):
        with pytest.raises(TypeError):
            MongoClient(1)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            MongoClient(1.14)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            MongoClient("localhost", "27017")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            MongoClient("localhost", 1.14)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            MongoClient("localhost", [])  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError):
            MongoClient([])

    def test_max_pool_size_zero(self, simple_client):
        simple_client(maxPoolSize=0)

    def test_uri_detection(self):
        with pytest.raises(ConfigurationError):
            MongoClient("/foo")
        with pytest.raises(ConfigurationError):
            MongoClient("://")
        with pytest.raises(ConfigurationError):
            MongoClient("foo/")

    def test_get_db(self, client):
        def make_db(base, name):
            return base[name]

        with pytest.raises(InvalidName):
            make_db(client, "")
        with pytest.raises(InvalidName):
            make_db(client, "te$t")
        with pytest.raises(InvalidName):
            make_db(client, "te.t")
        with pytest.raises(InvalidName):
            make_db(client, "te\\t")
        with pytest.raises(InvalidName):
            make_db(client, "te/t")
        with pytest.raises(InvalidName):
            make_db(client, "te st")
        # Type and equality assertions
        assert isinstance(client.test, Database)
        assert client.test == client["test"]
        assert client.test == Database(client, "test")

    def test_get_database(self, client):
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        db = client.get_database("foo", codec_options, ReadPreference.SECONDARY, write_concern)
        assert db.name == "foo"
        assert db.codec_options == codec_options
        assert db.read_preference == ReadPreference.SECONDARY
        assert db.write_concern == write_concern

    def test_getattr(self, client):
        assert isinstance(client["_does_not_exist"], Database)

        with pytest.raises(AttributeError) as context:
            client.client._does_not_exist

        # Message should be:
        # "AttributeError: MongoClient has no attribute '_does_not_exist'. To
        # access the _does_not_exist database, use client['_does_not_exist']".
        assert "has no attribute '_does_not_exist'" in str(context.value)

    def test_iteration(self, client):
        if _IS_SYNC or sys.version_info < (3, 10):
            msg = "'MongoClient' object is not iterable"
        else:
            msg = "'MongoClient' object is not an async iterator"

        with pytest.raises(TypeError, match="'MongoClient' object is not iterable"):
            for _ in client:
                break

        # Index fails
        with pytest.raises(TypeError):
            _ = client[0]

        # 'next' function fails
        with pytest.raises(TypeError, match=msg):
            _ = next(client)

        # 'next()' method fails
        with pytest.raises(TypeError, match="'MongoClient' object is not iterable"):
            _ = client.next()

        # Do not implement typing.Iterable
        assert not isinstance(client, Iterable)

    def test_get_default_database(self, rs_or_single_client, client_context_fixture):
        c = rs_or_single_client(
            "mongodb://%s:%d/foo" % (client_context_fixture.host, client_context_fixture.port),
            connect=False,
        )
        assert Database(c, "foo") == c.get_default_database()
        # Test that default doesn't override the URI value.
        assert Database(c, "foo") == c.get_default_database("bar")
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        db = c.get_default_database(None, codec_options, ReadPreference.SECONDARY, write_concern)
        assert "foo" == db.name
        assert codec_options == db.codec_options
        assert ReadPreference.SECONDARY == db.read_preference
        assert write_concern == db.write_concern

        c = rs_or_single_client(
            "mongodb://%s:%d/" % (client_context_fixture.host, client_context_fixture.port),
            connect=False,
        )
        assert Database(c, "foo") == c.get_default_database("foo")

    def test_get_default_database_error(self, rs_or_single_client, client_context_fixture):
        # URI with no database.
        c = rs_or_single_client(
            "mongodb://%s:%d/" % (client_context_fixture.host, client_context_fixture.port),
            connect=False,
        )
        with pytest.raises(ConfigurationError):
            c.get_default_database()

    def test_get_default_database_with_authsource(
        self, client_context_fixture, rs_or_single_client
    ):
        # Ensure we distinguish database name from authSource.
        uri = "mongodb://%s:%d/foo?authSource=src" % (
            client_context_fixture.host,
            client_context_fixture.port,
        )
        c = rs_or_single_client(uri, connect=False)
        assert Database(c, "foo") == c.get_default_database()

    def test_get_database_default(self, client_context_fixture, rs_or_single_client):
        c = rs_or_single_client(
            "mongodb://%s:%d/foo" % (client_context_fixture.host, client_context_fixture.port),
            connect=False,
        )
        assert Database(c, "foo") == c.get_database()

    def test_get_database_default_error(self, client_context_fixture, rs_or_single_client):
        # URI with no database.
        c = rs_or_single_client(
            "mongodb://%s:%d/" % (client_context_fixture.host, client_context_fixture.port),
            connect=False,
        )
        with pytest.raises(ConfigurationError):
            c.get_database()

    def test_get_database_default_with_authsource(
        self, client_context_fixture, rs_or_single_client
    ):
        # Ensure we distinguish database name from authSource.
        uri = "mongodb://%s:%d/foo?authSource=src" % (
            client_context_fixture.host,
            client_context_fixture.port,
        )
        c = rs_or_single_client(uri, connect=False)
        assert Database(c, "foo") == c.get_database()

    def test_primary_read_pref_with_tags(self, single_client):
        # No tags allowed with "primary".
        with pytest.raises(ConfigurationError):
            with single_client("mongodb://host/?readpreferencetags=dc:east"):
                pass
        with pytest.raises(ConfigurationError):
            with single_client("mongodb://host/?readpreference=primary&readpreferencetags=dc:east"):
                pass

    def test_read_preference(self, client_context_fixture, rs_or_single_client):
        c = rs_or_single_client(
            "mongodb://host", connect=False, readpreference=ReadPreference.NEAREST.mongos_mode
        )
        assert c.read_preference == ReadPreference.NEAREST

    def test_metadata(self, simple_client):
        metadata = copy.deepcopy(_METADATA)
        if has_c():
            metadata["driver"]["name"] = "PyMongo|c"
        else:
            metadata["driver"]["name"] = "PyMongo"
        metadata["application"] = {"name": "foobar"}

        client = simple_client("mongodb://foo:27017/?appname=foobar&connect=false")
        options = client.options
        assert options.pool_options.metadata == metadata

        client = simple_client("foo", 27017, appname="foobar", connect=False)
        options = client.options
        assert options.pool_options.metadata == metadata

        # No error
        simple_client(appname="x" * 128)
        with pytest.raises(ValueError):
            simple_client(appname="x" * 129)

            # Bad "driver" options.
        with pytest.raises(TypeError):
            DriverInfo("Foo", 1, "a")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            DriverInfo(version="1", platform="a")  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            DriverInfo()  # type: ignore[call-arg]
        with pytest.raises(TypeError):
            simple_client(driver=1)
        with pytest.raises(TypeError):
            simple_client(driver="abc")
        with pytest.raises(TypeError):
            simple_client(driver=("Foo", "1", "a"))

            # Test appending to driver info.
        if has_c():
            metadata["driver"]["name"] = "PyMongo|c|FooDriver"
        else:
            metadata["driver"]["name"] = "PyMongo|FooDriver"
        metadata["driver"]["version"] = "{}|1.2.3".format(_METADATA["driver"]["version"])

        client = simple_client(
            "foo",
            27017,
            appname="foobar",
            driver=DriverInfo("FooDriver", "1.2.3", None),
            connect=False,
        )
        options = client.options
        assert options.pool_options.metadata == metadata

        metadata["platform"] = "{}|FooPlatform".format(_METADATA["platform"])
        client = simple_client(
            "foo",
            27017,
            appname="foobar",
            driver=DriverInfo("FooDriver", "1.2.3", "FooPlatform"),
            connect=False,
        )
        options = client.options
        assert options.pool_options.metadata == metadata

        # Test truncating driver info metadata.
        client = simple_client(
            driver=DriverInfo(name="s" * _MAX_METADATA_SIZE),
            connect=False,
        )
        options = client.options
        assert len(bson.encode(options.pool_options.metadata)) <= _MAX_METADATA_SIZE

        client = simple_client(
            driver=DriverInfo(name="s" * _MAX_METADATA_SIZE, version="s" * _MAX_METADATA_SIZE),
            connect=False,
        )
        options = client.options
        assert len(bson.encode(options.pool_options.metadata)) <= _MAX_METADATA_SIZE

    @mock.patch.dict("os.environ", {ENV_VAR_K8S: "1"})
    def test_container_metadata(self, simple_client):
        metadata = copy.deepcopy(_METADATA)
        metadata["driver"]["name"] = "PyMongo"
        metadata["env"] = {}
        metadata["env"]["container"] = {"orchestrator": "kubernetes"}

        client = simple_client("mongodb://foo:27017/?appname=foobar&connect=false")
        options = client.options
        assert options.pool_options.metadata["env"] == metadata["env"]

    def test_kwargs_codec_options(self, simple_client):
        class MyFloatType:
            def __init__(self, x):
                self.__x = x

            @property
            def x(self):
                return self.__x

        class MyFloatAsIntEncoder(TypeEncoder):
            python_type = MyFloatType

            def transform_python(self, value):
                return int(value)

        # Ensure codec options are passed in correctly
        document_class: Type[SON] = SON
        type_registry = TypeRegistry([MyFloatAsIntEncoder()])
        tz_aware = True
        uuid_representation_label = "javaLegacy"
        unicode_decode_error_handler = "ignore"
        tzinfo = utc
        c = simple_client(
            document_class=document_class,
            type_registry=type_registry,
            tz_aware=tz_aware,
            uuidrepresentation=uuid_representation_label,
            unicode_decode_error_handler=unicode_decode_error_handler,
            tzinfo=tzinfo,
            connect=False,
        )
        assert c.codec_options.document_class == document_class
        assert c.codec_options.type_registry == type_registry
        assert c.codec_options.tz_aware == tz_aware
        assert (
            c.codec_options.uuid_representation == _UUID_REPRESENTATIONS[uuid_representation_label]
        )
        assert c.codec_options.unicode_decode_error_handler == unicode_decode_error_handler
        assert c.codec_options.tzinfo == tzinfo

    def test_uri_codec_options(self, client_context_fixture, simple_client):
        uuid_representation_label = "javaLegacy"
        unicode_decode_error_handler = "ignore"
        datetime_conversion = "DATETIME_CLAMP"
        uri = (
            "mongodb://%s:%d/foo?tz_aware=true&uuidrepresentation="
            "%s&unicode_decode_error_handler=%s"
            "&datetime_conversion=%s"
            % (
                client_context_fixture.host,
                client_context_fixture.port,
                uuid_representation_label,
                unicode_decode_error_handler,
                datetime_conversion,
            )
        )
        c = simple_client(uri, connect=False)
        assert c.codec_options.tz_aware is True
        assert (
            c.codec_options.uuid_representation == _UUID_REPRESENTATIONS[uuid_representation_label]
        )
        assert c.codec_options.unicode_decode_error_handler == unicode_decode_error_handler
        assert c.codec_options.datetime_conversion == DatetimeConversion[datetime_conversion]
        # Change the passed datetime_conversion to a number and re-assert.
        uri = uri.replace(datetime_conversion, f"{int(DatetimeConversion[datetime_conversion])}")
        c = simple_client(uri, connect=False)
        assert c.codec_options.datetime_conversion == DatetimeConversion[datetime_conversion]

    def test_uri_option_precedence(self, simple_client):
        # Ensure kwarg options override connection string options.
        uri = "mongodb://localhost/?ssl=true&replicaSet=name&readPreference=primary"
        c = simple_client(uri, ssl=False, replicaSet="newname", readPreference="secondaryPreferred")
        clopts = c.options
        opts = clopts._options
        assert opts["tls"] is False
        assert clopts.replica_set_name == "newname"
        assert clopts.read_preference == ReadPreference.SECONDARY_PREFERRED

    def test_connection_timeout_ms_propagates_to_DNS_resolver(self, patch_resolver, simple_client):
        base_uri = "mongodb+srv://test5.test.build.10gen.cc"
        connectTimeoutMS = 5000
        expected_kw_value = 5.0
        uri_with_timeout = base_uri + "/?connectTimeoutMS=6000"
        expected_uri_value = 6.0

        def test_scenario(args, kwargs, expected_value):
            patch_resolver.reset()
            simple_client(*args, **kwargs)
            for _, kw in patch_resolver.call_list():
                assert pytest.approx(kw["lifetime"], rel=1e-6) == expected_value

        # No timeout specified.
        test_scenario((base_uri,), {}, CONNECT_TIMEOUT)

        # Timeout only specified in connection string.
        test_scenario((uri_with_timeout,), {}, expected_uri_value)

        # Timeout only specified in keyword arguments.
        kwarg = {"connectTimeoutMS": connectTimeoutMS}
        test_scenario((base_uri,), kwarg, expected_kw_value)

        # Timeout specified in both kwargs and connection string.
        test_scenario((uri_with_timeout,), kwarg, expected_kw_value)

    def test_uri_security_options(self, simple_client):
        # Ensure that we don't silently override security-related options.
        with pytest.raises(InvalidURI):
            simple_client("mongodb://localhost/?ssl=true", tls=False, connect=False)

        # Matching SSL and TLS options should not cause errors.
        c = simple_client("mongodb://localhost/?ssl=false", tls=False, connect=False)
        assert c.options._options["tls"] is False

        # Conflicting tlsInsecure options should raise an error.
        with pytest.raises(InvalidURI):
            simple_client(
                "mongodb://localhost/?tlsInsecure=true",
                connect=False,
                tlsAllowInvalidHostnames=True,
            )

        # Conflicting legacy tlsInsecure options should also raise an error.
        with pytest.raises(InvalidURI):
            simple_client(
                "mongodb://localhost/?tlsInsecure=true",
                connect=False,
                tlsAllowInvalidCertificates=False,
            )

        # Conflicting kwargs should raise InvalidURI
        with pytest.raises(InvalidURI):
            simple_client(ssl=True, tls=False)

    def test_event_listeners(self, simple_client):
        c = simple_client(event_listeners=[], connect=False)
        assert c.options.event_listeners == []
        listeners = [
            event_loggers.CommandLogger(),
            event_loggers.HeartbeatLogger(),
            event_loggers.ServerLogger(),
            event_loggers.TopologyLogger(),
            event_loggers.ConnectionPoolLogger(),
        ]
        c = simple_client(event_listeners=listeners, connect=False)
        assert c.options.event_listeners == listeners

    def test_client_options(self, simple_client):
        c = simple_client(connect=False)
        assert isinstance(c.options, ClientOptions)
        assert isinstance(c.options.pool_options, PoolOptions)
        assert c.options.server_selection_timeout == 30
        assert c.options.pool_options.max_idle_time_seconds is None
        assert isinstance(c.options.retry_writes, bool)
        assert isinstance(c.options.retry_reads, bool)

    def test_validate_suggestion(self):
        """Validate kwargs in constructor."""
        for typo in ["auth", "Auth", "AUTH"]:
            expected = (
                f"Unknown option: {typo}. Did you mean one of (authsource, authmechanism, "
                f"authoidcallowedhosts) or maybe a camelCase version of one? Refer to docstring."
            )
            expected = re.escape(expected)
            with pytest.raises(ConfigurationError, match=expected):
                MongoClient(**{typo: "standard"})  # type: ignore[arg-type]

    def test_detected_environment_logging(self, caplog):
        normal_hosts = [
            "normal.host.com",
            "host.cosmos.azure.com",
            "host.docdb.amazonaws.com",
            "host.docdb-elastic.amazonaws.com",
        ]
        srv_hosts = ["mongodb+srv://<test>:<test>@" + s for s in normal_hosts]
        multi_host = (
            "host.cosmos.azure.com,host.docdb.amazonaws.com,host.docdb-elastic.amazonaws.com"
        )
        with caplog.at_level(logging.INFO, logger="pymongo"):
            with mock.patch("pymongo.srv_resolver._SrvResolver.get_hosts") as mock_get_hosts:
                for host in normal_hosts:
                    MongoClient(host, connect=False)
                for host in srv_hosts:
                    mock_get_hosts.return_value = [(host, 1)]
                    MongoClient(host, connect=False)
                MongoClient(multi_host, connect=False)
                logs = [
                    record.getMessage()
                    for record in caplog.records
                    if record.name == "pymongo.client"
                ]
                assert len(logs) == 7

    def test_detected_environment_warning(self, caplog, simple_client):
        normal_hosts = [
            "host.cosmos.azure.com",
            "host.docdb.amazonaws.com",
            "host.docdb-elastic.amazonaws.com",
        ]
        srv_hosts = ["mongodb+srv://<test>:<test>@" + s for s in normal_hosts]
        multi_host = (
            "host.cosmos.azure.com,host.docdb.amazonaws.com,host.docdb-elastic.amazonaws.com"
        )
        with caplog.at_level(logging.WARN, logger="pymongo"):
            with mock.patch("pymongo.srv_resolver._SrvResolver.get_hosts") as mock_get_hosts:
                with pytest.warns(UserWarning):
                    for host in normal_hosts:
                        simple_client(host)
                    for host in srv_hosts:
                        mock_get_hosts.return_value = [(host, 1)]
                        simple_client(host)
                    simple_client(multi_host)


@pytest.mark.usefixtures("require_integration")
@pytest.mark.integration
class TestClientIntegrationTest(PyMongoTestCasePyTest):
    def test_multiple_uris(self):
        with pytest.raises(ConfigurationError):
            MongoClient(
                host=[
                    "mongodb+srv://cluster-a.abc12.mongodb.net",
                    "mongodb+srv://cluster-b.abc12.mongodb.net",
                    "mongodb+srv://cluster-c.abc12.mongodb.net",
                ]
            )

    def test_max_idle_time_reaper_default(self, rs_or_single_client):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper doesn't remove connections when maxIdleTimeMS not set
            client = rs_or_single_client()
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            with server._pool.checkout() as conn:
                pass
            assert 1 == len(server._pool.conns)
            assert conn in server._pool.conns

    def test_max_idle_time_reaper_removes_stale_minPoolSize(self, rs_or_single_client):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper removes idle socket and replaces it with a new one
            client = rs_or_single_client(maxIdleTimeMS=500, minPoolSize=1)
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            with server._pool.checkout() as conn:
                pass
            # When the reaper runs at the same time as the get_socket, two
            # connections could be created and checked into the pool.
            assert len(server._pool.conns) >= 1
            wait_until(lambda: conn not in server._pool.conns, "remove stale socket")
            wait_until(lambda: len(server._pool.conns) >= 1, "replace stale socket")

    def test_max_idle_time_reaper_does_not_exceed_maxPoolSize(self, rs_or_single_client):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper respects maxPoolSize when adding new connections.
            client = rs_or_single_client(maxIdleTimeMS=500, minPoolSize=1, maxPoolSize=1)
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            with server._pool.checkout() as conn:
                pass
            # When the reaper runs at the same time as the get_socket,
            # maxPoolSize=1 should prevent two connections from being created.
            assert 1 == len(server._pool.conns)
            wait_until(lambda: conn not in server._pool.conns, "remove stale socket")
            wait_until(lambda: len(server._pool.conns) == 1, "replace stale socket")

    def test_max_idle_time_reaper_removes_stale(self, rs_or_single_client):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert that the reaper has removed the idle socket and NOT replaced it.
            client = rs_or_single_client(maxIdleTimeMS=500)
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            with server._pool.checkout() as conn_one:
                pass
            # Assert that the pool does not close connections prematurely
            time.sleep(0.300)
            with server._pool.checkout() as conn_two:
                pass
            assert conn_one is conn_two
            wait_until(
                lambda: len(server._pool.conns) == 0,
                "stale socket reaped and new one NOT added to the pool",
            )

    def test_min_pool_size(self, rs_or_single_client):
        with client_knobs(kill_cursor_frequency=0.1):
            client = rs_or_single_client()
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            assert len(server._pool.conns) == 0

            # Assert that pool started up at minPoolSize
            client = rs_or_single_client(minPoolSize=10)
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            wait_until(
                lambda: len(server._pool.conns) == 10,
                "pool initialized with 10 connections",
            )
            # Assert that if a socket is closed, a new one takes its place.
            with server._pool.checkout() as conn:
                conn.close_conn(None)
            wait_until(
                lambda: len(server._pool.conns) == 10,
                "a closed socket gets replaced from the pool",
            )
            assert conn not in server._pool.conns

    def test_max_idle_time_checkout(self, rs_or_single_client):
        # Use high frequency to test _get_socket_no_auth.
        with client_knobs(kill_cursor_frequency=99999999):
            client = rs_or_single_client(maxIdleTimeMS=500)
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            with server._pool.checkout() as conn:
                pass
            assert len(server._pool.conns) == 1
            time.sleep(1)  # Sleep so that the socket becomes stale.

            with server._pool.checkout() as new_conn:
                assert conn != new_conn
            assert len(server._pool.conns) == 1
            assert conn not in server._pool.conns
            assert new_conn in server._pool.conns

            # Test that connections are reused if maxIdleTimeMS is not set.
            client = rs_or_single_client()
            server = (client._get_topology()).select_server(readable_server_selector, _Op.TEST)
            with server._pool.checkout() as conn:
                pass
            assert len(server._pool.conns) == 1
            time.sleep(1)
            with server._pool.checkout() as new_conn:
                assert conn == new_conn
            assert len(server._pool.conns) == 1

    def test_constants(self, client_context_fixture, simple_client):
        """This test uses MongoClient explicitly to make sure that host and
        port are not overloaded.
        """
        host, port = (
            client_context_fixture.host,
            client_context_fixture.port,
        )
        kwargs: dict = client_context_fixture.default_client_options.copy()
        if client_context_fixture.auth_enabled:
            kwargs["username"] = db_user
            kwargs["password"] = db_pwd

        # Set bad defaults.
        MongoClient.HOST = "somedomainthatdoesntexist.org"
        MongoClient.PORT = 123456789
        with pytest.raises(AutoReconnect):
            c = simple_client(serverSelectionTimeoutMS=10, **kwargs)
            connected(c)
        c = simple_client(host, port, **kwargs)
        # Override the defaults. No error.
        connected(c)
        # Set good defaults.
        MongoClient.HOST = host
        MongoClient.PORT = port
        # No error.
        c = simple_client(**kwargs)
        connected(c)

    def test_init_disconnected(self, client_context_fixture, rs_or_single_client, simple_client):
        host, port = (
            client_context_fixture.host,
            client_context_fixture.port,
        )
        c = rs_or_single_client(connect=False)
        # is_primary causes client to block until connected
        assert isinstance(c.is_primary, bool)
        c = rs_or_single_client(connect=False)
        assert isinstance(c.is_mongos, bool)
        c = rs_or_single_client(connect=False)
        assert isinstance(c.options.pool_options.max_pool_size, int)
        assert isinstance(c.nodes, frozenset)

        c = rs_or_single_client(connect=False)
        assert c.codec_options == CodecOptions()
        c = rs_or_single_client(connect=False)
        assert not c.primary
        assert not c.secondaries
        c = rs_or_single_client(connect=False)
        assert isinstance(c.topology_description, TopologyDescription)
        assert c.topology_description == c._topology._description
        if client_context_fixture.is_rs:
            # The primary's host and port are from the replica set config.
            assert c.address is not None
        else:
            assert c.address == (host, port)
        bad_host = "somedomainthatdoesntexist.org"
        c = simple_client(bad_host, port, connectTimeoutMS=1, serverSelectionTimeoutMS=10)
        with pytest.raises(ConnectionFailure):
            c.pymongo_test.test.find_one()

    def test_init_disconnected_with_auth(self, simple_client):
        uri = "mongodb://user:pass@somedomainthatdoesntexist"
        c = simple_client(uri, connectTimeoutMS=1, serverSelectionTimeoutMS=10)
        with pytest.raises(ConnectionFailure):
            c.pymongo_test.test.find_one()

    def test_equality(self, client_context_fixture, rs_or_single_client, simple_client):
        seed = "{}:{}".format(*list(client_context_fixture.client._topology_settings.seeds)[0])
        c = rs_or_single_client(seed, connect=False)
        assert client_context_fixture.client == c
        # Explicitly test inequality
        assert not client_context_fixture.client != c

        c = rs_or_single_client("invalid.com", connect=False)
        assert client_context_fixture.client != c
        assert client_context_fixture.client != c

        c1 = simple_client("a", connect=False)
        c2 = simple_client("b", connect=False)

        # Seeds differ:
        assert c1 != c2

        c1 = simple_client(["a", "b", "c"], connect=False)
        c2 = simple_client(["c", "a", "b"], connect=False)

        # Same seeds but out of order still compares equal:
        assert c1 == c2

    def test_hashable(self, client_context_fixture, rs_or_single_client):
        seed = "{}:{}".format(*list(client_context_fixture.client._topology_settings.seeds)[0])
        c = rs_or_single_client(seed, connect=False)
        assert c in {client_context_fixture.client}
        c = rs_or_single_client("invalid.com", connect=False)
        assert c not in {client_context_fixture.client}

    def test_host_w_port(self, client_context_fixture):
        with pytest.raises(ValueError):
            host = client_context_fixture.host
            connected(
                MongoClient(
                    f"{host}:1234567",
                    connectTimeoutMS=1,
                    serverSelectionTimeoutMS=10,
                )
            )

    def test_repr(self, simple_client):
        # Used to test 'eval' below.
        import bson

        client = MongoClient(  # type: ignore[type-var]
            "mongodb://localhost:27017,localhost:27018/?replicaSet=replset"
            "&connectTimeoutMS=12345&w=1&wtimeoutms=100",
            connect=False,
            document_class=SON,
        )
        the_repr = repr(client)
        assert "MongoClient(host=" in the_repr
        assert "document_class=bson.son.SON, tz_aware=False, connect=False, " in the_repr
        assert "connecttimeoutms=12345" in the_repr
        assert "replicaset='replset'" in the_repr
        assert "w=1" in the_repr
        assert "wtimeoutms=100" in the_repr
        with eval(the_repr) as client_two:
            assert client_two == client
        client = simple_client(
            "localhost:27017,localhost:27018",
            replicaSet="replset",
            connectTimeoutMS=12345,
            socketTimeoutMS=None,
            w=1,
            wtimeoutms=100,
            connect=False,
        )
        the_repr = repr(client)
        assert "MongoClient(host=" in the_repr
        assert "document_class=dict, tz_aware=False, connect=False, " in the_repr
        assert "connecttimeoutms=12345" in the_repr
        assert "replicaset='replset'" in the_repr
        assert "sockettimeoutms=None" in the_repr
        assert "w=1" in the_repr
        assert "wtimeoutms=100" in the_repr
        with eval(the_repr) as client_two:
            assert client_two == client

    def test_getters(self, client_context_fixture):
        wait_until(
            lambda: client_context_fixture.nodes == client_context_fixture.client.nodes,
            "find all nodes",
        )

    def test_list_databases(self, client_context_fixture, rs_or_single_client):
        cmd_docs = (client_context_fixture.client.admin.command("listDatabases"))["databases"]
        cursor = client_context_fixture.client.list_databases()
        assert isinstance(cursor, CommandCursor)
        helper_docs = cursor.to_list()
        assert len(helper_docs) > 0
        assert len(helper_docs) == len(cmd_docs)
        # PYTHON-3529 Some fields may change between calls, just compare names.
        for helper_doc, cmd_doc in zip(helper_docs, cmd_docs):
            assert isinstance(helper_doc, dict)
            assert helper_doc.keys() == cmd_doc.keys()

        client_doc = rs_or_single_client(document_class=SON)
        for doc in client_doc.list_databases():
            assert isinstance(doc, dict)

        client_context_fixture.client.pymongo_test.test.insert_one({})
        cursor = client_context_fixture.client.list_databases(filter={"name": "admin"})
        docs = cursor.to_list()
        assert len(docs) == 1
        assert docs[0]["name"] == "admin"

        cursor = client_context_fixture.client.list_databases(nameOnly=True)
        for doc in cursor:
            assert list(doc) == ["name"]

    def test_list_database_names(self, client_context_fixture):
        client_context_fixture.client.pymongo_test.test.insert_one({"dummy": "object"})
        client_context_fixture.client.pymongo_test_mike.test.insert_one({"dummy": "object"})
        cmd_docs = (client_context_fixture.client.admin.command("listDatabases"))["databases"]
        cmd_names = [doc["name"] for doc in cmd_docs]

        db_names = client_context_fixture.client.list_database_names()
        assert "pymongo_test" in db_names
        assert "pymongo_test_mike" in db_names
        assert db_names == cmd_names

    def test_drop_database(self, client_context_fixture, rs_or_single_client):
        with pytest.raises(TypeError):
            client_context_fixture.client.drop_database(5)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            client_context_fixture.client.drop_database(None)  # type: ignore[arg-type]

        client_context_fixture.client.pymongo_test.test.insert_one({"dummy": "object"})
        client_context_fixture.client.pymongo_test2.test.insert_one({"dummy": "object"})
        dbs = client_context_fixture.client.list_database_names()
        assert "pymongo_test" in dbs
        assert "pymongo_test2" in dbs
        client_context_fixture.client.drop_database("pymongo_test")

        if client_context_fixture.is_rs:
            wc_client = rs_or_single_client(w=len(client_context_fixture.nodes) + 1)
            with pytest.raises(WriteConcernError):
                wc_client.drop_database("pymongo_test2")

        client_context_fixture.client.drop_database(client_context_fixture.client.pymongo_test2)
        dbs = client_context_fixture.client.list_database_names()
        assert "pymongo_test" not in dbs
        assert "pymongo_test2" not in dbs

    def test_close(self, rs_or_single_client):
        test_client = rs_or_single_client()
        coll = test_client.pymongo_test.bar
        test_client.close()
        with pytest.raises(InvalidOperation):
            coll.count_documents({})

    def test_close_kills_cursors(self, rs_or_single_client):
        if sys.platform.startswith("java"):
            # We can't figure out how to make this test reliable with Jython.
            raise SkipTest("Can't test with Jython")
        test_client = rs_or_single_client()
        # Kill any cursors possibly queued up by previous tests.
        gc.collect()
        test_client._process_periodic_tasks()

        # Add some test data.
        coll = test_client.pymongo_test.test_close_kills_cursors
        docs_inserted = 1000
        coll.insert_many([{"i": i} for i in range(docs_inserted)])

        # Open a cursor and leave it open on the server.
        cursor = coll.find().batch_size(10)
        assert bool(next(cursor))
        assert cursor.retrieved < docs_inserted

        # Open a command cursor and leave it open on the server.
        cursor = coll.aggregate([], batchSize=10)
        assert bool(next(cursor))
        del cursor
        # Required for PyPy, Jython and other Python implementations that
        # don't use reference counting garbage collection.
        gc.collect()

        # Close the client and ensure the topology is closed.
        assert test_client._topology._opened
        test_client.close()
        assert not test_client._topology._opened
        test_client = rs_or_single_client()
        # The killCursors task should not need to re-open the topology.
        test_client._process_periodic_tasks()
        assert test_client._topology._opened

    def test_close_stops_kill_cursors_thread(self, rs_client):
        client = rs_client()
        client.test.test.find_one()
        assert not client._kill_cursors_executor._stopped

        # Closing the client should stop the thread.
        client.close()
        assert client._kill_cursors_executor._stopped

        # Reusing the closed client should raise an InvalidOperation error.
        with pytest.raises(InvalidOperation):
            client.admin.command("ping")
        # Thread is still stopped.
        assert client._kill_cursors_executor._stopped

    def test_uri_connect_option(self, rs_client):
        # Ensure that topology is not opened if connect=False.
        client = rs_client(connect=False)
        assert not client._topology._opened

        # Ensure kill cursors thread has not been started.
        if _IS_SYNC:
            kc_thread = client._kill_cursors_executor._thread
            assert not (kc_thread and kc_thread.is_alive())
        else:
            kc_task = client._kill_cursors_executor._task
            assert not (kc_task and not kc_task.done())
        # Using the client should open topology and start the thread.
        client.admin.command("ping")
        assert client._topology._opened
        if _IS_SYNC:
            kc_thread = client._kill_cursors_executor._thread
            assert kc_thread and kc_thread.is_alive()
        else:
            kc_task = client._kill_cursors_executor._task
            assert kc_task and not kc_task.done()

    def test_close_does_not_open_servers(self, rs_client):
        client = rs_client(connect=False)
        topology = client._topology
        assert topology._servers == {}
        client.close()
        assert topology._servers == {}

    def test_close_closes_sockets(self, rs_client):
        client = rs_client()
        client.test.test.find_one()
        topology = client._topology
        client.close()
        for server in topology._servers.values():
            assert not server._pool.conns
            assert server._monitor._executor._stopped
            assert server._monitor._rtt_monitor._executor._stopped
            assert not server._monitor._pool.conns
            assert not server._monitor._rtt_monitor._pool.conns

    def test_bad_uri(self):
        with pytest.raises(InvalidURI):
            MongoClient("http://localhost")

    @pytest.mark.usefixtures("require_auth")
    @pytest.mark.usefixtures("require_no_fips")
    @pytest.mark.parametrize("remove_all_users_fixture", ["pymongo_test"], indirect=True)
    @pytest.mark.parametrize("drop_user_fixture", [("admin", "admin")], indirect=True)
    def test_auth_from_uri(
        self,
        client_context_fixture,
        rs_or_single_client_noauth,
        remove_all_users_fixture,
        drop_user_fixture,
    ):
        host, port = (
            client_context_fixture.host,
            client_context_fixture.port,
        )
        client_context_fixture.create_user("admin", "admin", "pass")

        client_context_fixture.create_user(
            "pymongo_test", "user", "pass", roles=["userAdmin", "readWrite"]
        )

        with pytest.raises(OperationFailure):
            connected(rs_or_single_client_noauth("mongodb://a:b@%s:%d" % (host, port)))

        # No error.
        connected(rs_or_single_client_noauth("mongodb://admin:pass@%s:%d" % (host, port)))

        # Wrong database.
        uri = "mongodb://admin:pass@%s:%d/pymongo_test" % (host, port)
        with pytest.raises(OperationFailure):
            connected(rs_or_single_client_noauth(uri))

        # No error.
        connected(
            rs_or_single_client_noauth("mongodb://user:pass@%s:%d/pymongo_test" % (host, port))
        )

        # Auth with lazy connection.
        (
            rs_or_single_client_noauth(
                "mongodb://user:pass@%s:%d/pymongo_test" % (host, port), connect=False
            )
        ).pymongo_test.test.find_one()

        # Wrong password.
        bad_client = rs_or_single_client_noauth(
            "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port), connect=False
        )

        with pytest.raises(OperationFailure):
            bad_client.pymongo_test.test.find_one()

    @pytest.mark.usefixtures("require_auth")
    @pytest.mark.parametrize("drop_user_fixture", [("admin", "ad min")], indirect=True)
    def test_username_and_password(
        self, client_context_fixture, rs_or_single_client_noauth, drop_user_fixture
    ):
        client_context_fixture.create_user("admin", "ad min", "pa/ss")

        c = rs_or_single_client_noauth(username="ad min", password="pa/ss")

        # Username and password aren't in strings that will likely be logged.
        assert "ad min" not in repr(c)
        assert "ad min" not in str(c)
        assert "pa/ss" not in repr(c)
        assert "pa/ss" not in str(c)

        # Auth succeeds.
        c.server_info()

        with pytest.raises(OperationFailure):
            (rs_or_single_client_noauth(username="ad min", password="foo")).server_info()

    @pytest.mark.usefixtures("require_auth")
    @pytest.mark.usefixtures("require_no_fips")
    def test_lazy_auth_raises_operation_failure(
        self, client_context_fixture, rs_or_single_client_noauth
    ):
        host = client_context_fixture.host
        lazy_client = rs_or_single_client_noauth(
            f"mongodb://user:wrong@{host}/pymongo_test", connect=False
        )

        assertRaisesExactly(OperationFailure, lazy_client.test.collection.find_one)

    @pytest.mark.usefixtures("require_no_tls")
    def test_unix_socket(self, client_context_fixture, rs_or_single_client, simple_client):
        if not hasattr(socket, "AF_UNIX"):
            pytest.skip("UNIX-sockets are not supported on this system")

        mongodb_socket = "/tmp/mongodb-%d.sock" % (client_context_fixture.port,)
        encoded_socket = "%2Ftmp%2F" + "mongodb-%d.sock" % (client_context_fixture.port,)
        if not os.access(mongodb_socket, os.R_OK):
            pytest.skip("Socket file is not accessible")

        uri = "mongodb://%s" % encoded_socket
        # Confirm we can do operations via the socket.
        client = rs_or_single_client(uri)
        client.pymongo_test.test.insert_one({"dummy": "object"})
        dbs = client.list_database_names()
        assert "pymongo_test" in dbs

        assert mongodb_socket in repr(client)

        # Confirm it fails with a missing socket.
        with pytest.raises(ConnectionFailure):
            c = simple_client("mongodb://%2Ftmp%2Fnon-existent.sock", serverSelectionTimeoutMS=100)
            connected(c)

    def test_document_class(self, client_context_fixture, rs_or_single_client):
        c = client_context_fixture.client
        db = c.pymongo_test
        db.test.insert_one({"x": 1})

        assert dict == c.codec_options.document_class
        assert isinstance(db.test.find_one(), dict)
        assert not isinstance(db.test.find_one(), SON)

        c = rs_or_single_client(document_class=SON)

        db = c.pymongo_test

        assert SON == c.codec_options.document_class
        assert isinstance(db.test.find_one(), SON)

    def test_timeouts(self, rs_or_single_client):
        client = rs_or_single_client(
            connectTimeoutMS=10500,
            socketTimeoutMS=10500,
            maxIdleTimeMS=10500,
            serverSelectionTimeoutMS=10500,
        )
        assert 10.5 == (get_pool(client)).opts.connect_timeout
        assert 10.5 == (get_pool(client)).opts.socket_timeout
        assert 10.5 == (get_pool(client)).opts.max_idle_time_seconds
        assert 10.5 == client.options.pool_options.max_idle_time_seconds
        assert 10.5 == client.options.server_selection_timeout

    def test_socket_timeout_ms_validation(self, rs_or_single_client):
        c = rs_or_single_client(socketTimeoutMS=10 * 1000)
        assert 10 == (get_pool(c)).opts.socket_timeout

        c = connected(rs_or_single_client(socketTimeoutMS=None))
        assert (get_pool(c)).opts.socket_timeout is None

        c = connected(rs_or_single_client(socketTimeoutMS=0))
        assert (get_pool(c)).opts.socket_timeout is None

        with pytest.raises(ValueError):
            with rs_or_single_client(socketTimeoutMS=-1):
                pass

        with pytest.raises(ValueError):
            with rs_or_single_client(socketTimeoutMS=1e10):
                pass

        with pytest.raises(ValueError):
            with rs_or_single_client(socketTimeoutMS="foo"):
                pass

    def test_socket_timeout(self, client_context_fixture, rs_or_single_client):
        no_timeout = client_context_fixture.client
        timeout_sec = 1
        timeout = rs_or_single_client(socketTimeoutMS=1000 * timeout_sec)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert_one({"x": 1})

        # A $where clause that takes a second longer than the timeout
        where_func = delay(timeout_sec + 1)

        def get_x(db):
            doc = next(db.test.find().where(where_func))
            return doc["x"]

        assert 1 == get_x(no_timeout.pymongo_test)
        with pytest.raises(NetworkTimeout):
            get_x(timeout.pymongo_test)

    def test_server_selection_timeout(self):
        client = MongoClient(serverSelectionTimeoutMS=100, connect=False)
        pytest.approx(client.options.server_selection_timeout, 0.1)
        client.close()

        client = MongoClient(serverSelectionTimeoutMS=0, connect=False)

        pytest.approx(client.options.server_selection_timeout, 0)

        pytest.raises(ValueError, MongoClient, serverSelectionTimeoutMS="foo", connect=False)
        pytest.raises(ValueError, MongoClient, serverSelectionTimeoutMS=-1, connect=False)
        pytest.raises(ConfigurationError, MongoClient, serverSelectionTimeoutMS=None, connect=False)
        client.close()

        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=100", connect=False)
        pytest.approx(client.options.server_selection_timeout, 0.1)
        client.close()

        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=0", connect=False)
        pytest.approx(client.options.server_selection_timeout, 0)
        client.close()

        # Test invalid timeout in URI ignored and set to default.
        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=-1", connect=False)
        pytest.approx(client.options.server_selection_timeout, 30)
        client.close()

        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=", connect=False)
        pytest.approx(client.options.server_selection_timeout, 30)

    def test_waitQueueTimeoutMS(self, rs_or_single_client):
        client = rs_or_single_client(waitQueueTimeoutMS=2000)
        assert 2 == (get_pool(client)).opts.wait_queue_timeout

    def test_socketKeepAlive(self, client_context_fixture):
        pool = get_pool(client_context_fixture.client)
        with pool.checkout() as conn:
            keepalive = conn.conn.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
            assert keepalive

    @no_type_check
    def test_tz_aware(self, client_context_fixture, rs_or_single_client):
        pytest.raises(ValueError, MongoClient, tz_aware="foo")

        aware = rs_or_single_client(tz_aware=True)
        naive = client_context_fixture.client
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        aware.pymongo_test.test.insert_one({"x": now})

        assert (naive.pymongo_test.test.find_one())["x"].tzinfo is None
        assert utc == (aware.pymongo_test.test.find_one())["x"].tzinfo
        assert (aware.pymongo_test.test.find_one())["x"].replace(tzinfo=None) == (
            naive.pymongo_test.test.find_one()
        )["x"]

    @pytest.mark.usefixtures("require_ipv6")
    def test_ipv6(self, client_context_fixture, rs_or_single_client_noauth):
        if client_context_fixture.tls:
            if not HAVE_IPADDRESS:
                pytest.skip("Need the ipaddress module to test with SSL")

        if client_context_fixture.auth_enabled:
            auth_str = f"{db_user}:{db_pwd}@"
        else:
            auth_str = ""

        uri = "mongodb://%s[::1]:%d" % (auth_str, client_context_fixture.port)
        if client_context_fixture.is_rs:
            uri += "/?replicaSet=" + (client_context_fixture.replica_set_name or "")

        client = rs_or_single_client_noauth(uri)
        client.pymongo_test.test.insert_one({"dummy": "object"})
        client.pymongo_test_bernie.test.insert_one({"dummy": "object"})

        dbs = client.list_database_names()
        assert "pymongo_test" in dbs
        assert "pymongo_test_bernie" in dbs

    def test_contextlib(self, rs_or_single_client):
        client = rs_or_single_client()
        client.pymongo_test.drop_collection("test")
        client.pymongo_test.test.insert_one({"foo": "bar"})

        # The socket used for the previous commands has been returned to the
        # pool
        assert 1 == len((get_pool(client)).conns)

        # contextlib async support was added in Python 3.10
        if _IS_SYNC or sys.version_info >= (3, 10):
            with contextlib.closing(client):
                assert "bar" == (client.pymongo_test.test.find_one())["foo"]
            with pytest.raises(InvalidOperation):
                client.pymongo_test.test.find_one()
            client = rs_or_single_client()
            with client as client:
                assert "bar" == (client.pymongo_test.test.find_one())["foo"]
            with pytest.raises(InvalidOperation):
                client.pymongo_test.test.find_one()

    @pytest.mark.usefixtures("require_sync")
    def test_interrupt_signal(self, client_context_fixture):
        if sys.platform.startswith("java"):
            # We can't figure out how to raise an exception on a thread that's
            # blocked on a socket, whether that's the main thread or a worker,
            # without simply killing the whole thread in Jython. This suggests
            # PYTHON-294 can't actually occur in Jython.
            pytest.skip("Can't test interrupts in Jython")
        if is_greenthread_patched():
            pytest.skip("Can't reliably test interrupts with green threads")

        # Test fix for PYTHON-294 -- make sure MongoClient closes its
        # socket if it gets an interrupt while waiting to recv() from it.
        db = client_context_fixture.client.pymongo_test

        # A $where clause which takes 1.5 sec to execute
        where = delay(1.5)

        # Need exactly 1 document so find() will execute its $where clause once
        db.drop_collection("foo")
        db.foo.insert_one({"_id": 1})

        old_signal_handler = None
        try:
            # Platform-specific hacks for raising a KeyboardInterrupt on the
            # main thread while find() is in-progress: On Windows, SIGALRM is
            # unavailable so we use a second thread. In our Evergreen setup on
            # Linux, the thread technique causes an error in the test at
            # conn.recv(): TypeError: 'int' object is not callable
            # We don't know what causes this, so we hack around it.

            if sys.platform == "win32":

                def interrupter():
                    # Raises KeyboardInterrupt in the main thread
                    time.sleep(0.25)
                    thread.interrupt_main()

                thread.start_new_thread(interrupter, ())
            else:
                # Convert SIGALRM to SIGINT -- it's hard to schedule a SIGINT
                # for one second in the future, but easy to schedule SIGALRM.
                def sigalarm(num, frame):
                    raise KeyboardInterrupt

                old_signal_handler = signal.signal(signal.SIGALRM, sigalarm)
                signal.alarm(1)

            raised = False
            try:
                # Will be interrupted by a KeyboardInterrupt.
                next(db.foo.find({"$where": where}))  # type: ignore[call-overload]
            except KeyboardInterrupt:
                raised = True

            assert raised, "Didn't raise expected KeyboardInterrupt"

            # Raises AssertionError due to PYTHON-294 -- Mongo's response to
            # the previous find() is still waiting to be read on the socket,
            # so the request id's don't match.
            assert {"_id": 1} == next(db.foo.find())  # type: ignore[call-overload]
        finally:
            if old_signal_handler:
                signal.signal(signal.SIGALRM, old_signal_handler)

    def test_operation_failure(self, single_client):
        # Ensure MongoClient doesn't close socket after it gets an error
        # response to getLastError. PYTHON-395. We need a new client here
        # to avoid race conditions caused by replica set failover or idle
        # socket reaping.
        client = single_client()
        client.pymongo_test.test.find_one()
        pool = get_pool(client)
        socket_count = len(pool.conns)
        assert socket_count >= 1
        old_conn = next(iter(pool.conns))
        client.pymongo_test.test.drop()
        client.pymongo_test.test.insert_one({"_id": "foo"})
        with pytest.raises(OperationFailure):
            client.pymongo_test.test.insert_one({"_id": "foo"})

        assert socket_count == len(pool.conns)
        new_conn = next(iter(pool.conns))
        assert old_conn == new_conn

    @pytest.mark.parametrize("drop_database_fixture", ["test_lazy_connect_w0"], indirect=True)
    def test_lazy_connect_w0(
        self, client_context_fixture, rs_or_single_client, drop_database_fixture
    ):
        # Ensure that connect-on-demand works when the first operation is
        # an unacknowledged write. This exercises _writable_max_wire_version().

        # Use a separate collection to avoid races where we're still
        # completing an operation on a collection while the next test begins.
        client_context_fixture.client.drop_database("test_lazy_connect_w0")

        client = rs_or_single_client(connect=False, w=0)
        client.test_lazy_connect_w0.test.insert_one({})

        def predicate():
            return client.test_lazy_connect_w0.test.count_documents({}) == 1

        wait_until(predicate, "find one document")

        client = rs_or_single_client(connect=False, w=0)
        client.test_lazy_connect_w0.test.update_one({}, {"$set": {"x": 1}})

        def predicate():
            return (client.test_lazy_connect_w0.test.find_one()).get("x") == 1

        wait_until(predicate, "update one document")

        client = rs_or_single_client(connect=False, w=0)
        client.test_lazy_connect_w0.test.delete_one({})

        def predicate():
            return client.test_lazy_connect_w0.test.count_documents({}) == 0

        wait_until(predicate, "delete one document")

    @pytest.mark.usefixtures("require_no_mongos")
    def test_exhaust_network_error(self, rs_or_single_client):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = rs_or_single_client(maxPoolSize=1, retryReads=False)
        collection = client.pymongo_test.test
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        # Ensure a socket.
        connected(client)

        # Cause a network error.
        conn = one(pool.conns)
        conn.conn.close()
        cursor = collection.find(cursor_type=CursorType.EXHAUST)
        with pytest.raises(ConnectionFailure):
            next(cursor)

        assert conn.closed

        # The semaphore was decremented despite the error.
        assert 0 == pool.requests

    @pytest.mark.usefixtures("require_auth")
    def test_auth_network_error(self, rs_or_single_client):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.

        # Get a client with one socket so we detect if it's leaked.
        c = connected(rs_or_single_client(maxPoolSize=1, waitQueueTimeoutMS=1, retryReads=False))

        # Cause a network error on the actual socket.
        pool = get_pool(c)
        conn = one(pool.conns)
        conn.conn.close()

        # Connection.authenticate logs, but gets a socket.error. Should be
        # reraised as AutoReconnect.
        with pytest.raises(AutoReconnect):
            c.test.collection.find_one()

        # No semaphore leak, the pool is allowed to make a new socket.
        c.test.collection.find_one()

    @pytest.mark.usefixtures("require_no_replica_set")
    def test_connect_to_standalone_using_replica_set_name(self, single_client):
        client = single_client(replicaSet="anything", serverSelectionTimeoutMS=100)
        with pytest.raises(AutoReconnect):
            client.test.test.find_one()

    @pytest.mark.usefixtures("require_replica_set")
    def test_stale_getmore(self, rs_client):
        # A cursor is created, but its member goes down and is removed from
        # the topology before the getMore message is sent. Test that
        # MongoClient._run_operation_with_response handles the error.
        with pytest.raises(AutoReconnect):
            client = rs_client(connect=False, serverSelectionTimeoutMS=100)
            client._run_operation(
                operation=message._GetMore(
                    "pymongo_test",
                    "collection",
                    101,
                    1234,
                    client.codec_options,
                    ReadPreference.PRIMARY,
                    None,
                    client,
                    None,
                    None,
                    False,
                    None,
                ),
                unpack_res=Cursor(client.pymongo_test.collection)._unpack_response,
                address=("not-a-member", 27017),
            )

    def test_heartbeat_frequency_ms(self, client_context_fixture, single_client):
        class HeartbeatStartedListener(ServerHeartbeatListener):
            def __init__(self):
                self.results = []

            def started(self, event):
                self.results.append(event)

            def succeeded(self, event):
                pass

            def failed(self, event):
                pass

        old_init = ServerHeartbeatStartedEvent.__init__
        heartbeat_times = []

        def init(self, *args):
            old_init(self, *args)
            heartbeat_times.append(time.time())

        try:
            ServerHeartbeatStartedEvent.__init__ = init  # type: ignore
            listener = HeartbeatStartedListener()
            uri = "mongodb://%s:%d/?heartbeatFrequencyMS=500" % (
                client_context_fixture.host,
                client_context_fixture.port,
            )
            single_client(uri, event_listeners=[listener])
            wait_until(
                lambda: len(listener.results) >= 2, "record two ServerHeartbeatStartedEvents"
            )

            # Default heartbeatFrequencyMS is 10 sec. Check the interval was
            # closer to 0.5 sec with heartbeatFrequencyMS configured.
            pytest.approx(heartbeat_times[1] - heartbeat_times[0], 0.5, abs=2)

        finally:
            ServerHeartbeatStartedEvent.__init__ = old_init  # type: ignore

    def test_small_heartbeat_frequency_ms(self):
        uri = "mongodb://example/?heartbeatFrequencyMS=499"
        with pytest.raises(ConfigurationError) as context:
            MongoClient(uri)

        assert "heartbeatFrequencyMS" in str(context.value)

    def test_compression(self, client_context_fixture, simple_client, single_client):
        def compression_settings(client):
            pool_options = client.options.pool_options
            return pool_options._compression_settings

        client = simple_client("mongodb://localhost:27017/?compressors=zlib", connect=False)
        opts = compression_settings(client)
        assert opts.compressors == ["zlib"]

        client = simple_client(
            "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=4", connect=False
        )
        opts = compression_settings(client)
        assert opts.compressors == ["zlib"]
        assert opts.zlib_compression_level == 4

        client = simple_client(
            "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=-1", connect=False
        )
        opts = compression_settings(client)
        assert opts.compressors == ["zlib"]
        assert opts.zlib_compression_level == -1

        client = simple_client("mongodb://localhost:27017", connect=False)
        opts = compression_settings(client)
        assert opts.compressors == []
        assert opts.zlib_compression_level == -1

        client = simple_client("mongodb://localhost:27017/?compressors=foobar", connect=False)
        opts = compression_settings(client)
        assert opts.compressors == []
        assert opts.zlib_compression_level == -1

        client = simple_client("mongodb://localhost:27017/?compressors=foobar,zlib", connect=False)
        opts = compression_settings(client)
        assert opts.compressors == ["zlib"]
        assert opts.zlib_compression_level == -1

        client = simple_client(
            "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=10", connect=False
        )
        opts = compression_settings(client)
        assert opts.compressors == ["zlib"]
        assert opts.zlib_compression_level == -1

        client = simple_client(
            "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=-2", connect=False
        )
        opts = compression_settings(client)
        assert opts.compressors == ["zlib"]
        assert opts.zlib_compression_level == -1

        if not _have_snappy():
            client = simple_client("mongodb://localhost:27017/?compressors=snappy", connect=False)
            opts = compression_settings(client)
            assert opts.compressors == []
        else:
            client = simple_client("mongodb://localhost:27017/?compressors=snappy", connect=False)
            opts = compression_settings(client)
            assert opts.compressors == ["snappy"]
            client = simple_client(
                "mongodb://localhost:27017/?compressors=snappy,zlib", connect=False
            )
            opts = compression_settings(client)
            assert opts.compressors == ["snappy", "zlib"]

        if not _have_zstd():
            client = simple_client("mongodb://localhost:27017/?compressors=zstd", connect=False)
            opts = compression_settings(client)
            assert opts.compressors == []
        else:
            client = simple_client("mongodb://localhost:27017/?compressors=zstd", connect=False)
            opts = compression_settings(client)
            assert opts.compressors == ["zstd"]
            client = simple_client(
                "mongodb://localhost:27017/?compressors=zstd,zlib", connect=False
            )
            opts = compression_settings(client)
            assert opts.compressors == ["zstd", "zlib"]

        options = client_context_fixture.default_client_options
        if "compressors" in options and "zlib" in options["compressors"]:
            for level in range(-1, 10):
                client = single_client(zlibcompressionlevel=level)
                client.pymongo_test.test.find_one()  # No error

    @pytest.mark.usefixtures("require_sync")
    def test_reset_during_update_pool(self, rs_or_single_client):
        client = rs_or_single_client(minPoolSize=10)
        client.admin.command("ping")
        pool = get_pool(client)
        generation = pool.gen.get_overall()

        # Continuously reset the pool.
        class ResetPoolThread(threading.Thread):
            def __init__(self, pool):
                super().__init__()
                self.running = True
                self.pool = pool

            def stop(self):
                self.running = False

            def _run(self):
                while self.running:
                    exc = AutoReconnect("mock pool error")
                    ctx = _ErrorContext(exc, 0, pool.gen.get_overall(), False, None)
                    client._topology.handle_error(pool.address, ctx)
                    time.sleep(0.001)

            def run(self):
                self._run()

        t = ResetPoolThread(pool)
        t.start()

        # Ensure that update_pool completes without error even when the pool is reset concurrently.
        try:
            while True:
                for _ in range(10):
                    client._topology.update_pool()
                if generation != pool.gen.get_overall():
                    break
        finally:
            t.stop()
            t.join()
        client.admin.command("ping")

    def test_background_connections_do_not_hold_locks(self, rs_or_single_client):
        min_pool_size = 10
        client = rs_or_single_client(
            serverSelectionTimeoutMS=3000, minPoolSize=min_pool_size, connect=False
        )
        client.admin.command("ping")  # Create a single connection in the pool

        # Cause new connections to stall for a few seconds.
        pool = get_pool(client)
        original_connect = pool.connect

        def stall_connect(*args, **kwargs):
            time.sleep(2)
            return original_connect(*args, **kwargs)

        try:
            pool.connect = stall_connect

            wait_until(lambda: len(pool.conns) > 1, "start creating connections")
            # Assert that application operations do not block.
            for _ in range(10):
                start = time.monotonic()
                client.admin.command("ping")
                total = time.monotonic() - start
                assert total < 2
        finally:
            delattr(pool, "connect")

    @pytest.mark.usefixtures("require_replica_set")
    def test_direct_connection(self, rs_or_single_client):
        client = rs_or_single_client(directConnection=True)
        client.admin.command("ping")
        assert len(client.nodes) == 1
        assert client._topology_settings.get_topology_type() == TOPOLOGY_TYPE.Single

        client = rs_or_single_client(directConnection=False)
        client.admin.command("ping")
        assert len(client.nodes) >= 1
        assert client._topology_settings.get_topology_type() in [
            TOPOLOGY_TYPE.ReplicaSetNoPrimary,
            TOPOLOGY_TYPE.ReplicaSetWithPrimary,
        ]

        with pytest.raises(ConfigurationError):
            MongoClient(["host1", "host2"], directConnection=True)

    @pytest.mark.skipif("PyPy" in sys.version, reason="PYTHON-2927 fails often on PyPy")
    def test_continuous_network_errors(self, simple_client):
        def server_description_count():
            i = 0
            for obj in gc.get_objects():
                try:
                    if isinstance(obj, ServerDescription):
                        i += 1
                except ReferenceError:
                    pass
            return i

        gc.collect()
        with client_knobs(min_heartbeat_interval=0.003):
            client = simple_client(
                "invalid:27017", heartbeatFrequencyMS=3, serverSelectionTimeoutMS=150
            )
            initial_count = server_description_count()
            with pytest.raises(ServerSelectionTimeoutError):
                client.test.test.find_one()
            gc.collect()
            final_count = server_description_count()
            assert pytest.approx(initial_count, abs=20) == final_count

    @pytest.mark.usefixtures("require_failCommand_fail_point")
    def test_network_error_message(self, single_client):
        client = single_client(retryReads=False)
        client.admin.command("ping")  # connect
        with self.fail_point(
            client,
            {"mode": {"times": 1}, "data": {"closeConnection": True, "failCommands": ["find"]}},
        ):
            assert client.address is not None
            expected = "{}:{}: ".format(*(client.address))
            with pytest.raises(AutoReconnect, match=expected):
                client.pymongo_test.test.find_one({})

    @pytest.mark.skipif("PyPy" in sys.version, reason="PYTHON-2938 could fail on PyPy")
    def test_process_periodic_tasks(self, rs_or_single_client):
        client = rs_or_single_client()
        coll = client.db.collection
        coll.insert_many([{} for _ in range(5)])
        cursor = coll.find(batch_size=2)
        cursor.next()
        c_id = cursor.cursor_id
        assert c_id is not None
        client.close()
        del cursor
        wait_until(lambda: client._kill_cursors_queue, "waited for cursor to be added to queue")
        client._process_periodic_tasks()  # This must not raise or print any exceptions
        with pytest.raises(InvalidOperation):
            coll.insert_many([{} for _ in range(5)])

    def test_service_name_from_kwargs(self):
        client = MongoClient(
            "mongodb+srv://user:password@test22.test.build.10gen.cc",
            srvServiceName="customname",
            connect=False,
        )
        assert client._topology_settings.srv_service_name == "customname"

        client = MongoClient(
            "mongodb+srv://user:password@test22.test.build.10gen.cc/?srvServiceName=shouldbeoverriden",
            srvServiceName="customname",
            connect=False,
        )
        assert client._topology_settings.srv_service_name == "customname"

        client = MongoClient(
            "mongodb+srv://user:password@test22.test.build.10gen.cc/?srvServiceName=customname",
            connect=False,
        )
        assert client._topology_settings.srv_service_name == "customname"

    def test_srv_max_hosts_kwarg(self, simple_client):
        client = simple_client("mongodb+srv://test1.test.build.10gen.cc/")
        assert len(client.topology_description.server_descriptions()) > 1

        client = simple_client("mongodb+srv://test1.test.build.10gen.cc/", srvmaxhosts=1)
        assert len(client.topology_description.server_descriptions()) == 1

        client = simple_client(
            "mongodb+srv://test1.test.build.10gen.cc/?srvMaxHosts=1", srvmaxhosts=2
        )
        assert len(client.topology_description.server_descriptions()) == 2

    @pytest.mark.skipif(sys.platform == "win32", reason="Windows does not support SIGSTOP")
    @pytest.mark.usefixtures("require_sdam")
    @pytest.mark.usefixtures("require_sync")
    def test_sigstop_sigcont(self, client_context_fixture):
        test_dir = os.path.dirname(os.path.realpath(__file__))
        script = os.path.join(test_dir, "sigstop_sigcont.py")
        with subprocess.Popen(
            [sys.executable, script, client_context_fixture.uri],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        ) as p:
            time.sleep(1)
            os.kill(p.pid, signal.SIGSTOP)
            time.sleep(2)
            os.kill(p.pid, signal.SIGCONT)
            time.sleep(0.5)
            outs, _ = p.communicate(input=b"q\n", timeout=10)
            assert outs
            log_output = outs.decode("utf-8")
            assert "TEST STARTED" in log_output
            assert "ServerHeartbeatStartedEvent" in log_output
            assert "ServerHeartbeatSucceededEvent" in log_output
            assert "TEST COMPLETED" in log_output
            assert "ServerHeartbeatFailedEvent" not in log_output

    def _test_handshake(self, env_vars, expected_env, rs_or_single_client):
        with patch.dict("os.environ", env_vars):
            metadata = copy.deepcopy(_METADATA)
            if has_c():
                metadata["driver"]["name"] = "PyMongo|c"
            else:
                metadata["driver"]["name"] = "PyMongo"

            if expected_env is not None:
                metadata["env"] = expected_env

                if "AWS_REGION" not in env_vars:
                    os.environ["AWS_REGION"] = ""

            client = rs_or_single_client(serverSelectionTimeoutMS=10000)
            client.admin.command("ping")
            options = client.options
            assert options.pool_options.metadata == metadata

    def test_handshake_01_aws(self, rs_or_single_client):
        self._test_handshake(
            {
                "AWS_EXECUTION_ENV": "AWS_Lambda_python3.9",
                "AWS_REGION": "us-east-2",
                "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "1024",
            },
            {"name": "aws.lambda", "region": "us-east-2", "memory_mb": 1024},
            rs_or_single_client,
        )

    def test_handshake_02_azure(self, rs_or_single_client):
        self._test_handshake(
            {"FUNCTIONS_WORKER_RUNTIME": "python"},
            {"name": "azure.func"},
            rs_or_single_client,
        )

    def test_handshake_03_gcp(self, rs_or_single_client):
        # Regular case with environment variables.
        self._test_handshake(
            {
                "K_SERVICE": "servicename",
                "FUNCTION_MEMORY_MB": "1024",
                "FUNCTION_TIMEOUT_SEC": "60",
                "FUNCTION_REGION": "us-central1",
            },
            {"name": "gcp.func", "region": "us-central1", "memory_mb": 1024, "timeout_sec": 60},
            rs_or_single_client,
        )

        # Extra case for FUNCTION_NAME.
        self._test_handshake(
            {
                "FUNCTION_NAME": "funcname",
                "FUNCTION_MEMORY_MB": "1024",
                "FUNCTION_TIMEOUT_SEC": "60",
                "FUNCTION_REGION": "us-central1",
            },
            {"name": "gcp.func", "region": "us-central1", "memory_mb": 1024, "timeout_sec": 60},
            rs_or_single_client,
        )

    def test_handshake_04_vercel(self, rs_or_single_client):
        self._test_handshake(
            {"VERCEL": "1", "VERCEL_REGION": "cdg1"},
            {"name": "vercel", "region": "cdg1"},
            rs_or_single_client,
        )

    def test_handshake_05_multiple(self, rs_or_single_client):
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.9", "FUNCTIONS_WORKER_RUNTIME": "python"},
            None,
            rs_or_single_client,
        )

        self._test_handshake(
            {"FUNCTIONS_WORKER_RUNTIME": "python", "K_SERVICE": "servicename"},
            None,
            rs_or_single_client,
        )

        self._test_handshake({"K_SERVICE": "servicename", "VERCEL": "1"}, None, rs_or_single_client)

    def test_handshake_06_region_too_long(self, rs_or_single_client):
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.9", "AWS_REGION": "a" * 512},
            {"name": "aws.lambda"},
            rs_or_single_client,
        )

    def test_handshake_07_memory_invalid_int(self, rs_or_single_client):
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.9", "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "big"},
            {"name": "aws.lambda"},
            rs_or_single_client,
        )

    def test_handshake_08_invalid_aws_ec2(self, rs_or_single_client):
        # AWS_EXECUTION_ENV needs to start with "AWS_Lambda_".
        self._test_handshake({"AWS_EXECUTION_ENV": "EC2"}, None, rs_or_single_client)

    def test_handshake_09_container_with_provider(self, rs_or_single_client):
        self._test_handshake(
            {
                ENV_VAR_K8S: "1",
                "AWS_LAMBDA_RUNTIME_API": "1",
                "AWS_REGION": "us-east-1",
                "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "256",
            },
            {
                "container": {"orchestrator": "kubernetes"},
                "name": "aws.lambda",
                "region": "us-east-1",
                "memory_mb": 256,
            },
            rs_or_single_client,
        )

    def test_dict_hints(self, client_context_fixture):
        client_context_fixture.client.db.t.find(hint={"x": 1})

    def test_dict_hints_sort(self, client_context_fixture):
        result = client_context_fixture.client.db.t.find()
        result.sort({"x": 1})
        client_context_fixture.client.db.t.find(sort={"x": 1})

    def test_dict_hints_create_index(self, client_context_fixture):
        client_context_fixture.client.db.t.create_index({"x": pymongo.ASCENDING})

    def test_legacy_java_uuid_roundtrip(self, client_context_fixture):
        data = BinaryData.java_data
        docs = bson.decode_all(data, CodecOptions(SON[str, Any], False, JAVA_LEGACY))

        client_context_fixture.client.pymongo_test.drop_collection("java_uuid")
        db = client_context_fixture.client.pymongo_test
        coll = db.get_collection("java_uuid", CodecOptions(uuid_representation=JAVA_LEGACY))

        coll.insert_many(docs)
        assert coll.count_documents({}) == 5
        for d in coll.find():
            assert d["newguid"] == uuid.UUID(d["newguidstring"])

        coll = db.get_collection("java_uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        for d in coll.find():
            assert d["newguid"] != d["newguidstring"]
        client_context_fixture.client.pymongo_test.drop_collection("java_uuid")

    def test_legacy_csharp_uuid_roundtrip(self, client_context_fixture):
        data = BinaryData.csharp_data
        docs = bson.decode_all(data, CodecOptions(SON[str, Any], False, CSHARP_LEGACY))

        client_context_fixture.client.pymongo_test.drop_collection("csharp_uuid")
        db = client_context_fixture.client.pymongo_test
        coll = db.get_collection("csharp_uuid", CodecOptions(uuid_representation=CSHARP_LEGACY))

        coll.insert_many(docs)
        assert coll.count_documents({}) == 5
        for d in coll.find():
            assert d["newguid"] == uuid.UUID(d["newguidstring"])

        coll = db.get_collection("csharp_uuid", CodecOptions(uuid_representation=PYTHON_LEGACY))
        for d in coll.find():
            assert d["newguid"] != d["newguidstring"]
        client_context_fixture.client.pymongo_test.drop_collection("csharp_uuid")

    def test_uri_to_uuid(self, single_client):
        uri = "mongodb://foo/?uuidrepresentation=csharpLegacy"
        client = single_client(uri, connect=False)
        assert client.pymongo_test.test.codec_options.uuid_representation == CSHARP_LEGACY

    def test_uuid_queries(self, client_context_fixture):
        db = client_context_fixture.client.pymongo_test
        coll = db.test
        coll.drop()

        uu = uuid.uuid4()
        coll.insert_one({"uuid": Binary(uu.bytes, 3)})
        assert coll.count_documents({}) == 1

        coll = db.get_collection(
            "test", CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
        )
        assert coll.count_documents({"uuid": uu}) == 0
        coll.insert_one({"uuid": uu})
        assert coll.count_documents({}) == 2
        docs = coll.find({"uuid": uu}).to_list(length=1)
        assert len(docs) == 1
        assert docs[0]["uuid"] == uu

        uu_legacy = Binary.from_uuid(uu, UuidRepresentation.PYTHON_LEGACY)
        predicate = {"uuid": {"$in": [uu, uu_legacy]}}
        assert coll.count_documents(predicate) == 2
        docs = coll.find(predicate).to_list(length=2)
        assert len(docs) == 2
        coll.drop()


@pytest.mark.usefixtures("require_no_mongos")
@pytest.mark.usefixtures("require_integration")
@pytest.mark.integration
class TestExhaustCursor(PyMongoTestCasePyTest):
    def test_exhaust_query_server_error(self, rs_or_single_client):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = connected(rs_or_single_client(maxPoolSize=1))

        collection = client.pymongo_test.test
        pool = get_pool(client)
        conn = one(pool.conns)

        # This will cause OperationFailure in all mongo versions since
        # the value for $orderby must be a document.
        cursor = collection.find(
            SON([("$query", {}), ("$orderby", True)]), cursor_type=CursorType.EXHAUST
        )

        with pytest.raises(OperationFailure):
            cursor.next()
        assert not conn.closed

        # The socket was checked in and the semaphore was decremented.
        assert conn in pool.conns
        assert pool.requests == 0

    def test_exhaust_getmore_server_error(self, rs_or_single_client):
        # When doing a getmore on an exhaust cursor, the socket stays checked
        # out on success but it's checked in on error to avoid semaphore leaks.
        client = rs_or_single_client(maxPoolSize=1)
        collection = client.pymongo_test.test
        collection.drop()

        collection.insert_many([{} for _ in range(200)])

        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.
        conn = one(pool.conns)

        cursor = collection.find(cursor_type=CursorType.EXHAUST)

        # Initial query succeeds.
        cursor.next()

        # Cause a server error on getmore.
        def receive_message(request_id):
            # Discard the actual server response.
            Connection.receive_message(conn, request_id)

            # responseFlags bit 1 is QueryFailure.
            msg = struct.pack("<iiiii", 1 << 1, 0, 0, 0, 0)
            msg += encode({"$err": "mock err", "code": 0})
            return message._OpReply.unpack(msg)

        conn.receive_message = receive_message
        with pytest.raises(OperationFailure):
            cursor.to_list()
        # Unpatch the instance.
        del conn.receive_message

        # The socket is returned to the pool and it still works.
        assert 200 == collection.count_documents({})
        assert conn in pool.conns

    def test_exhaust_query_network_error(self, rs_or_single_client):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = connected(rs_or_single_client(maxPoolSize=1, retryReads=False))
        collection = client.pymongo_test.test
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        # Cause a network error.
        conn = one(pool.conns)
        conn.conn.close()

        cursor = collection.find(cursor_type=CursorType.EXHAUST)
        with pytest.raises(ConnectionFailure):
            cursor.next()
        assert conn.closed

        # The socket was closed and the semaphore was decremented.
        assert conn not in pool.conns
        assert 0 == pool.requests

    def test_exhaust_getmore_network_error(self, rs_or_single_client):
        # When doing a getmore on an exhaust cursor, the socket stays checked
        # out on success but it's checked in on error to avoid semaphore leaks.
        client = rs_or_single_client(maxPoolSize=1)
        collection = client.pymongo_test.test
        collection.drop()
        collection.insert_many([{} for _ in range(200)])  # More than one batch.
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        cursor = collection.find(cursor_type=CursorType.EXHAUST)

        # Initial query succeeds.
        cursor.next()

        # Cause a network error.
        conn = cursor._sock_mgr.conn
        conn.conn.close()

        # A getmore fails.
        with pytest.raises(ConnectionFailure):
            cursor.to_list()
        assert conn.closed

        wait_until(
            lambda: len(client._kill_cursors_queue) == 0,
            "waited for all killCursor requests to complete",
        )
        # The socket was closed and the semaphore was decremented.
        assert conn not in pool.conns
        assert 0 == pool.requests

    @pytest.mark.usefixtures("require_sync")
    def test_gevent_task(self, client_context_fixture):
        if not gevent_monkey_patched():
            pytest.skip("Must be running monkey patched by gevent")
        from gevent import spawn

        def poller():
            while True:
                client_context_fixture.client.pymongo_test.test.insert_one({})

        task = spawn(poller)
        task.kill()
        assert task.dead

    @pytest.mark.usefixtures("require_sync")
    def test_gevent_timeout(self, rs_or_single_client):
        if not gevent_monkey_patched():
            pytest.skip("Must be running monkey patched by gevent")
        from gevent import Timeout, spawn

        client = rs_or_single_client(maxPoolSize=1)
        coll = client.pymongo_test.test
        coll.insert_one({})

        def contentious_task():
            # The 10 second timeout causes this test to fail without blocking
            # forever if a bug like PYTHON-2334 is reintroduced.
            with Timeout(10):
                coll.find_one({"$where": delay(1)})

        def timeout_task():
            with Timeout(0.5):
                try:
                    coll.find_one({})
                except Timeout:
                    pass

        ct = spawn(contentious_task)
        tt = spawn(timeout_task)
        tt.join(15)
        ct.join(15)
        assert tt.dead
        assert ct.dead
        assert tt.get() is None
        assert ct.get() is None

    @pytest.mark.usefixtures("require_sync")
    def test_gevent_timeout_when_creating_connection(self, rs_or_single_client):
        if not gevent_monkey_patched():
            pytest.skip("Must be running monkey patched by gevent")
        from gevent import Timeout, spawn

        client = rs_or_single_client()

        coll = client.pymongo_test.test
        pool = get_pool(client)

        # Patch the pool to delay the connect method.
        def delayed_connect(*args, **kwargs):
            time.sleep(3)
            return pool.__class__.connect(pool, *args, **kwargs)

        pool.connect = delayed_connect

        def timeout_task():
            with Timeout(1):
                try:
                    coll.find_one({})
                    return False
                except Timeout:
                    return True

        tt = spawn(timeout_task)
        tt.join(10)

        # Assert that we got our active_sockets count back
        assert pool.active_sockets == 0
        # Assert the greenlet is dead
        assert tt.dead
        # Assert that the Timeout was raised all the way to the try
        assert tt.get()
        # Unpatch the instance.
        del pool.connect


@pytest.mark.usefixtures("require_sync")
@pytest.mark.usefixtures("require_integration")
@pytest.mark.integration
class TestClientLazyConnect:
    """Test concurrent operations on a lazily-connecting MongoClient."""

    @pytest.fixture
    def _get_client(self, rs_or_single_client):
        return rs_or_single_client(connect=False)

    def test_insert_one(self, _get_client, client_context_fixture):
        def reset(collection):
            collection.drop()

        def insert_one(collection, _):
            collection.insert_one({})

        def test(collection):
            assert NTHREADS == collection.count_documents({})

        lazy_client_trial(reset, insert_one, test, _get_client, client_context_fixture)

    def test_update_one(self, _get_client, client_context_fixture):
        def reset(collection):
            collection.drop()
            collection.insert_one({"i": 0})

        # Update doc 10 times.
        def update_one(collection, _):
            collection.update_one({}, {"$inc": {"i": 1}})

        def test(collection):
            assert NTHREADS == collection.find_one()["i"]

        lazy_client_trial(reset, update_one, test, _get_client, client_context_fixture)

    def test_delete_one(self, _get_client, client_context_fixture):
        def reset(collection):
            collection.drop()
            collection.insert_many([{"i": i} for i in range(NTHREADS)])

        def delete_one(collection, i):
            collection.delete_one({"i": i})

        def test(collection):
            assert 0 == collection.count_documents({})

        lazy_client_trial(reset, delete_one, test, _get_client, client_context_fixture)

    def test_find_one(self, _get_client, client_context_fixture):
        results: list = []

        def reset(collection):
            collection.drop()
            collection.insert_one({})
            results[:] = []

        def find_one(collection, _):
            results.append(collection.find_one())

        def test(collection):
            assert NTHREADS == len(results)

        lazy_client_trial(reset, find_one, test, _get_client, client_context_fixture)


@pytest.mark.usefixtures("require_no_load_balancer")
@pytest.mark.unit
class TestMongoClientFailover:
    @pytest.fixture(scope="class", autouse=True)
    def _client_knobs(self):
        knobs = client_knobs(heartbeat_frequency=0.001, min_heartbeat_interval=0.001)
        knobs.enable()
        yield knobs
        knobs.disable()

    def test_discover_primary(self, mock_client):
        c = mock_client(
            standalones=[],
            members=["a:1", "b:2", "c:3"],
            mongoses=[],
            host="b:2",  # Pass a secondary.
            replicaSet="rs",
            heartbeatFrequencyMS=500,
        )

        wait_until(lambda: len(c.nodes) == 3, "connect")

        assert c.address == ("a", 1)
        # Fail over.
        c.kill_host("a:1")
        c.mock_primary = "b:2"

        def predicate():
            return (c.address) == ("b", 2)

        wait_until(predicate, "wait for server address to be updated")
        # a:1 not longer in nodes.
        assert len(c.nodes) < 3

    def test_reconnect(self, mock_client):
        # Verify the node list isn't forgotten during a network failure.
        c = mock_client(
            standalones=[],
            members=["a:1", "b:2", "c:3"],
            mongoses=[],
            host="b:2",  # Pass a secondary.
            replicaSet="rs",
            retryReads=False,
            serverSelectionTimeoutMS=1000,
        )

        wait_until(lambda: len(c.nodes) == 3, "connect")

        # Total failure.
        c.kill_host("a:1")
        c.kill_host("b:2")
        c.kill_host("c:3")

        # MongoClient discovers it's alone. The first attempt raises either
        # ServerSelectionTimeoutError or AutoReconnect (from
        # AsyncMockPool.get_socket).
        with pytest.raises(AutoReconnect):
            c.db.collection.find_one()

        # But it can reconnect.
        c.revive_host("a:1")
        (c._get_topology()).select_servers(
            writable_server_selector, _Op.TEST, server_selection_timeout=10
        )
        assert c.address == ("a", 1)

    def _test_network_error(self, mock_client, operation_callback):
        # Verify only the disconnected server is reset by a network failure.

        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = mock_client(
                standalones=[],
                members=["a:1", "b:2"],
                mongoses=[],
                host="a:1",
                replicaSet="rs",
                connect=False,
                retryReads=False,
                serverSelectionTimeoutMS=1000,
            )

            # Set host-specific information so we can test whether it is reset.
            c.set_wire_version_range("a:1", 2, MIN_SUPPORTED_WIRE_VERSION)
            c.set_wire_version_range("b:2", 2, MIN_SUPPORTED_WIRE_VERSION + 1)
            (c._get_topology()).select_servers(writable_server_selector, _Op.TEST)
            wait_until(lambda: len(c.nodes) == 2, "connect")

            c.kill_host("a:1")

            # MongoClient is disconnected from the primary. This raises either
            # ServerSelectionTimeoutError or AutoReconnect (from
            # MockPool.get_socket).
            with pytest.raises(AutoReconnect):
                operation_callback(c)

            # The primary's description is reset.
            server_a = (c._get_topology()).get_server_by_address(("a", 1))
            sd_a = server_a.description
            assert SERVER_TYPE.Unknown == sd_a.server_type
            assert 0 == sd_a.min_wire_version
            assert 0 == sd_a.max_wire_version

            # ...but not the secondary's.
            server_b = (c._get_topology()).get_server_by_address(("b", 2))
            sd_b = server_b.description
            assert sd_b.server_type == SERVER_TYPE.RSSecondary
            assert sd_b.min_wire_version == 2
            assert sd_b.max_wire_version == MIN_SUPPORTED_WIRE_VERSION + 1

    def test_network_error_on_query(self, mock_client):
        def callback(client):
            return client.db.collection.find_one()

        self._test_network_error(mock_client, callback)

    def test_network_error_on_insert(self, mock_client):
        def callback(client):
            return client.db.collection.insert_one({})

        self._test_network_error(mock_client, callback)

    def test_network_error_on_update(self, mock_client):
        def callback(client):
            return client.db.collection.update_one({}, {"$unset": "x"})

        self._test_network_error(mock_client, callback)

    def test_network_error_on_replace(self, mock_client):
        def callback(client):
            return client.db.collection.replace_one({}, {})

        self._test_network_error(mock_client, callback)

    def test_network_error_on_delete(self, mock_client):
        def callback(client):
            return client.db.collection.delete_many({})

        self._test_network_error(mock_client, callback)


@pytest.mark.usefixtures("require_integration")
@pytest.mark.integration
class TestClientPool:
    def test_rs_client_does_not_maintain_pool_to_arbiters(self, mock_client):
        listener = CMAPListener()
        c = mock_client(
            standalones=[],
            members=["a:1", "b:2", "c:3", "d:4"],
            mongoses=[],
            arbiters=["c:3"],  # c:3 is an arbiter.
            down_hosts=["d:4"],  # d:4 is unreachable.
            host=["a:1", "b:2", "c:3", "d:4"],
            replicaSet="rs",
            minPoolSize=1,  # minPoolSize
            event_listeners=[listener],
        )

        wait_until(lambda: len(c.nodes) == 3, "connect")
        assert c.address == ("a", 1)
        assert c.arbiters == {("c", 3)}
        listener.wait_for_event(monitoring.ConnectionReadyEvent, 2)
        assert listener.event_count(monitoring.ConnectionCreatedEvent) == 2
        arbiter = c._topology.get_server_by_address(("c", 3))
        assert not arbiter.pool.conns
        arbiter = c._topology.get_server_by_address(("d", 4))
        assert not arbiter.pool.conns
        assert listener.event_count(monitoring.PoolReadyEvent) == 2

    def test_direct_client_maintains_pool_to_arbiter(self, mock_client):
        listener = CMAPListener()
        c = mock_client(
            standalones=[],
            members=["a:1", "b:2", "c:3"],
            mongoses=[],
            arbiters=["c:3"],  # c:3 is an arbiter.
            host="c:3",
            directConnection=True,
            minPoolSize=1,  # minPoolSize
            event_listeners=[listener],
        )

        wait_until(lambda: len(c.nodes) == 1, "connect")
        assert c.address == ("c", 3)
        listener.wait_for_event(monitoring.ConnectionReadyEvent, 1)
        assert listener.event_count(monitoring.ConnectionCreatedEvent) == 1
        arbiter = c._topology.get_server_by_address(("c", 3))
        assert len(arbiter.pool.conns) == 1
        assert listener.event_count(monitoring.PoolReadyEvent) == 1

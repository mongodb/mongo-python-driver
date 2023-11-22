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
import contextlib
import copy
import datetime
import gc
import os
import re
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from typing import Iterable, Type, no_type_check
from unittest import mock
from unittest.mock import patch

sys.path[0:0] = [""]

from test import (
    HAVE_IPADDRESS,
    IntegrationTest,
    MockClientTest,
    SkipTest,
    client_context,
    client_knobs,
    db_pwd,
    db_user,
    unittest,
)
from test.pymongo_mocks import MockClient
from test.utils import (
    NTHREADS,
    CMAPListener,
    FunctionCallRecorder,
    assertRaisesExactly,
    connected,
    delay,
    get_pool,
    gevent_monkey_patched,
    is_greenthread_patched,
    lazy_client_trial,
    one,
    remove_all_users,
    rs_client,
    rs_or_single_client,
    rs_or_single_client_noauth,
    single_client,
    wait_until,
)

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
from pymongo.command_cursor import CommandCursor
from pymongo.common import _UUID_REPRESENTATIONS, CONNECT_TIMEOUT
from pymongo.compression_support import _HAVE_SNAPPY, _HAVE_ZSTD
from pymongo.cursor import Cursor, CursorType
from pymongo.database import Database
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
from pymongo.mongo_client import MongoClient
from pymongo.monitoring import ServerHeartbeatListener, ServerHeartbeatStartedEvent
from pymongo.pool import _METADATA, DOCKER_ENV_PATH, ENV_VAR_K8S, Connection, PoolOptions
from pymongo.read_preferences import ReadPreference
from pymongo.server_description import ServerDescription
from pymongo.server_selectors import readable_server_selector, writable_server_selector
from pymongo.server_type import SERVER_TYPE
from pymongo.settings import TOPOLOGY_TYPE
from pymongo.srv_resolver import _HAVE_DNSPYTHON
from pymongo.topology import _ErrorContext
from pymongo.topology_description import TopologyDescription
from pymongo.write_concern import WriteConcern


class ClientUnitTest(unittest.TestCase):
    """MongoClient tests that don't require a server."""

    client: MongoClient

    @classmethod
    def setUpClass(cls):
        cls.client = rs_or_single_client(connect=False, serverSelectionTimeoutMS=100)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_keyword_arg_defaults(self):
        client = MongoClient(
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

        options = client._MongoClient__options
        pool_opts = options.pool_options
        self.assertEqual(None, pool_opts.socket_timeout)
        # socket.Socket.settimeout takes a float in seconds
        self.assertEqual(20.0, pool_opts.connect_timeout)
        self.assertEqual(None, pool_opts.wait_queue_timeout)
        self.assertEqual(None, pool_opts._ssl_context)
        self.assertEqual(None, options.replica_set_name)
        self.assertEqual(ReadPreference.PRIMARY, client.read_preference)
        self.assertAlmostEqual(12, client.options.server_selection_timeout)

    def test_connect_timeout(self):
        client = MongoClient(connect=False, connectTimeoutMS=None, socketTimeoutMS=None)
        pool_opts = client._MongoClient__options.pool_options
        self.assertEqual(None, pool_opts.socket_timeout)
        self.assertEqual(None, pool_opts.connect_timeout)
        client = MongoClient(connect=False, connectTimeoutMS=0, socketTimeoutMS=0)
        pool_opts = client._MongoClient__options.pool_options
        self.assertEqual(None, pool_opts.socket_timeout)
        self.assertEqual(None, pool_opts.connect_timeout)
        client = MongoClient(
            "mongodb://localhost/?connectTimeoutMS=0&socketTimeoutMS=0", connect=False
        )
        pool_opts = client._MongoClient__options.pool_options
        self.assertEqual(None, pool_opts.socket_timeout)
        self.assertEqual(None, pool_opts.connect_timeout)

    def test_types(self):
        self.assertRaises(TypeError, MongoClient, 1)
        self.assertRaises(TypeError, MongoClient, 1.14)
        self.assertRaises(TypeError, MongoClient, "localhost", "27017")
        self.assertRaises(TypeError, MongoClient, "localhost", 1.14)
        self.assertRaises(TypeError, MongoClient, "localhost", [])

        self.assertRaises(ConfigurationError, MongoClient, [])

    def test_max_pool_size_zero(self):
        MongoClient(maxPoolSize=0)

    def test_uri_detection(self):
        self.assertRaises(ConfigurationError, MongoClient, "/foo")
        self.assertRaises(ConfigurationError, MongoClient, "://")
        self.assertRaises(ConfigurationError, MongoClient, "foo/")

    def test_get_db(self):
        def make_db(base, name):
            return base[name]

        self.assertRaises(InvalidName, make_db, self.client, "")
        self.assertRaises(InvalidName, make_db, self.client, "te$t")
        self.assertRaises(InvalidName, make_db, self.client, "te.t")
        self.assertRaises(InvalidName, make_db, self.client, "te\\t")
        self.assertRaises(InvalidName, make_db, self.client, "te/t")
        self.assertRaises(InvalidName, make_db, self.client, "te st")

        self.assertTrue(isinstance(self.client.test, Database))
        self.assertEqual(self.client.test, self.client["test"])
        self.assertEqual(self.client.test, Database(self.client, "test"))

    def test_get_database(self):
        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        db = self.client.get_database("foo", codec_options, ReadPreference.SECONDARY, write_concern)
        self.assertEqual("foo", db.name)
        self.assertEqual(codec_options, db.codec_options)
        self.assertEqual(ReadPreference.SECONDARY, db.read_preference)
        self.assertEqual(write_concern, db.write_concern)

    def test_getattr(self):
        self.assertTrue(isinstance(self.client["_does_not_exist"], Database))

        with self.assertRaises(AttributeError) as context:
            self.client._does_not_exist

        # Message should be:
        # "AttributeError: MongoClient has no attribute '_does_not_exist'. To
        # access the _does_not_exist database, use client['_does_not_exist']".
        self.assertIn("has no attribute '_does_not_exist'", str(context.exception))

    def test_iteration(self):
        client = self.client
        if "PyPy" in sys.version and sys.version_info < (3, 8, 15):
            msg = "'NoneType' object is not callable"
        else:
            msg = "'MongoClient' object is not iterable"
        # Iteration fails
        with self.assertRaisesRegex(TypeError, msg):
            for _ in client:  # type: ignore[misc] # error: "None" not callable  [misc]
                break
        # Index fails
        with self.assertRaises(TypeError):
            _ = client[0]
        # next fails
        with self.assertRaisesRegex(TypeError, "'MongoClient' object is not iterable"):
            _ = next(client)
        # .next() fails
        with self.assertRaisesRegex(TypeError, "'MongoClient' object is not iterable"):
            _ = client.next()
        # Do not implement typing.Iterable.
        self.assertNotIsInstance(client, Iterable)

    def test_get_default_database(self):
        c = rs_or_single_client(
            "mongodb://%s:%d/foo" % (client_context.host, client_context.port), connect=False
        )
        self.assertEqual(Database(c, "foo"), c.get_default_database())
        # Test that default doesn't override the URI value.
        self.assertEqual(Database(c, "foo"), c.get_default_database("bar"))

        codec_options = CodecOptions(tz_aware=True)
        write_concern = WriteConcern(w=2, j=True)
        db = c.get_default_database(None, codec_options, ReadPreference.SECONDARY, write_concern)
        self.assertEqual("foo", db.name)
        self.assertEqual(codec_options, db.codec_options)
        self.assertEqual(ReadPreference.SECONDARY, db.read_preference)
        self.assertEqual(write_concern, db.write_concern)

        c = rs_or_single_client(
            "mongodb://%s:%d/" % (client_context.host, client_context.port), connect=False
        )
        self.assertEqual(Database(c, "foo"), c.get_default_database("foo"))

    def test_get_default_database_error(self):
        # URI with no database.
        c = rs_or_single_client(
            "mongodb://%s:%d/" % (client_context.host, client_context.port), connect=False
        )
        self.assertRaises(ConfigurationError, c.get_default_database)

    def test_get_default_database_with_authsource(self):
        # Ensure we distinguish database name from authSource.
        uri = "mongodb://%s:%d/foo?authSource=src" % (client_context.host, client_context.port)
        c = rs_or_single_client(uri, connect=False)
        self.assertEqual(Database(c, "foo"), c.get_default_database())

    def test_get_database_default(self):
        c = rs_or_single_client(
            "mongodb://%s:%d/foo" % (client_context.host, client_context.port), connect=False
        )
        self.assertEqual(Database(c, "foo"), c.get_database())

    def test_get_database_default_error(self):
        # URI with no database.
        c = rs_or_single_client(
            "mongodb://%s:%d/" % (client_context.host, client_context.port), connect=False
        )
        self.assertRaises(ConfigurationError, c.get_database)

    def test_get_database_default_with_authsource(self):
        # Ensure we distinguish database name from authSource.
        uri = "mongodb://%s:%d/foo?authSource=src" % (client_context.host, client_context.port)
        c = rs_or_single_client(uri, connect=False)
        self.assertEqual(Database(c, "foo"), c.get_database())

    def test_primary_read_pref_with_tags(self):
        # No tags allowed with "primary".
        with self.assertRaises(ConfigurationError):
            MongoClient("mongodb://host/?readpreferencetags=dc:east")

        with self.assertRaises(ConfigurationError):
            MongoClient("mongodb://host/?readpreference=primary&readpreferencetags=dc:east")

    def test_read_preference(self):
        c = rs_or_single_client(
            "mongodb://host", connect=False, readpreference=ReadPreference.NEAREST.mongos_mode
        )
        self.assertEqual(c.read_preference, ReadPreference.NEAREST)

    def test_metadata(self):
        metadata = copy.deepcopy(_METADATA)
        metadata["application"] = {"name": "foobar"}
        client = MongoClient("mongodb://foo:27017/?appname=foobar&connect=false")
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata, metadata)
        client = MongoClient("foo", 27017, appname="foobar", connect=False)
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata, metadata)
        # No error
        MongoClient(appname="x" * 128)
        self.assertRaises(ValueError, MongoClient, appname="x" * 129)
        # Bad "driver" options.
        self.assertRaises(TypeError, DriverInfo, "Foo", 1, "a")
        self.assertRaises(TypeError, DriverInfo, version="1", platform="a")
        self.assertRaises(TypeError, DriverInfo)
        self.assertRaises(TypeError, MongoClient, driver=1)
        self.assertRaises(TypeError, MongoClient, driver="abc")
        self.assertRaises(TypeError, MongoClient, driver=("Foo", "1", "a"))
        # Test appending to driver info.
        metadata["driver"]["name"] = "PyMongo|FooDriver"
        metadata["driver"]["version"] = "{}|1.2.3".format(_METADATA["driver"]["version"])
        client = MongoClient(
            "foo",
            27017,
            appname="foobar",
            driver=DriverInfo("FooDriver", "1.2.3", None),
            connect=False,
        )
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata, metadata)
        metadata["platform"] = "{}|FooPlatform".format(_METADATA["platform"])
        client = MongoClient(
            "foo",
            27017,
            appname="foobar",
            driver=DriverInfo("FooDriver", "1.2.3", "FooPlatform"),
            connect=False,
        )
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata, metadata)

    @mock.patch.dict("os.environ", {ENV_VAR_K8S: "1"})
    def test_container_metadata(self):
        metadata = copy.deepcopy(_METADATA)
        metadata["env"] = {}
        metadata["env"]["container"] = {"orchestrator": "kubernetes"}
        client = MongoClient("mongodb://foo:27017/?appname=foobar&connect=false")
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata["env"], metadata["env"])

    def test_kwargs_codec_options(self):
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
        c = MongoClient(
            document_class=document_class,
            type_registry=type_registry,
            tz_aware=tz_aware,
            uuidrepresentation=uuid_representation_label,
            unicode_decode_error_handler=unicode_decode_error_handler,
            tzinfo=tzinfo,
            connect=False,
        )

        self.assertEqual(c.codec_options.document_class, document_class)
        self.assertEqual(c.codec_options.type_registry, type_registry)
        self.assertEqual(c.codec_options.tz_aware, tz_aware)
        self.assertEqual(
            c.codec_options.uuid_representation, _UUID_REPRESENTATIONS[uuid_representation_label]
        )
        self.assertEqual(c.codec_options.unicode_decode_error_handler, unicode_decode_error_handler)
        self.assertEqual(c.codec_options.tzinfo, tzinfo)

    def test_uri_codec_options(self):
        # Ensure codec options are passed in correctly
        uuid_representation_label = "javaLegacy"
        unicode_decode_error_handler = "ignore"
        datetime_conversion = "DATETIME_CLAMP"
        uri = (
            "mongodb://%s:%d/foo?tz_aware=true&uuidrepresentation="
            "%s&unicode_decode_error_handler=%s"
            "&datetime_conversion=%s"
            % (
                client_context.host,
                client_context.port,
                uuid_representation_label,
                unicode_decode_error_handler,
                datetime_conversion,
            )
        )
        c = MongoClient(uri, connect=False)

        self.assertEqual(c.codec_options.tz_aware, True)
        self.assertEqual(
            c.codec_options.uuid_representation, _UUID_REPRESENTATIONS[uuid_representation_label]
        )
        self.assertEqual(c.codec_options.unicode_decode_error_handler, unicode_decode_error_handler)
        self.assertEqual(
            c.codec_options.datetime_conversion, DatetimeConversion[datetime_conversion]
        )

        # Change the passed datetime_conversion to a number and re-assert.
        uri = uri.replace(datetime_conversion, f"{int(DatetimeConversion[datetime_conversion])}")
        c = MongoClient(uri, connect=False)

        self.assertEqual(
            c.codec_options.datetime_conversion, DatetimeConversion[datetime_conversion]
        )

    def test_uri_option_precedence(self):
        # Ensure kwarg options override connection string options.
        uri = "mongodb://localhost/?ssl=true&replicaSet=name&readPreference=primary"
        c = MongoClient(uri, ssl=False, replicaSet="newname", readPreference="secondaryPreferred")
        clopts = c._MongoClient__options
        opts = clopts._options

        self.assertEqual(opts["tls"], False)
        self.assertEqual(clopts.replica_set_name, "newname")
        self.assertEqual(clopts.read_preference, ReadPreference.SECONDARY_PREFERRED)

    @unittest.skipUnless(_HAVE_DNSPYTHON, "DNS-related tests need dnspython to be installed")
    def test_connection_timeout_ms_propagates_to_DNS_resolver(self):
        # Patch the resolver.
        from pymongo.srv_resolver import _resolve

        patched_resolver = FunctionCallRecorder(_resolve)
        pymongo.srv_resolver._resolve = patched_resolver

        def reset_resolver():
            pymongo.srv_resolver._resolve = _resolve

        self.addCleanup(reset_resolver)

        # Setup.
        base_uri = "mongodb+srv://test5.test.build.10gen.cc"
        connectTimeoutMS = 5000
        expected_kw_value = 5.0
        uri_with_timeout = base_uri + "/?connectTimeoutMS=6000"
        expected_uri_value = 6.0

        def test_scenario(args, kwargs, expected_value):
            patched_resolver.reset()
            MongoClient(*args, **kwargs)
            for _, kw in patched_resolver.call_list():
                self.assertAlmostEqual(kw["lifetime"], expected_value)

        # No timeout specified.
        test_scenario((base_uri,), {}, CONNECT_TIMEOUT)

        # Timeout only specified in connection string.
        test_scenario((uri_with_timeout,), {}, expected_uri_value)

        # Timeout only specified in keyword arguments.
        kwarg = {"connectTimeoutMS": connectTimeoutMS}
        test_scenario((base_uri,), kwarg, expected_kw_value)

        # Timeout specified in both kwargs and connection string.
        test_scenario((uri_with_timeout,), kwarg, expected_kw_value)

    def test_uri_security_options(self):
        # Ensure that we don't silently override security-related options.
        with self.assertRaises(InvalidURI):
            MongoClient("mongodb://localhost/?ssl=true", tls=False, connect=False)

        # Matching SSL and TLS options should not cause errors.
        c = MongoClient("mongodb://localhost/?ssl=false", tls=False, connect=False)
        self.assertEqual(c._MongoClient__options._options["tls"], False)

        # Conflicting tlsInsecure options should raise an error.
        with self.assertRaises(InvalidURI):
            MongoClient(
                "mongodb://localhost/?tlsInsecure=true",
                connect=False,
                tlsAllowInvalidHostnames=True,
            )

        # Conflicting legacy tlsInsecure options should also raise an error.
        with self.assertRaises(InvalidURI):
            MongoClient(
                "mongodb://localhost/?tlsInsecure=true",
                connect=False,
                tlsAllowInvalidCertificates=False,
            )

        # Conflicting kwargs should raise InvalidURI
        with self.assertRaises(InvalidURI):
            MongoClient(ssl=True, tls=False)

    def test_event_listeners(self):
        c = MongoClient(event_listeners=[], connect=False)
        self.assertEqual(c.options.event_listeners, [])
        listeners = [
            event_loggers.CommandLogger(),
            event_loggers.HeartbeatLogger(),
            event_loggers.ServerLogger(),
            event_loggers.TopologyLogger(),
            event_loggers.ConnectionPoolLogger(),
        ]
        c = MongoClient(event_listeners=listeners, connect=False)
        self.assertEqual(c.options.event_listeners, listeners)

    def test_client_options(self):
        c = MongoClient(connect=False)
        self.assertIsInstance(c.options, ClientOptions)
        self.assertIsInstance(c.options.pool_options, PoolOptions)
        self.assertEqual(c.options.server_selection_timeout, 30)
        self.assertEqual(c.options.pool_options.max_idle_time_seconds, None)
        self.assertIsInstance(c.options.retry_writes, bool)
        self.assertIsInstance(c.options.retry_reads, bool)

    def test_validate_suggestion(self):
        """Validate kwargs in constructor."""
        for typo in ["auth", "Auth", "AUTH"]:
            expected = f"Unknown option: {typo}. Did you mean one of (authsource, authmechanism, authoidcallowedhosts) or maybe a camelCase version of one? Refer to docstring."
            expected = re.escape(expected)
            with self.assertRaisesRegex(ConfigurationError, expected):
                MongoClient(**{typo: "standard"})  # type: ignore[arg-type]


class TestClient(IntegrationTest):
    def test_multiple_uris(self):
        with self.assertRaises(ConfigurationError):
            MongoClient(
                host=[
                    "mongodb+srv://cluster-a.abc12.mongodb.net",
                    "mongodb+srv://cluster-b.abc12.mongodb.net",
                    "mongodb+srv://cluster-c.abc12.mongodb.net",
                ]
            )

    def test_max_idle_time_reaper_default(self):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper doesn't remove connections when maxIdleTimeMS not set
            client = rs_or_single_client()
            server = client._get_topology().select_server(readable_server_selector)
            with server._pool.checkout() as conn:
                pass
            self.assertEqual(1, len(server._pool.conns))
            self.assertTrue(conn in server._pool.conns)
            client.close()

    def test_max_idle_time_reaper_removes_stale_minPoolSize(self):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper removes idle socket and replaces it with a new one
            client = rs_or_single_client(maxIdleTimeMS=500, minPoolSize=1)
            server = client._get_topology().select_server(readable_server_selector)
            with server._pool.checkout() as conn:
                pass
            # When the reaper runs at the same time as the get_socket, two
            # connections could be created and checked into the pool.
            self.assertGreaterEqual(len(server._pool.conns), 1)
            wait_until(lambda: conn not in server._pool.conns, "remove stale socket")
            wait_until(lambda: len(server._pool.conns) >= 1, "replace stale socket")
            client.close()

    def test_max_idle_time_reaper_does_not_exceed_maxPoolSize(self):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper respects maxPoolSize when adding new connections.
            client = rs_or_single_client(maxIdleTimeMS=500, minPoolSize=1, maxPoolSize=1)
            server = client._get_topology().select_server(readable_server_selector)
            with server._pool.checkout() as conn:
                pass
            # When the reaper runs at the same time as the get_socket,
            # maxPoolSize=1 should prevent two connections from being created.
            self.assertEqual(1, len(server._pool.conns))
            wait_until(lambda: conn not in server._pool.conns, "remove stale socket")
            wait_until(lambda: len(server._pool.conns) == 1, "replace stale socket")
            client.close()

    def test_max_idle_time_reaper_removes_stale(self):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper has removed idle socket and NOT replaced it
            client = rs_or_single_client(maxIdleTimeMS=500)
            server = client._get_topology().select_server(readable_server_selector)
            with server._pool.checkout() as conn_one:
                pass
            # Assert that the pool does not close connections prematurely.
            time.sleep(0.300)
            with server._pool.checkout() as conn_two:
                pass
            self.assertIs(conn_one, conn_two)
            wait_until(
                lambda: len(server._pool.conns) == 0,
                "stale socket reaped and new one NOT added to the pool",
            )
            client.close()

    def test_min_pool_size(self):
        with client_knobs(kill_cursor_frequency=0.1):
            client = rs_or_single_client()
            server = client._get_topology().select_server(readable_server_selector)
            self.assertEqual(0, len(server._pool.conns))

            # Assert that pool started up at minPoolSize
            client = rs_or_single_client(minPoolSize=10)
            server = client._get_topology().select_server(readable_server_selector)
            wait_until(
                lambda: len(server._pool.conns) == 10, "pool initialized with 10 connections"
            )

            # Assert that if a socket is closed, a new one takes its place
            with server._pool.checkout() as conn:
                conn.close_conn(None)
            wait_until(
                lambda: len(server._pool.conns) == 10,
                "a closed socket gets replaced from the pool",
            )
            self.assertFalse(conn in server._pool.conns)

    def test_max_idle_time_checkout(self):
        # Use high frequency to test _get_socket_no_auth.
        with client_knobs(kill_cursor_frequency=99999999):
            client = rs_or_single_client(maxIdleTimeMS=500)
            server = client._get_topology().select_server(readable_server_selector)
            with server._pool.checkout() as conn:
                pass
            self.assertEqual(1, len(server._pool.conns))
            time.sleep(1)  # Sleep so that the socket becomes stale.

            with server._pool.checkout() as new_con:
                self.assertNotEqual(conn, new_con)
            self.assertEqual(1, len(server._pool.conns))
            self.assertFalse(conn in server._pool.conns)
            self.assertTrue(new_con in server._pool.conns)

            # Test that connections are reused if maxIdleTimeMS is not set.
            client = rs_or_single_client()
            server = client._get_topology().select_server(readable_server_selector)
            with server._pool.checkout() as conn:
                pass
            self.assertEqual(1, len(server._pool.conns))
            time.sleep(1)
            with server._pool.checkout() as new_con:
                self.assertEqual(conn, new_con)
            self.assertEqual(1, len(server._pool.conns))

    def test_constants(self):
        """This test uses MongoClient explicitly to make sure that host and
        port are not overloaded.
        """
        host, port = client_context.host, client_context.port
        kwargs: dict = client_context.default_client_options.copy()
        if client_context.auth_enabled:
            kwargs["username"] = db_user
            kwargs["password"] = db_pwd

        # Set bad defaults.
        MongoClient.HOST = "somedomainthatdoesntexist.org"
        MongoClient.PORT = 123456789
        with self.assertRaises(AutoReconnect):
            connected(MongoClient(serverSelectionTimeoutMS=10, **kwargs))

        # Override the defaults. No error.
        connected(MongoClient(host, port, **kwargs))

        # Set good defaults.
        MongoClient.HOST = host
        MongoClient.PORT = port

        # No error.
        connected(MongoClient(**kwargs))

    def test_init_disconnected(self):
        host, port = client_context.host, client_context.port
        c = rs_or_single_client(connect=False)
        # is_primary causes client to block until connected
        self.assertIsInstance(c.is_primary, bool)

        c = rs_or_single_client(connect=False)
        self.assertIsInstance(c.is_mongos, bool)
        c = rs_or_single_client(connect=False)
        self.assertIsInstance(c.options.pool_options.max_pool_size, int)
        self.assertIsInstance(c.nodes, frozenset)

        c = rs_or_single_client(connect=False)
        self.assertEqual(c.codec_options, CodecOptions())
        c = rs_or_single_client(connect=False)
        self.assertFalse(c.primary)
        self.assertFalse(c.secondaries)
        c = rs_or_single_client(connect=False)
        self.assertIsInstance(c.topology_description, TopologyDescription)
        self.assertEqual(c.topology_description, c._topology._description)
        self.assertIsNone(c.address)  # PYTHON-2981
        c.admin.command("ping")  # connect
        if client_context.is_rs:
            # The primary's host and port are from the replica set config.
            self.assertIsNotNone(c.address)
        else:
            self.assertEqual(c.address, (host, port))

        bad_host = "somedomainthatdoesntexist.org"
        c = MongoClient(bad_host, port, connectTimeoutMS=1, serverSelectionTimeoutMS=10)
        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth(self):
        uri = "mongodb://user:pass@somedomainthatdoesntexist"
        c = MongoClient(uri, connectTimeoutMS=1, serverSelectionTimeoutMS=10)
        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_equality(self):
        seed = "{}:{}".format(*list(self.client._topology_settings.seeds)[0])
        c = rs_or_single_client(seed, connect=False)
        self.addCleanup(c.close)
        self.assertEqual(client_context.client, c)
        # Explicitly test inequality
        self.assertFalse(client_context.client != c)

        c = rs_or_single_client("invalid.com", connect=False)
        self.addCleanup(c.close)
        self.assertNotEqual(client_context.client, c)
        self.assertTrue(client_context.client != c)
        # Seeds differ:
        self.assertNotEqual(MongoClient("a", connect=False), MongoClient("b", connect=False))
        # Same seeds but out of order still compares equal:
        self.assertEqual(
            MongoClient(["a", "b", "c"], connect=False), MongoClient(["c", "a", "b"], connect=False)
        )

    def test_hashable(self):
        seed = "{}:{}".format(*list(self.client._topology_settings.seeds)[0])
        c = rs_or_single_client(seed, connect=False)
        self.addCleanup(c.close)
        self.assertIn(c, {client_context.client})
        c = rs_or_single_client("invalid.com", connect=False)
        self.addCleanup(c.close)
        self.assertNotIn(c, {client_context.client})

    def test_host_w_port(self):
        with self.assertRaises(ValueError):
            connected(
                MongoClient(
                    f"{client_context.host}:1234567",
                    connectTimeoutMS=1,
                    serverSelectionTimeoutMS=10,
                )
            )

    def test_repr(self):
        # Used to test 'eval' below.
        import bson

        client = MongoClient(  # type: ignore[type-var]
            "mongodb://localhost:27017,localhost:27018/?replicaSet=replset"
            "&connectTimeoutMS=12345&w=1&wtimeoutms=100",
            connect=False,
            document_class=SON,
        )

        the_repr = repr(client)
        self.assertIn("MongoClient(host=", the_repr)
        self.assertIn("document_class=bson.son.SON, tz_aware=False, connect=False, ", the_repr)
        self.assertIn("connecttimeoutms=12345", the_repr)
        self.assertIn("replicaset='replset'", the_repr)
        self.assertIn("w=1", the_repr)
        self.assertIn("wtimeoutms=100", the_repr)

        self.assertEqual(eval(the_repr), client)

        client = MongoClient(
            "localhost:27017,localhost:27018",
            replicaSet="replset",
            connectTimeoutMS=12345,
            socketTimeoutMS=None,
            w=1,
            wtimeoutms=100,
            connect=False,
        )
        the_repr = repr(client)
        self.assertIn("MongoClient(host=", the_repr)
        self.assertIn("document_class=dict, tz_aware=False, connect=False, ", the_repr)
        self.assertIn("connecttimeoutms=12345", the_repr)
        self.assertIn("replicaset='replset'", the_repr)
        self.assertIn("sockettimeoutms=None", the_repr)
        self.assertIn("w=1", the_repr)
        self.assertIn("wtimeoutms=100", the_repr)

        self.assertEqual(eval(the_repr), client)

    def test_getters(self):
        wait_until(lambda: client_context.nodes == self.client.nodes, "find all nodes")

    def test_list_databases(self):
        cmd_docs = self.client.admin.command("listDatabases")["databases"]
        cursor = self.client.list_databases()
        self.assertIsInstance(cursor, CommandCursor)
        helper_docs = list(cursor)
        self.assertTrue(len(helper_docs) > 0)
        self.assertEqual(len(helper_docs), len(cmd_docs))
        # PYTHON-3529 Some fields may change between calls, just compare names.
        for helper_doc, cmd_doc in zip(helper_docs, cmd_docs):
            self.assertIs(type(helper_doc), dict)
            self.assertEqual(helper_doc.keys(), cmd_doc.keys())
        client = rs_or_single_client(document_class=SON)
        self.addCleanup(client.close)
        for doc in client.list_databases():
            self.assertIs(type(doc), dict)

        self.client.pymongo_test.test.insert_one({})
        cursor = self.client.list_databases(filter={"name": "admin"})
        docs = list(cursor)
        self.assertEqual(1, len(docs))
        self.assertEqual(docs[0]["name"], "admin")

        cursor = self.client.list_databases(nameOnly=True)
        for doc in cursor:
            self.assertEqual(["name"], list(doc))

    def test_list_database_names(self):
        self.client.pymongo_test.test.insert_one({"dummy": "object"})
        self.client.pymongo_test_mike.test.insert_one({"dummy": "object"})
        cmd_docs = self.client.admin.command("listDatabases")["databases"]
        cmd_names = [doc["name"] for doc in cmd_docs]

        db_names = self.client.list_database_names()
        self.assertTrue("pymongo_test" in db_names)
        self.assertTrue("pymongo_test_mike" in db_names)
        self.assertEqual(db_names, cmd_names)

    def test_drop_database(self):
        self.assertRaises(TypeError, self.client.drop_database, 5)
        self.assertRaises(TypeError, self.client.drop_database, None)

        self.client.pymongo_test.test.insert_one({"dummy": "object"})
        self.client.pymongo_test2.test.insert_one({"dummy": "object"})
        dbs = self.client.list_database_names()
        self.assertIn("pymongo_test", dbs)
        self.assertIn("pymongo_test2", dbs)
        self.client.drop_database("pymongo_test")

        if client_context.is_rs:
            wc_client = rs_or_single_client(w=len(client_context.nodes) + 1)
            with self.assertRaises(WriteConcernError):
                wc_client.drop_database("pymongo_test2")

        self.client.drop_database(self.client.pymongo_test2)
        dbs = self.client.list_database_names()
        self.assertNotIn("pymongo_test", dbs)
        self.assertNotIn("pymongo_test2", dbs)

    def test_close(self):
        test_client = rs_or_single_client()
        coll = test_client.pymongo_test.bar
        test_client.close()
        self.assertRaises(InvalidOperation, coll.count_documents, {})

    def test_close_kills_cursors(self):
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
        self.assertTrue(bool(next(cursor)))
        self.assertLess(cursor.retrieved, docs_inserted)

        # Open a command cursor and leave it open on the server.
        cursor = coll.aggregate([], batchSize=10)
        self.assertTrue(bool(next(cursor)))
        del cursor
        # Required for PyPy, Jython and other Python implementations that
        # don't use reference counting garbage collection.
        gc.collect()

        # Close the client and ensure the topology is closed.
        self.assertTrue(test_client._topology._opened)
        test_client.close()
        self.assertFalse(test_client._topology._opened)
        test_client = rs_or_single_client()
        # The killCursors task should not need to re-open the topology.
        test_client._process_periodic_tasks()
        self.assertTrue(test_client._topology._opened)

    def test_close_stops_kill_cursors_thread(self):
        client = rs_client()
        client.test.test.find_one()
        self.assertFalse(client._kill_cursors_executor._stopped)

        # Closing the client should stop the thread.
        client.close()
        self.assertTrue(client._kill_cursors_executor._stopped)

        # Reusing the closed client should raise an InvalidOperation error.
        self.assertRaises(InvalidOperation, client.admin.command, "ping")
        # Thread is still stopped.
        self.assertTrue(client._kill_cursors_executor._stopped)

    def test_uri_connect_option(self):
        # Ensure that topology is not opened if connect=False.
        client = rs_client(connect=False)
        self.assertFalse(client._topology._opened)

        # Ensure kill cursors thread has not been started.
        kc_thread = client._kill_cursors_executor._thread
        self.assertFalse(kc_thread and kc_thread.is_alive())

        # Using the client should open topology and start the thread.
        client.admin.command("ping")
        self.assertTrue(client._topology._opened)
        kc_thread = client._kill_cursors_executor._thread
        self.assertTrue(kc_thread and kc_thread.is_alive())

        # Tear down.
        client.close()

    def test_close_does_not_open_servers(self):
        client = rs_client(connect=False)
        topology = client._topology
        self.assertEqual(topology._servers, {})
        client.close()
        self.assertEqual(topology._servers, {})

    def test_close_closes_sockets(self):
        client = rs_client()
        self.addCleanup(client.close)
        client.test.test.find_one()
        topology = client._topology
        client.close()
        for server in topology._servers.values():
            self.assertFalse(server._pool.conns)
            self.assertTrue(server._monitor._executor._stopped)
            self.assertTrue(server._monitor._rtt_monitor._executor._stopped)
            self.assertFalse(server._monitor._pool.conns)
            self.assertFalse(server._monitor._rtt_monitor._pool.conns)

    def test_bad_uri(self):
        with self.assertRaises(InvalidURI):
            MongoClient("http://localhost")

    @client_context.require_auth
    def test_auth_from_uri(self):
        host, port = client_context.host, client_context.port
        client_context.create_user("admin", "admin", "pass")
        self.addCleanup(client_context.drop_user, "admin", "admin")
        self.addCleanup(remove_all_users, self.client.pymongo_test)

        client_context.create_user("pymongo_test", "user", "pass", roles=["userAdmin", "readWrite"])

        with self.assertRaises(OperationFailure):
            connected(rs_or_single_client_noauth("mongodb://a:b@%s:%d" % (host, port)))

        # No error.
        connected(rs_or_single_client_noauth("mongodb://admin:pass@%s:%d" % (host, port)))

        # Wrong database.
        uri = "mongodb://admin:pass@%s:%d/pymongo_test" % (host, port)
        with self.assertRaises(OperationFailure):
            connected(rs_or_single_client_noauth(uri))

        # No error.
        connected(
            rs_or_single_client_noauth("mongodb://user:pass@%s:%d/pymongo_test" % (host, port))
        )

        # Auth with lazy connection.
        rs_or_single_client_noauth(
            "mongodb://user:pass@%s:%d/pymongo_test" % (host, port), connect=False
        ).pymongo_test.test.find_one()

        # Wrong password.
        bad_client = rs_or_single_client_noauth(
            "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port), connect=False
        )

        self.assertRaises(OperationFailure, bad_client.pymongo_test.test.find_one)

    @client_context.require_auth
    def test_username_and_password(self):
        client_context.create_user("admin", "ad min", "pa/ss")
        self.addCleanup(client_context.drop_user, "admin", "ad min")

        c = rs_or_single_client_noauth(username="ad min", password="pa/ss")

        # Username and password aren't in strings that will likely be logged.
        self.assertNotIn("ad min", repr(c))
        self.assertNotIn("ad min", str(c))
        self.assertNotIn("pa/ss", repr(c))
        self.assertNotIn("pa/ss", str(c))

        # Auth succeeds.
        c.server_info()

        with self.assertRaises(OperationFailure):
            rs_or_single_client_noauth(username="ad min", password="foo").server_info()

    @client_context.require_auth
    def test_lazy_auth_raises_operation_failure(self):
        lazy_client = rs_or_single_client_noauth(
            f"mongodb://user:wrong@{client_context.host}/pymongo_test", connect=False
        )

        assertRaisesExactly(OperationFailure, lazy_client.test.collection.find_one)

    @client_context.require_no_tls
    def test_unix_socket(self):
        if not hasattr(socket, "AF_UNIX"):
            raise SkipTest("UNIX-sockets are not supported on this system")

        mongodb_socket = "/tmp/mongodb-%d.sock" % (client_context.port,)
        encoded_socket = "%2Ftmp%2F" + "mongodb-%d.sock" % (client_context.port,)
        if not os.access(mongodb_socket, os.R_OK):
            raise SkipTest("Socket file is not accessible")

        uri = "mongodb://%s" % encoded_socket
        # Confirm we can do operations via the socket.
        client = rs_or_single_client(uri)
        self.addCleanup(client.close)
        client.pymongo_test.test.insert_one({"dummy": "object"})
        dbs = client.list_database_names()
        self.assertTrue("pymongo_test" in dbs)

        self.assertTrue(mongodb_socket in repr(client))

        # Confirm it fails with a missing socket.
        self.assertRaises(
            ConnectionFailure,
            connected,
            MongoClient("mongodb://%2Ftmp%2Fnon-existent.sock", serverSelectionTimeoutMS=100),
        )

    def test_document_class(self):
        c = self.client
        db = c.pymongo_test
        db.test.insert_one({"x": 1})

        self.assertEqual(dict, c.codec_options.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c = rs_or_single_client(document_class=SON)
        self.addCleanup(c.close)
        db = c.pymongo_test

        self.assertEqual(SON, c.codec_options.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))

    def test_timeouts(self):
        client = rs_or_single_client(
            connectTimeoutMS=10500,
            socketTimeoutMS=10500,
            maxIdleTimeMS=10500,
            serverSelectionTimeoutMS=10500,
        )
        self.assertEqual(10.5, get_pool(client).opts.connect_timeout)
        self.assertEqual(10.5, get_pool(client).opts.socket_timeout)
        self.assertEqual(10.5, get_pool(client).opts.max_idle_time_seconds)
        self.assertEqual(10.5, client.options.pool_options.max_idle_time_seconds)
        self.assertEqual(10.5, client.options.server_selection_timeout)

    def test_socket_timeout_ms_validation(self):
        c = rs_or_single_client(socketTimeoutMS=10 * 1000)
        self.assertEqual(10, get_pool(c).opts.socket_timeout)

        c = connected(rs_or_single_client(socketTimeoutMS=None))
        self.assertEqual(None, get_pool(c).opts.socket_timeout)

        c = connected(rs_or_single_client(socketTimeoutMS=0))
        self.assertEqual(None, get_pool(c).opts.socket_timeout)

        self.assertRaises(ValueError, rs_or_single_client, socketTimeoutMS=-1)

        self.assertRaises(ValueError, rs_or_single_client, socketTimeoutMS=1e10)

        self.assertRaises(ValueError, rs_or_single_client, socketTimeoutMS="foo")

    def test_socket_timeout(self):
        no_timeout = self.client
        timeout_sec = 1
        timeout = rs_or_single_client(socketTimeoutMS=1000 * timeout_sec)
        self.addCleanup(timeout.close)

        no_timeout.pymongo_test.drop_collection("test")
        no_timeout.pymongo_test.test.insert_one({"x": 1})

        # A $where clause that takes a second longer than the timeout
        where_func = delay(timeout_sec + 1)

        def get_x(db):
            doc = next(db.test.find().where(where_func))
            return doc["x"]

        self.assertEqual(1, get_x(no_timeout.pymongo_test))
        self.assertRaises(NetworkTimeout, get_x, timeout.pymongo_test)

    def test_server_selection_timeout(self):
        client = MongoClient(serverSelectionTimeoutMS=100, connect=False)
        self.assertAlmostEqual(0.1, client.options.server_selection_timeout)

        client = MongoClient(serverSelectionTimeoutMS=0, connect=False)
        self.assertAlmostEqual(0, client.options.server_selection_timeout)

        self.assertRaises(ValueError, MongoClient, serverSelectionTimeoutMS="foo", connect=False)
        self.assertRaises(ValueError, MongoClient, serverSelectionTimeoutMS=-1, connect=False)
        self.assertRaises(
            ConfigurationError, MongoClient, serverSelectionTimeoutMS=None, connect=False
        )

        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=100", connect=False)
        self.assertAlmostEqual(0.1, client.options.server_selection_timeout)

        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=0", connect=False)
        self.assertAlmostEqual(0, client.options.server_selection_timeout)

        # Test invalid timeout in URI ignored and set to default.
        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=-1", connect=False)
        self.assertAlmostEqual(30, client.options.server_selection_timeout)

        client = MongoClient("mongodb://localhost/?serverSelectionTimeoutMS=", connect=False)
        self.assertAlmostEqual(30, client.options.server_selection_timeout)

    def test_waitQueueTimeoutMS(self):
        client = rs_or_single_client(waitQueueTimeoutMS=2000)
        self.assertEqual(get_pool(client).opts.wait_queue_timeout, 2)

    def test_socketKeepAlive(self):
        pool = get_pool(self.client)
        with pool.checkout() as conn:
            keepalive = conn.conn.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
            self.assertTrue(keepalive)

    @no_type_check
    def test_tz_aware(self):
        self.assertRaises(ValueError, MongoClient, tz_aware="foo")

        aware = rs_or_single_client(tz_aware=True)
        self.addCleanup(aware.close)
        naive = self.client
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.now(tz=datetime.timezone.utc)
        aware.pymongo_test.test.insert_one({"x": now})

        self.assertEqual(None, naive.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(utc, aware.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(
            aware.pymongo_test.test.find_one()["x"].replace(tzinfo=None),
            naive.pymongo_test.test.find_one()["x"],
        )

    @client_context.require_ipv6
    def test_ipv6(self):
        if client_context.tls:
            if not HAVE_IPADDRESS:
                raise SkipTest("Need the ipaddress module to test with SSL")

        if client_context.auth_enabled:
            auth_str = f"{db_user}:{db_pwd}@"
        else:
            auth_str = ""

        uri = "mongodb://%s[::1]:%d" % (auth_str, client_context.port)
        if client_context.is_rs:
            uri += "/?replicaSet=" + (client_context.replica_set_name or "")

        client = rs_or_single_client_noauth(uri)
        self.addCleanup(client.close)
        client.pymongo_test.test.insert_one({"dummy": "object"})
        client.pymongo_test_bernie.test.insert_one({"dummy": "object"})

        dbs = client.list_database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)

    def test_contextlib(self):
        client = rs_or_single_client()
        client.pymongo_test.drop_collection("test")
        client.pymongo_test.test.insert_one({"foo": "bar"})

        # The socket used for the previous commands has been returned to the
        # pool
        self.assertEqual(1, len(get_pool(client).conns))

        with contextlib.closing(client):
            self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
        with self.assertRaises(InvalidOperation):
            client.pymongo_test.test.find_one()
        client = rs_or_single_client()
        with client as client:
            self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
        with self.assertRaises(InvalidOperation):
            client.pymongo_test.test.find_one()

    def test_interrupt_signal(self):
        if sys.platform.startswith("java"):
            # We can't figure out how to raise an exception on a thread that's
            # blocked on a socket, whether that's the main thread or a worker,
            # without simply killing the whole thread in Jython. This suggests
            # PYTHON-294 can't actually occur in Jython.
            raise SkipTest("Can't test interrupts in Jython")
        if is_greenthread_patched():
            raise SkipTest("Can't reliably test interrupts with green threads")

        # Test fix for PYTHON-294 -- make sure MongoClient closes its
        # socket if it gets an interrupt while waiting to recv() from it.
        db = self.client.pymongo_test

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
                next(db.foo.find({"$where": where}))
            except KeyboardInterrupt:
                raised = True

            # Can't use self.assertRaises() because it doesn't catch system
            # exceptions
            self.assertTrue(raised, "Didn't raise expected KeyboardInterrupt")

            # Raises AssertionError due to PYTHON-294 -- Mongo's response to
            # the previous find() is still waiting to be read on the socket,
            # so the request id's don't match.
            self.assertEqual({"_id": 1}, next(db.foo.find()))
        finally:
            if old_signal_handler:
                signal.signal(signal.SIGALRM, old_signal_handler)

    def test_operation_failure(self):
        # Ensure MongoClient doesn't close socket after it gets an error
        # response to getLastError. PYTHON-395. We need a new client here
        # to avoid race conditions caused by replica set failover or idle
        # socket reaping.
        client = single_client()
        self.addCleanup(client.close)
        client.pymongo_test.test.find_one()
        pool = get_pool(client)
        socket_count = len(pool.conns)
        self.assertGreaterEqual(socket_count, 1)
        old_conn = next(iter(pool.conns))
        client.pymongo_test.test.drop()
        client.pymongo_test.test.insert_one({"_id": "foo"})
        self.assertRaises(OperationFailure, client.pymongo_test.test.insert_one, {"_id": "foo"})

        self.assertEqual(socket_count, len(pool.conns))
        new_con = next(iter(pool.conns))
        self.assertEqual(old_conn, new_con)

    def test_lazy_connect_w0(self):
        # Ensure that connect-on-demand works when the first operation is
        # an unacknowledged write. This exercises _writable_max_wire_version().

        # Use a separate collection to avoid races where we're still
        # completing an operation on a collection while the next test begins.
        client_context.client.drop_database("test_lazy_connect_w0")
        self.addCleanup(client_context.client.drop_database, "test_lazy_connect_w0")

        client = rs_or_single_client(connect=False, w=0)
        self.addCleanup(client.close)
        client.test_lazy_connect_w0.test.insert_one({})
        wait_until(
            lambda: client.test_lazy_connect_w0.test.count_documents({}) == 1, "find one document"
        )

        client = rs_or_single_client(connect=False, w=0)
        self.addCleanup(client.close)
        client.test_lazy_connect_w0.test.update_one({}, {"$set": {"x": 1}})
        wait_until(
            lambda: client.test_lazy_connect_w0.test.find_one().get("x") == 1, "update one document"
        )

        client = rs_or_single_client(connect=False, w=0)
        self.addCleanup(client.close)
        client.test_lazy_connect_w0.test.delete_one({})
        wait_until(
            lambda: client.test_lazy_connect_w0.test.count_documents({}) == 0, "delete one document"
        )

    @client_context.require_no_mongos
    def test_exhaust_network_error(self):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = rs_or_single_client(maxPoolSize=1, retryReads=False)
        self.addCleanup(client.close)
        collection = client.pymongo_test.test
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        # Ensure a socket.
        connected(client)

        # Cause a network error.
        conn = one(pool.conns)
        conn.conn.close()
        cursor = collection.find(cursor_type=CursorType.EXHAUST)
        with self.assertRaises(ConnectionFailure):
            next(cursor)

        self.assertTrue(conn.closed)

        # The semaphore was decremented despite the error.
        self.assertEqual(0, pool.requests)

    @client_context.require_auth
    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.

        # Get a client with one socket so we detect if it's leaked.
        c = connected(rs_or_single_client(maxPoolSize=1, waitQueueTimeoutMS=1, retryReads=False))

        # Cause a network error on the actual socket.
        pool = get_pool(c)
        socket_info = one(pool.conns)
        socket_info.conn.close()

        # Connection.authenticate logs, but gets a socket.error. Should be
        # reraised as AutoReconnect.
        self.assertRaises(AutoReconnect, c.test.collection.find_one)

        # No semaphore leak, the pool is allowed to make a new socket.
        c.test.collection.find_one()

    @client_context.require_no_replica_set
    def test_connect_to_standalone_using_replica_set_name(self):
        client = single_client(replicaSet="anything", serverSelectionTimeoutMS=100)

        with self.assertRaises(AutoReconnect):
            client.test.test.find_one()

    @client_context.require_replica_set
    def test_stale_getmore(self):
        # A cursor is created, but its member goes down and is removed from
        # the topology before the getMore message is sent. Test that
        # MongoClient._run_operation_with_response handles the error.
        with self.assertRaises(AutoReconnect):
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

    def test_heartbeat_frequency_ms(self):
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
                client_context.host,
                client_context.port,
            )
            client = single_client(uri, event_listeners=[listener])
            wait_until(
                lambda: len(listener.results) >= 2, "record two ServerHeartbeatStartedEvents"
            )

            # Default heartbeatFrequencyMS is 10 sec. Check the interval was
            # closer to 0.5 sec with heartbeatFrequencyMS configured.
            self.assertAlmostEqual(heartbeat_times[1] - heartbeat_times[0], 0.5, delta=2)

            client.close()
        finally:
            ServerHeartbeatStartedEvent.__init__ = old_init  # type: ignore

    def test_small_heartbeat_frequency_ms(self):
        uri = "mongodb://example/?heartbeatFrequencyMS=499"
        with self.assertRaises(ConfigurationError) as context:
            MongoClient(uri)

        self.assertIn("heartbeatFrequencyMS", str(context.exception))

    def test_compression(self):
        def compression_settings(client):
            pool_options = client._MongoClient__options.pool_options
            return pool_options._compression_settings

        uri = "mongodb://localhost:27017/?compressors=zlib"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, ["zlib"])
        uri = "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=4"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, ["zlib"])
        self.assertEqual(opts.zlib_compression_level, 4)
        uri = "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=-1"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, ["zlib"])
        self.assertEqual(opts.zlib_compression_level, -1)
        uri = "mongodb://localhost:27017"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, [])
        self.assertEqual(opts.zlib_compression_level, -1)
        uri = "mongodb://localhost:27017/?compressors=foobar"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, [])
        self.assertEqual(opts.zlib_compression_level, -1)
        uri = "mongodb://localhost:27017/?compressors=foobar,zlib"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, ["zlib"])
        self.assertEqual(opts.zlib_compression_level, -1)

        # According to the connection string spec, unsupported values
        # just raise a warning and are ignored.
        uri = "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=10"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, ["zlib"])
        self.assertEqual(opts.zlib_compression_level, -1)
        uri = "mongodb://localhost:27017/?compressors=zlib&zlibCompressionLevel=-2"
        client = MongoClient(uri, connect=False)
        opts = compression_settings(client)
        self.assertEqual(opts.compressors, ["zlib"])
        self.assertEqual(opts.zlib_compression_level, -1)

        if not _HAVE_SNAPPY:
            uri = "mongodb://localhost:27017/?compressors=snappy"
            client = MongoClient(uri, connect=False)
            opts = compression_settings(client)
            self.assertEqual(opts.compressors, [])
        else:
            uri = "mongodb://localhost:27017/?compressors=snappy"
            client = MongoClient(uri, connect=False)
            opts = compression_settings(client)
            self.assertEqual(opts.compressors, ["snappy"])
            uri = "mongodb://localhost:27017/?compressors=snappy,zlib"
            client = MongoClient(uri, connect=False)
            opts = compression_settings(client)
            self.assertEqual(opts.compressors, ["snappy", "zlib"])

        if not _HAVE_ZSTD:
            uri = "mongodb://localhost:27017/?compressors=zstd"
            client = MongoClient(uri, connect=False)
            opts = compression_settings(client)
            self.assertEqual(opts.compressors, [])
        else:
            uri = "mongodb://localhost:27017/?compressors=zstd"
            client = MongoClient(uri, connect=False)
            opts = compression_settings(client)
            self.assertEqual(opts.compressors, ["zstd"])
            uri = "mongodb://localhost:27017/?compressors=zstd,zlib"
            client = MongoClient(uri, connect=False)
            opts = compression_settings(client)
            self.assertEqual(opts.compressors, ["zstd", "zlib"])

        options = client_context.default_client_options
        if "compressors" in options and "zlib" in options["compressors"]:
            for level in range(-1, 10):
                client = single_client(zlibcompressionlevel=level)
                # No error
                client.pymongo_test.test.find_one()

    def test_reset_during_update_pool(self):
        client = rs_or_single_client(minPoolSize=10)
        self.addCleanup(client.close)
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

            def run(self):
                while self.running:
                    exc = AutoReconnect("mock pool error")
                    ctx = _ErrorContext(exc, 0, pool.gen.get_overall(), False, None)
                    client._topology.handle_error(pool.address, ctx)
                    time.sleep(0.001)

        t = ResetPoolThread(pool)
        t.start()

        # Ensure that update_pool completes without error even when the pool
        # is reset concurrently.
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

    def test_background_connections_do_not_hold_locks(self):
        min_pool_size = 10
        client = rs_or_single_client(
            serverSelectionTimeoutMS=3000, minPoolSize=min_pool_size, connect=False
        )
        self.addCleanup(client.close)

        # Create a single connection in the pool.
        client.admin.command("ping")

        # Cause new connections stall for a few seconds.
        pool = get_pool(client)
        original_connect = pool.connect

        def stall_connect(*args, **kwargs):
            time.sleep(2)
            return original_connect(*args, **kwargs)

        pool.connect = stall_connect
        # Un-patch Pool.connect to break the cyclic reference.
        self.addCleanup(delattr, pool, "connect")

        # Wait for the background thread to start creating connections
        wait_until(lambda: len(pool.conns) > 1, "start creating connections")

        # Assert that application operations do not block.
        for _ in range(10):
            start = time.monotonic()
            client.admin.command("ping")
            total = time.monotonic() - start
            # Each ping command should not take more than 2 seconds
            self.assertLess(total, 2)

    @client_context.require_replica_set
    def test_direct_connection(self):
        # direct_connection=True should result in Single topology.
        client = rs_or_single_client(directConnection=True)
        client.admin.command("ping")
        self.assertEqual(len(client.nodes), 1)
        self.assertEqual(client._topology_settings.get_topology_type(), TOPOLOGY_TYPE.Single)
        client.close()

        # direct_connection=False should result in RS topology.
        client = rs_or_single_client(directConnection=False)
        client.admin.command("ping")
        self.assertGreaterEqual(len(client.nodes), 1)
        self.assertIn(
            client._topology_settings.get_topology_type(),
            [TOPOLOGY_TYPE.ReplicaSetNoPrimary, TOPOLOGY_TYPE.ReplicaSetWithPrimary],
        )
        client.close()

        # directConnection=True, should error with multiple hosts as a list.
        with self.assertRaises(ConfigurationError):
            MongoClient(["host1", "host2"], directConnection=True)

    @unittest.skipIf("PyPy" in sys.version, "PYTHON-2927 fails often on PyPy")
    def test_continuous_network_errors(self):
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
            client = MongoClient(
                "invalid:27017", heartbeatFrequencyMS=3, serverSelectionTimeoutMS=150
            )
            initial_count = server_description_count()
            self.addCleanup(client.close)
            with self.assertRaises(ServerSelectionTimeoutError):
                client.test.test.find_one()
            gc.collect()
            final_count = server_description_count()
            # If a bug like PYTHON-2433 is reintroduced then too many
            # ServerDescriptions will be kept alive and this test will fail:
            # AssertionError: 19 != 46 within 15 delta (27 difference)
            # On Python 3.11 we seem to get more of a delta.
            self.assertAlmostEqual(initial_count, final_count, delta=20)

    @client_context.require_failCommand_fail_point
    def test_network_error_message(self):
        client = single_client(retryReads=False)
        self.addCleanup(client.close)
        client.admin.command("ping")  # connect
        with self.fail_point(
            {"mode": {"times": 1}, "data": {"closeConnection": True, "failCommands": ["find"]}}
        ):
            assert client.address is not None
            expected = "{}:{}: ".format(*client.address)
            with self.assertRaisesRegex(AutoReconnect, expected):
                client.pymongo_test.test.find_one({})

    @unittest.skipIf("PyPy" in sys.version, "PYTHON-2938 could fail on PyPy")
    def test_process_periodic_tasks(self):
        client = rs_or_single_client()
        coll = client.db.collection
        coll.insert_many([{} for _ in range(5)])
        cursor = coll.find(batch_size=2)
        cursor.next()
        c_id = cursor.cursor_id
        self.assertIsNotNone(c_id)
        client.close()
        # Add cursor to kill cursors queue
        del cursor
        wait_until(
            lambda: client._MongoClient__kill_cursors_queue,
            "waited for cursor to be added to queue",
        )
        client._process_periodic_tasks()  # This must not raise or print any exceptions
        with self.assertRaises(InvalidOperation):
            coll.insert_many([{} for _ in range(5)])

    @unittest.skipUnless(_HAVE_DNSPYTHON, "DNS-related tests need dnspython to be installed")
    def test_service_name_from_kwargs(self):
        client = MongoClient(
            "mongodb+srv://user:password@test22.test.build.10gen.cc",
            srvServiceName="customname",
            connect=False,
        )
        self.assertEqual(client._topology_settings.srv_service_name, "customname")
        client = MongoClient(
            "mongodb+srv://user:password@test22.test.build.10gen.cc"
            "/?srvServiceName=shouldbeoverriden",
            srvServiceName="customname",
            connect=False,
        )
        self.assertEqual(client._topology_settings.srv_service_name, "customname")
        client = MongoClient(
            "mongodb+srv://user:password@test22.test.build.10gen.cc/?srvServiceName=customname",
            connect=False,
        )
        self.assertEqual(client._topology_settings.srv_service_name, "customname")

    @unittest.skipUnless(_HAVE_DNSPYTHON, "DNS-related tests need dnspython to be installed")
    def test_srv_max_hosts_kwarg(self):
        client = MongoClient("mongodb+srv://test1.test.build.10gen.cc/")
        self.assertGreater(len(client.topology_description.server_descriptions()), 1)
        client = MongoClient("mongodb+srv://test1.test.build.10gen.cc/", srvmaxhosts=1)
        self.assertEqual(len(client.topology_description.server_descriptions()), 1)
        client = MongoClient(
            "mongodb+srv://test1.test.build.10gen.cc/?srvMaxHosts=1", srvmaxhosts=2
        )
        self.assertEqual(len(client.topology_description.server_descriptions()), 2)

    @unittest.skipIf(_HAVE_DNSPYTHON, "dnspython must not be installed")
    def test_srv_no_dnspython_error(self):
        with self.assertRaisesRegex(ConfigurationError, 'The "dnspython" module must be'):
            MongoClient("mongodb+srv://test1.test.build.10gen.cc/")

    @unittest.skipIf(
        client_context.load_balancer or client_context.serverless,
        "loadBalanced clients do not run SDAM",
    )
    @unittest.skipIf(sys.platform == "win32", "Windows does not support SIGSTOP")
    def test_sigstop_sigcont(self):
        test_dir = os.path.dirname(os.path.realpath(__file__))
        script = os.path.join(test_dir, "sigstop_sigcont.py")
        p = subprocess.Popen(
            [sys.executable, script, client_context.uri],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.addCleanup(p.wait, timeout=1)
        self.addCleanup(p.kill)
        time.sleep(1)
        # Stop the child, sleep for twice the streaming timeout
        # (heartbeatFrequencyMS + connectTimeoutMS), and restart.
        os.kill(p.pid, signal.SIGSTOP)
        time.sleep(2)
        os.kill(p.pid, signal.SIGCONT)
        time.sleep(0.5)
        # Tell the script to exit gracefully.
        outs, _ = p.communicate(input=b"q\n", timeout=10)
        self.assertTrue(outs)
        log_output = outs.decode("utf-8")
        self.assertIn("TEST STARTED", log_output)
        self.assertIn("ServerHeartbeatStartedEvent", log_output)
        self.assertIn("ServerHeartbeatSucceededEvent", log_output)
        self.assertIn("TEST COMPLETED", log_output)
        self.assertNotIn("ServerHeartbeatFailedEvent", log_output)

    def _test_handshake(self, env_vars, expected_env):
        with patch.dict("os.environ", env_vars):
            metadata = copy.deepcopy(_METADATA)
            if expected_env is not None:
                metadata["env"] = expected_env

                if "AWS_REGION" not in env_vars:
                    os.environ["AWS_REGION"] = ""
            with rs_or_single_client(serverSelectionTimeoutMS=10000) as client:
                client.admin.command("ping")
                options = client._MongoClient__options
                self.assertEqual(options.pool_options.metadata, metadata)

    def test_handshake_01_aws(self):
        self._test_handshake(
            {
                "AWS_EXECUTION_ENV": "AWS_Lambda_python3.9",
                "AWS_REGION": "us-east-2",
                "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "1024",
            },
            {"name": "aws.lambda", "region": "us-east-2", "memory_mb": 1024},
        )

    def test_handshake_02_azure(self):
        self._test_handshake({"FUNCTIONS_WORKER_RUNTIME": "python"}, {"name": "azure.func"})

    def test_handshake_03_gcp(self):
        self._test_handshake(
            {
                "K_SERVICE": "servicename",
                "FUNCTION_MEMORY_MB": "1024",
                "FUNCTION_TIMEOUT_SEC": "60",
                "FUNCTION_REGION": "us-central1",
            },
            {"name": "gcp.func", "region": "us-central1", "memory_mb": 1024, "timeout_sec": 60},
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
        )

    def test_handshake_04_vercel(self):
        self._test_handshake(
            {"VERCEL": "1", "VERCEL_REGION": "cdg1"}, {"name": "vercel", "region": "cdg1"}
        )

    def test_handshake_05_multiple(self):
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.9", "FUNCTIONS_WORKER_RUNTIME": "python"},
            None,
        )
        # Extra cases for other combos.
        self._test_handshake(
            {"FUNCTIONS_WORKER_RUNTIME": "python", "K_SERVICE": "servicename"},
            None,
        )
        self._test_handshake({"K_SERVICE": "servicename", "VERCEL": "1"}, None)

    def test_handshake_06_region_too_long(self):
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.9", "AWS_REGION": "a" * 512},
            {"name": "aws.lambda"},
        )

    def test_handshake_07_memory_invalid_int(self):
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "AWS_Lambda_python3.9", "AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "big"},
            {"name": "aws.lambda"},
        )

    def test_handshake_08_invalid_aws_ec2(self):
        # AWS_EXECUTION_ENV needs to start with "AWS_Lambda_".
        self._test_handshake(
            {"AWS_EXECUTION_ENV": "EC2"},
            None,
        )

    def test_dict_hints(self):
        self.db.t.find(hint={"x": 1})

    def test_dict_hints_sort(self):
        result = self.db.t.find()
        result.sort({"x": 1})

        self.db.t.find(sort={"x": 1})

    def test_dict_hints_create_index(self):
        self.db.t.create_index({"x": pymongo.ASCENDING})


class TestExhaustCursor(IntegrationTest):
    """Test that clients properly handle errors from exhaust cursors."""

    def setUp(self):
        super().setUp()
        if client_context.is_mongos:
            raise SkipTest("mongos doesn't support exhaust, SERVER-2627")

    def test_exhaust_query_server_error(self):
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

        self.assertRaises(OperationFailure, cursor.next)
        self.assertFalse(conn.closed)

        # The socket was checked in and the semaphore was decremented.
        self.assertIn(conn, pool.conns)
        self.assertEqual(0, pool.requests)

    def test_exhaust_getmore_server_error(self):
        # When doing a getmore on an exhaust cursor, the socket stays checked
        # out on success but it's checked in on error to avoid semaphore leaks.
        client = rs_or_single_client(maxPoolSize=1)
        collection = client.pymongo_test.test
        collection.drop()

        collection.insert_many([{} for _ in range(200)])
        self.addCleanup(client_context.client.pymongo_test.test.drop)

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
        self.assertRaises(OperationFailure, list, cursor)
        # Unpatch the instance.
        del conn.receive_message

        # The socket is returned to the pool and it still works.
        self.assertEqual(200, collection.count_documents({}))
        self.assertIn(conn, pool.conns)

    def test_exhaust_query_network_error(self):
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
        self.assertRaises(ConnectionFailure, cursor.next)
        self.assertTrue(conn.closed)

        # The socket was closed and the semaphore was decremented.
        self.assertNotIn(conn, pool.conns)
        self.assertEqual(0, pool.requests)

    def test_exhaust_getmore_network_error(self):
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
        conn = cursor._Cursor__sock_mgr.conn
        conn.conn.close()

        # A getmore fails.
        self.assertRaises(ConnectionFailure, list, cursor)
        self.assertTrue(conn.closed)

        wait_until(
            lambda: len(client._MongoClient__kill_cursors_queue) == 0,
            "waited for all killCursor requests to complete",
        )
        # The socket was closed and the semaphore was decremented.
        self.assertNotIn(conn, pool.conns)
        self.assertEqual(0, pool.requests)

    def test_gevent_task(self):
        if not gevent_monkey_patched():
            raise SkipTest("Must be running monkey patched by gevent")
        from gevent import spawn

        def poller():
            while True:
                client_context.client.pymongo_test.test.insert_one({})

        task = spawn(poller)
        task.kill()
        self.assertTrue(task.dead)

    def test_gevent_timeout(self):
        if not gevent_monkey_patched():
            raise SkipTest("Must be running monkey patched by gevent")
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
        self.assertTrue(tt.dead)
        self.assertTrue(ct.dead)
        self.assertIsNone(tt.get())
        self.assertIsNone(ct.get())

    def test_gevent_timeout_when_creating_connection(self):
        if not gevent_monkey_patched():
            raise SkipTest("Must be running monkey patched by gevent")
        from gevent import Timeout, spawn

        client = rs_or_single_client()
        self.addCleanup(client.close)
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
        self.assertEqual(pool.active_sockets, 0)
        # Assert the greenlet is dead
        self.assertTrue(tt.dead)
        # Assert that the Timeout was raised all the way to the try
        self.assertTrue(tt.get())
        # Unpatch the instance.
        del pool.connect


class TestClientLazyConnect(IntegrationTest):
    """Test concurrent operations on a lazily-connecting MongoClient."""

    def _get_client(self):
        return rs_or_single_client(connect=False)

    def test_insert_one(self):
        def reset(collection):
            collection.drop()

        def insert_one(collection, _):
            collection.insert_one({})

        def test(collection):
            self.assertEqual(NTHREADS, collection.count_documents({}))

        lazy_client_trial(reset, insert_one, test, self._get_client)

    def test_update_one(self):
        def reset(collection):
            collection.drop()
            collection.insert_one({"i": 0})

        # Update doc 10 times.
        def update_one(collection, _):
            collection.update_one({}, {"$inc": {"i": 1}})

        def test(collection):
            self.assertEqual(NTHREADS, collection.find_one()["i"])

        lazy_client_trial(reset, update_one, test, self._get_client)

    def test_delete_one(self):
        def reset(collection):
            collection.drop()
            collection.insert_many([{"i": i} for i in range(NTHREADS)])

        def delete_one(collection, i):
            collection.delete_one({"i": i})

        def test(collection):
            self.assertEqual(0, collection.count_documents({}))

        lazy_client_trial(reset, delete_one, test, self._get_client)

    def test_find_one(self):
        results: list = []

        def reset(collection):
            collection.drop()
            collection.insert_one({})
            results[:] = []

        def find_one(collection, _):
            results.append(collection.find_one())

        def test(collection):
            self.assertEqual(NTHREADS, len(results))

        lazy_client_trial(reset, find_one, test, self._get_client)


class TestMongoClientFailover(MockClientTest):
    def test_discover_primary(self):
        c = MockClient(
            standalones=[],
            members=["a:1", "b:2", "c:3"],
            mongoses=[],
            host="b:2",  # Pass a secondary.
            replicaSet="rs",
            heartbeatFrequencyMS=500,
        )
        self.addCleanup(c.close)

        wait_until(lambda: len(c.nodes) == 3, "connect")

        self.assertEqual(c.address, ("a", 1))
        # Fail over.
        c.kill_host("a:1")
        c.mock_primary = "b:2"
        wait_until(lambda: c.address == ("b", 2), "wait for server address to be updated")
        # a:1 not longer in nodes.
        self.assertLess(len(c.nodes), 3)

    def test_reconnect(self):
        # Verify the node list isn't forgotten during a network failure.
        c = MockClient(
            standalones=[],
            members=["a:1", "b:2", "c:3"],
            mongoses=[],
            host="b:2",  # Pass a secondary.
            replicaSet="rs",
            retryReads=False,
            serverSelectionTimeoutMS=1000,
        )
        self.addCleanup(c.close)

        wait_until(lambda: len(c.nodes) == 3, "connect")

        # Total failure.
        c.kill_host("a:1")
        c.kill_host("b:2")
        c.kill_host("c:3")

        # MongoClient discovers it's alone. The first attempt raises either
        # ServerSelectionTimeoutError or AutoReconnect (from
        # MockPool.get_socket).
        self.assertRaises(AutoReconnect, c.db.collection.find_one)

        # But it can reconnect.
        c.revive_host("a:1")
        c._get_topology().select_servers(writable_server_selector)
        self.assertEqual(c.address, ("a", 1))

    def _test_network_error(self, operation_callback):
        # Verify only the disconnected server is reset by a network failure.

        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = MockClient(
                standalones=[],
                members=["a:1", "b:2"],
                mongoses=[],
                host="a:1",
                replicaSet="rs",
                connect=False,
                retryReads=False,
                serverSelectionTimeoutMS=1000,
            )
            self.addCleanup(c.close)

            # Set host-specific information so we can test whether it is reset.
            c.set_wire_version_range("a:1", 2, 6)
            c.set_wire_version_range("b:2", 2, 7)
            c._get_topology().select_servers(writable_server_selector)
            wait_until(lambda: len(c.nodes) == 2, "connect")

            c.kill_host("a:1")

            # MongoClient is disconnected from the primary. This raises either
            # ServerSelectionTimeoutError or AutoReconnect (from
            # MockPool.get_socket).
            self.assertRaises(AutoReconnect, operation_callback, c)

            # The primary's description is reset.
            server_a = c._get_topology().get_server_by_address(("a", 1))
            sd_a = server_a.description
            self.assertEqual(SERVER_TYPE.Unknown, sd_a.server_type)
            self.assertEqual(0, sd_a.min_wire_version)
            self.assertEqual(0, sd_a.max_wire_version)

            # ...but not the secondary's.
            server_b = c._get_topology().get_server_by_address(("b", 2))
            sd_b = server_b.description
            self.assertEqual(SERVER_TYPE.RSSecondary, sd_b.server_type)
            self.assertEqual(2, sd_b.min_wire_version)
            self.assertEqual(7, sd_b.max_wire_version)

    def test_network_error_on_query(self):
        def callback(client):
            return client.db.collection.find_one()

        self._test_network_error(callback)

    def test_network_error_on_insert(self):
        def callback(client):
            return client.db.collection.insert_one({})

        self._test_network_error(callback)

    def test_network_error_on_update(self):
        def callback(client):
            return client.db.collection.update_one({}, {"$unset": "x"})

        self._test_network_error(callback)

    def test_network_error_on_replace(self):
        def callback(client):
            return client.db.collection.replace_one({}, {})

        self._test_network_error(callback)

    def test_network_error_on_delete(self):
        def callback(client):
            return client.db.collection.delete_many({})

        self._test_network_error(callback)


class TestClientPool(MockClientTest):
    @client_context.require_connection
    def test_rs_client_does_not_maintain_pool_to_arbiters(self):
        listener = CMAPListener()
        c = MockClient(
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
        self.addCleanup(c.close)

        wait_until(lambda: len(c.nodes) == 3, "connect")
        self.assertEqual(c.address, ("a", 1))
        self.assertEqual(c.arbiters, {("c", 3)})
        # Assert that we create 2 and only 2 pooled connections.
        listener.wait_for_event(monitoring.ConnectionReadyEvent, 2)
        self.assertEqual(listener.event_count(monitoring.ConnectionCreatedEvent), 2)
        # Assert that we do not create connections to arbiters.
        arbiter = c._topology.get_server_by_address(("c", 3))
        self.assertFalse(arbiter.pool.conns)
        # Assert that we do not create connections to unknown servers.
        arbiter = c._topology.get_server_by_address(("d", 4))
        self.assertFalse(arbiter.pool.conns)
        # Arbiter pool is not marked ready.
        self.assertEqual(listener.event_count(monitoring.PoolReadyEvent), 2)

    @client_context.require_connection
    def test_direct_client_maintains_pool_to_arbiter(self):
        listener = CMAPListener()
        c = MockClient(
            standalones=[],
            members=["a:1", "b:2", "c:3"],
            mongoses=[],
            arbiters=["c:3"],  # c:3 is an arbiter.
            host="c:3",
            directConnection=True,
            minPoolSize=1,  # minPoolSize
            event_listeners=[listener],
        )
        self.addCleanup(c.close)

        wait_until(lambda: len(c.nodes) == 1, "connect")
        self.assertEqual(c.address, ("c", 3))
        # Assert that we create 1 pooled connection.
        listener.wait_for_event(monitoring.ConnectionReadyEvent, 1)
        self.assertEqual(listener.event_count(monitoring.ConnectionCreatedEvent), 1)
        arbiter = c._topology.get_server_by_address(("c", 3))
        self.assertEqual(len(arbiter.pool.conns), 1)
        # Arbiter pool is marked ready.
        self.assertEqual(listener.event_count(monitoring.PoolReadyEvent), 1)


if __name__ == "__main__":
    unittest.main()

# Copyright 2013-2015 MongoDB, Inc.
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

import contextlib
import datetime
import gc
import os
import signal
import socket
import struct
import sys
import time
import traceback

sys.path[0:0] = [""]

from bson import BSON
from bson.codec_options import CodecOptions
from bson.py3compat import thread
from bson.son import SON
from bson.tz_util import utc
from pymongo import auth, message
from pymongo.common import _UUID_REPRESENTATIONS
from pymongo.cursor import CursorType
from pymongo.database import Database
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            InvalidName,
                            InvalidURI,
                            NetworkTimeout,
                            OperationFailure,
                            WriteConcernError)
from pymongo.monitoring import (ServerHeartbeatListener,
                                ServerHeartbeatStartedEvent)
from pymongo.mongo_client import MongoClient
from pymongo.pool import SocketInfo, _METADATA
from pymongo.read_preferences import ReadPreference
from pymongo.server_selectors import (any_server_selector,
                                      writable_server_selector)
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  client_knobs,
                  SkipTest,
                  unittest,
                  IntegrationTest,
                  db_pwd,
                  db_user,
                  MockClientTest,
                  HAVE_IPADDRESS)
from test.pymongo_mocks import MockClient
from test.utils import (assertRaisesExactly,
                        delay,
                        remove_all_users,
                        server_is_master_with_slave,
                        get_pool,
                        one,
                        connected,
                        wait_until,
                        rs_client,
                        rs_or_single_client,
                        rs_or_single_client_noauth,
                        single_client,
                        lazy_client_trial,
                        NTHREADS)


class ClientUnitTest(unittest.TestCase):
    """MongoClient tests that don't require a server."""

    @classmethod
    @client_context.require_connection
    def setUpClass(cls):
        cls.client = rs_or_single_client(connect=False,
                                         serverSelectionTimeoutMS=100)

    def test_keyword_arg_defaults(self):
        client = MongoClient(socketTimeoutMS=None,
                             connectTimeoutMS=20000,
                             waitQueueTimeoutMS=None,
                             waitQueueMultiple=None,
                             socketKeepAlive=False,
                             replicaSet=None,
                             read_preference=ReadPreference.PRIMARY,
                             ssl=False,
                             ssl_keyfile=None,
                             ssl_certfile=None,
                             ssl_cert_reqs=0,  # ssl.CERT_NONE
                             ssl_ca_certs=None,
                             connect=False,
                             serverSelectionTimeoutMS=12000)

        options = client._MongoClient__options
        pool_opts = options.pool_options
        self.assertEqual(None, pool_opts.socket_timeout)
        # socket.Socket.settimeout takes a float in seconds
        self.assertEqual(20.0, pool_opts.connect_timeout)
        self.assertEqual(None, pool_opts.wait_queue_timeout)
        self.assertEqual(None, pool_opts.wait_queue_multiple)
        self.assertFalse(pool_opts.socket_keepalive)
        self.assertEqual(None, pool_opts.ssl_context)
        self.assertEqual(None, options.replica_set_name)
        self.assertEqual(ReadPreference.PRIMARY, client.read_preference)
        self.assertAlmostEqual(12, client.server_selection_timeout)

    def test_types(self):
        self.assertRaises(TypeError, MongoClient, 1)
        self.assertRaises(TypeError, MongoClient, 1.14)
        self.assertRaises(TypeError, MongoClient, "localhost", "27017")
        self.assertRaises(TypeError, MongoClient, "localhost", 1.14)
        self.assertRaises(TypeError, MongoClient, "localhost", [])

        self.assertRaises(ConfigurationError, MongoClient, [])

    def test_max_pool_size_zero(self):
        with self.assertRaises(ValueError):
            MongoClient(maxPoolSize=0)

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
        db = self.client.get_database(
            'foo', codec_options, ReadPreference.SECONDARY, write_concern)
        self.assertEqual('foo', db.name)
        self.assertEqual(codec_options, db.codec_options)
        self.assertEqual(ReadPreference.SECONDARY, db.read_preference)
        self.assertEqual(write_concern, db.write_concern)

    def test_getattr(self):
        self.assertTrue(isinstance(self.client['_does_not_exist'], Database))

        with self.assertRaises(AttributeError) as context:
            self.client._does_not_exist

        # Message should be:
        # "AttributeError: MongoClient has no attribute '_does_not_exist'. To
        # access the _does_not_exist database, use client['_does_not_exist']".
        self.assertIn("has no attribute '_does_not_exist'",
                      str(context.exception))

    def test_iteration(self):
        def iterate():
            [a for a in self.client]

        self.assertRaises(TypeError, iterate)

    def test_get_default_database(self):
        c = rs_or_single_client("mongodb://%s:%d/foo" % (client_context.host,
                                                         client_context.port),
                                connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_get_default_database_error(self):
        # URI with no database.
        c = rs_or_single_client("mongodb://%s:%d/" % (client_context.host,
                                                      client_context.port),
                                connect=False)
        self.assertRaises(ConfigurationError, c.get_default_database)

    def test_get_default_database_with_authsource(self):
        # Ensure we distinguish database name from authSource.
        uri = "mongodb://%s:%d/foo?authSource=src" % (
            client_context.host, client_context.port)
        c = rs_or_single_client(uri, connect=False)
        self.assertEqual(Database(c, 'foo'), c.get_default_database())

    def test_primary_read_pref_with_tags(self):
        # No tags allowed with "primary".
        with self.assertRaises(ConfigurationError):
            MongoClient('mongodb://host/?readpreferencetags=dc:east')

        with self.assertRaises(ConfigurationError):
            MongoClient('mongodb://host/?'
                        'readpreference=primary&readpreferencetags=dc:east')

    def test_metadata(self):
        metadata = _METADATA.copy()
        metadata['application'] = {'name': 'foobar'}
        client = MongoClient(
            "mongodb://foo:27017/?appname=foobar&connect=false")
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata, metadata)
        client = MongoClient('foo', 27017, appname='foobar', connect=False)
        options = client._MongoClient__options
        self.assertEqual(options.pool_options.metadata, metadata)
        # No error
        MongoClient(appname='x' * 128)
        self.assertRaises(ValueError, MongoClient, appname='x' * 129)

    def test_kwargs_codec_options(self):
        # Ensure codec options are passed in correctly
        document_class = SON
        tz_aware = True
        uuid_representation_label = 'javaLegacy'
        unicode_decode_error_handler = 'ignore'
        tzinfo = utc
        c = MongoClient(
            document_class=document_class,
            tz_aware=tz_aware,
            uuidrepresentation=uuid_representation_label,
            unicode_decode_error_handler=unicode_decode_error_handler,
            tzinfo=tzinfo,
            connect=False
        )

        self.assertEqual(c.codec_options.document_class, document_class)
        self.assertEqual(c.codec_options.tz_aware, tz_aware)
        self.assertEqual(
            c.codec_options.uuid_representation,
            _UUID_REPRESENTATIONS[uuid_representation_label])
        self.assertEqual(
            c.codec_options.unicode_decode_error_handler,
            unicode_decode_error_handler)
        self.assertEqual(c.codec_options.tzinfo, tzinfo)

    def test_uri_codec_options(self):
        # Ensure codec options are passed in correctly
        uuid_representation_label = 'javaLegacy'
        unicode_decode_error_handler = 'ignore'
        uri = ("mongodb://%s:%d/foo?tz_aware=true&uuidrepresentation="
               "%s&unicode_decode_error_handler=%s" % (
                   client_context.host,
                   client_context.port,
                   uuid_representation_label,
                   unicode_decode_error_handler))
        c = MongoClient(uri, connect=False)

        self.assertEqual(c.codec_options.tz_aware, True)
        self.assertEqual(
            c.codec_options.uuid_representation,
            _UUID_REPRESENTATIONS[uuid_representation_label])
        self.assertEqual(
            c.codec_options.unicode_decode_error_handler,
            unicode_decode_error_handler)


class TestClient(IntegrationTest):

    def test_max_idle_time_reaper(self):
        with client_knobs(kill_cursor_frequency=0.1):
            # Assert reaper doesn't remove sockets when maxIdleTimeMS not set
            client = rs_or_single_client()
            server = client._get_topology().select_server(any_server_selector)
            with server._pool.get_socket({}) as sock_info:
                pass
            self.assertEqual(1, len(server._pool.sockets))
            self.assertTrue(sock_info in server._pool.sockets)

            # Assert reaper removes idle socket and replaces it with a new one
            client = rs_or_single_client(maxIdleTimeMS=.5,
                                         minPoolSize=1)
            server = client._get_topology().select_server(any_server_selector)
            with server._pool.get_socket({}) as sock_info:
                pass
            self.assertEqual(1, len(server._pool.sockets))
            wait_until(lambda: sock_info not in server._pool.sockets,
                       "reaper removes stale socket eventually")
            wait_until(lambda: 1 == len(server._pool.sockets),
                       "reaper replaces stale socket with new one")

            # Assert reaper has removed idle socket and NOT replaced it
            client = rs_or_single_client(maxIdleTimeMS=.5)
            server = client._get_topology().select_server(any_server_selector)
            with server._pool.get_socket({}):
                pass
            wait_until(
                lambda: 0 == len(server._pool.sockets),
                "stale socket reaped and new one NOT added to the pool")

    def test_min_pool_size(self):
        with client_knobs(kill_cursor_frequency=.1):
            client = rs_or_single_client()
            server = client._get_topology().select_server(any_server_selector)
            self.assertEqual(0, len(server._pool.sockets))

            # Assert that pool started up at minPoolSize
            client = rs_or_single_client(minPoolSize=10)
            server = client._get_topology().select_server(any_server_selector)
            wait_until(lambda: 10 == len(server._pool.sockets),
                       "pool initialized with 10 sockets")

            # Assert that if a socket is closed, a new one takes its place
            with server._pool.get_socket({}) as sock_info:
                sock_info.close()
            wait_until(lambda: 10 == len(server._pool.sockets),
                       "a closed socket gets replaced from the pool")
            self.assertFalse(sock_info in server._pool.sockets)

    def test_max_idle_time_checkout(self):
        # Use high frequency to test _get_socket_no_auth.
        with client_knobs(kill_cursor_frequency=99999999):
            client = rs_or_single_client(maxIdleTimeMS=.5)
            server = client._get_topology().select_server(any_server_selector)
            with server._pool.get_socket({}) as sock_info:
                pass
            self.assertEqual(1, len(server._pool.sockets))
            time.sleep(1) #  Sleep so that the socket becomes stale.

            with server._pool.get_socket({}) as new_sock_info:
                self.assertNotEqual(sock_info, new_sock_info)
            self.assertEqual(1, len(server._pool.sockets))
            self.assertFalse(sock_info in server._pool.sockets)
            self.assertTrue(new_sock_info in server._pool.sockets)

            # Test that sockets are reused if maxIdleTimeMS is not set.
            client = rs_or_single_client()
            server = client._get_topology().select_server(any_server_selector)
            with server._pool.get_socket({}) as sock_info:
                pass
            self.assertEqual(1, len(server._pool.sockets))
            time.sleep(1)
            with server._pool.get_socket({}) as new_sock_info:
                self.assertEqual(sock_info, new_sock_info)
            self.assertEqual(1, len(server._pool.sockets))

    def test_constants(self):
        """This test uses MongoClient explicitly to make sure that host and
        port are not overloaded.
        """
        host, port = client_context.host, client_context.port
        # Set bad defaults.
        MongoClient.HOST = "somedomainthatdoesntexist.org"
        MongoClient.PORT = 123456789
        with self.assertRaises(AutoReconnect):
            connected(MongoClient(serverSelectionTimeoutMS=10,
                                  **client_context.ssl_client_options))

        # Override the defaults. No error.
        connected(MongoClient(host, port,
                              **client_context.ssl_client_options))

        # Set good defaults.
        MongoClient.HOST = host
        MongoClient.PORT = port

        # No error.
        connected(MongoClient(**client_context.ssl_client_options))

    def test_init_disconnected(self):
        host, port = client_context.host, client_context.port
        c = rs_or_single_client(connect=False)
        # is_primary causes client to block until connected
        self.assertIsInstance(c.is_primary, bool)

        c = rs_or_single_client(connect=False)
        self.assertIsInstance(c.is_mongos, bool)
        c = rs_or_single_client(connect=False)
        self.assertIsInstance(c.max_pool_size, int)
        self.assertIsInstance(c.nodes, frozenset)

        c = rs_or_single_client(connect=False)
        self.assertEqual(c.codec_options, CodecOptions())
        self.assertIsInstance(c.max_bson_size, int)
        c = rs_or_single_client(connect=False)
        self.assertFalse(c.primary)
        self.assertFalse(c.secondaries)
        c = rs_or_single_client(connect=False)
        self.assertIsInstance(c.max_write_batch_size, int)

        if client_context.is_rs:
            # The primary's host and port are from the replica set config.
            self.assertIsNotNone(c.address)
        else:
            self.assertEqual(c.address, (host, port))

        bad_host = "somedomainthatdoesntexist.org"
        c = MongoClient(bad_host, port, connectTimeoutMS=1,
                        serverSelectionTimeoutMS=10)
        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_init_disconnected_with_auth(self):
        uri = "mongodb://user:pass@somedomainthatdoesntexist"
        c = MongoClient(uri, connectTimeoutMS=1,
                        serverSelectionTimeoutMS=10)
        self.assertRaises(ConnectionFailure, c.pymongo_test.test.find_one)

    def test_equality(self):
        c = connected(rs_or_single_client())
        self.assertEqual(client_context.client, c)

        # Explicitly test inequality
        self.assertFalse(client_context.client != c)

    def test_host_w_port(self):
        with self.assertRaises(ValueError):
            connected(MongoClient("%s:1234567" % (client_context.host,),
                                  connectTimeoutMS=1,
                                  serverSelectionTimeoutMS=10))

    def test_repr(self):
        # Used to test 'eval' below.
        import bson

        client = MongoClient(
            'mongodb://localhost:27017,localhost:27018/?replicaSet=replset'
            '&connectTimeoutMS=12345&w=1&wtimeoutms=100',
            connect=False, document_class=SON)

        the_repr = repr(client)
        self.assertIn('MongoClient(host=', the_repr)
        self.assertIn(
            "document_class=bson.son.SON, "
            "tz_aware=False, "
            "connect=False, ",
            the_repr)
        self.assertIn("connecttimeoutms=12345", the_repr)
        self.assertIn("replicaset='replset'", the_repr)
        self.assertIn("w=1", the_repr)
        self.assertIn("wtimeoutms=100", the_repr)

        self.assertEqual(eval(the_repr), client)

        client = MongoClient("localhost:27017,localhost:27018",
                             replicaSet='replset',
                             connectTimeoutMS=12345,
                             socketTimeoutMS=None,
                             w=1,
                             wtimeoutms=100,
                             connect=False)
        the_repr = repr(client)
        self.assertIn('MongoClient(host=', the_repr)
        self.assertIn(
            "document_class=dict, "
            "tz_aware=False, "
            "connect=False, ",
            the_repr)
        self.assertIn("connecttimeoutms=12345", the_repr)
        self.assertIn("replicaset='replset'", the_repr)
        self.assertIn("sockettimeoutms=None", the_repr)
        self.assertIn("w=1", the_repr)
        self.assertIn("wtimeoutms=100", the_repr)

        self.assertEqual(eval(the_repr), client)

    def test_getters(self):
        wait_until(lambda: client_context.nodes == self.client.nodes,
                   "find all nodes")

    def test_database_names(self):
        self.client.pymongo_test.test.insert_one({"dummy": u"object"})
        self.client.pymongo_test_mike.test.insert_one({"dummy": u"object"})

        dbs = self.client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_mike" in dbs)

    def test_drop_database(self):
        self.assertRaises(TypeError, self.client.drop_database, 5)
        self.assertRaises(TypeError, self.client.drop_database, None)

        self.client.pymongo_test.test.insert_one({"dummy": u"object"})
        self.client.pymongo_test2.test.insert_one({"dummy": u"object"})
        dbs = self.client.database_names()
        self.assertIn("pymongo_test", dbs)
        self.assertIn("pymongo_test2", dbs)
        self.client.drop_database("pymongo_test")

        if client_context.version.at_least(3, 3, 9) and client_context.is_rs:
            wc_client = rs_or_single_client(w=len(client_context.nodes) + 1)
            with self.assertRaises(WriteConcernError):
                wc_client.drop_database('pymongo_test2')

        self.client.drop_database(self.client.pymongo_test2)

        raise SkipTest("This test often fails due to SERVER-2329")

        dbs = self.client.database_names()
        self.assertNotIn("pymongo_test", dbs)
        self.assertNotIn("pymongo_test2", dbs)

    def test_close(self):
        coll = self.client.pymongo_test.bar

        self.client.close()
        self.client.close()

        coll.count()

        self.client.close()
        self.client.close()

        coll.count()

    def test_close_kills_cursors(self):
        if sys.platform.startswith('java'):
            # We can't figure out how to make this test reliable with Jython.
            raise SkipTest("Can't test with Jython")
        # Kill any cursors possibly queued up by previous tests.
        gc.collect()
        self.client._process_periodic_tasks()

        # Add some test data.
        coll = self.client.pymongo_test.test_close_kills_cursors
        docs_inserted = 1000
        coll.insert_many([{"i": i} for i in range(docs_inserted)])

        # Open a cursor and leave it open on the server.
        cursor = coll.find().batch_size(10)
        self.assertTrue(bool(next(cursor)))
        self.assertLess(cursor.retrieved, docs_inserted)
        del cursor
        # Required for PyPy, Jython and other Python implementations that
        # don't use reference counting garbage collection.
        gc.collect()

        # Close the client and ensure the topology is closed.
        self.assertTrue(self.client._topology._opened)
        self.client.close()
        self.assertFalse(self.client._topology._opened)

        # The killCursors task should not need to re-open the topology.
        self.client._process_periodic_tasks()
        self.assertFalse(self.client._topology._opened)

    def test_bad_uri(self):
        with self.assertRaises(InvalidURI):
            MongoClient("http://localhost")

    @client_context.require_auth
    def test_auth_from_uri(self):
        host, port = client_context.host, client_context.port
        self.client.admin.add_user("admin", "pass", roles=["root"])
        self.addCleanup(self.client.admin.remove_user, 'admin')
        self.addCleanup(remove_all_users, self.client.pymongo_test)

        self.client.pymongo_test.add_user(
            "user", "pass", roles=['userAdmin', 'readWrite'])

        with self.assertRaises(OperationFailure):
            connected(rs_or_single_client(
                "mongodb://a:b@%s:%d" % (host, port)))

        # No error.
        connected(rs_or_single_client_noauth(
            "mongodb://admin:pass@%s:%d" % (host, port)))

        # Wrong database.
        uri = "mongodb://admin:pass@%s:%d/pymongo_test" % (host, port)
        with self.assertRaises(OperationFailure):
            connected(rs_or_single_client(uri))

        # No error.
        connected(rs_or_single_client_noauth(
            "mongodb://user:pass@%s:%d/pymongo_test" % (host, port)))

        # Auth with lazy connection.
        rs_or_single_client_noauth(
            "mongodb://user:pass@%s:%d/pymongo_test" % (host, port),
            connect=False).pymongo_test.test.find_one()

        # Wrong password.
        bad_client = rs_or_single_client_noauth(
            "mongodb://user:wrong@%s:%d/pymongo_test" % (host, port),
            connect=False)

        self.assertRaises(OperationFailure,
                          bad_client.pymongo_test.test.find_one)

    @client_context.require_auth
    def test_multiple_logins(self):
        self.client.pymongo_test.add_user('user1', 'pass', roles=['readWrite'])
        self.client.pymongo_test.add_user('user2', 'pass', roles=['readWrite'])
        self.addCleanup(remove_all_users, self.client.pymongo_test)

        client = rs_or_single_client_noauth(
            "mongodb://user1:pass@%s:%d/pymongo_test" % (
                client_context.host, client_context.port))

        client.pymongo_test.test.find_one()
        with self.assertRaises(OperationFailure):
            # Can't log in to the same database with multiple users.
            client.pymongo_test.authenticate('user2', 'pass')

        client.pymongo_test.test.find_one()
        client.pymongo_test.logout()
        with self.assertRaises(OperationFailure):
            client.pymongo_test.test.find_one()

        client.pymongo_test.authenticate('user2', 'pass')
        client.pymongo_test.test.find_one()

        with self.assertRaises(OperationFailure):
            client.pymongo_test.authenticate('user1', 'pass')

        client.pymongo_test.test.find_one()

    @client_context.require_auth
    def test_lazy_auth_raises_operation_failure(self):
        lazy_client = rs_or_single_client_noauth(
            "mongodb://user:wrong@%s/pymongo_test" % (client_context.host,),
            connect=False)

        assertRaisesExactly(
            OperationFailure, lazy_client.test.collection.find_one)

    @client_context.require_no_ssl
    def test_unix_socket(self):
        if not hasattr(socket, "AF_UNIX"):
            raise SkipTest("UNIX-sockets are not supported on this system")

        mongodb_socket = '/tmp/mongodb-%d.sock' % (client_context.port,)
        encoded_socket = (
            '%2Ftmp%2F' + 'mongodb-%d.sock' % (client_context.port,))
        if not os.access(mongodb_socket, os.R_OK):
            raise SkipTest("Socket file is not accessible")

        if client_context.auth_enabled:
            uri = "mongodb://%s:%s@%s" % (db_user, db_pwd, encoded_socket)
        else:
            uri = "mongodb://%s" % encoded_socket

        # Confirm we can do operations via the socket.
        client = rs_or_single_client(uri)
        client.pymongo_test.test.insert_one({"dummy": "object"})
        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)

        self.assertTrue(mongodb_socket in repr(client))

        # Confirm it fails with a missing socket.
        self.assertRaises(
            ConnectionFailure,
            connected, MongoClient("mongodb://%2Ftmp%2Fnon-existent.sock",
                                   serverSelectionTimeoutMS=100))

    def test_document_class(self):
        c = self.client
        db = c.pymongo_test
        db.test.insert_one({"x": 1})

        self.assertEqual(dict, c.codec_options.document_class)
        self.assertTrue(isinstance(db.test.find_one(), dict))
        self.assertFalse(isinstance(db.test.find_one(), SON))

        c = rs_or_single_client(document_class=SON)
        db = c.pymongo_test

        self.assertEqual(SON, c.codec_options.document_class)
        self.assertTrue(isinstance(db.test.find_one(), SON))

    def test_timeouts(self):
        client = rs_or_single_client(connectTimeoutMS=10500)
        self.assertEqual(10.5, get_pool(client).opts.connect_timeout)
        client = rs_or_single_client(socketTimeoutMS=10500)
        self.assertEqual(10.5, get_pool(client).opts.socket_timeout)

    def test_socket_timeout_ms_validation(self):
        c = rs_or_single_client(socketTimeoutMS=10 * 1000)
        self.assertEqual(10, get_pool(c).opts.socket_timeout)

        c = connected(rs_or_single_client(socketTimeoutMS=None))
        self.assertEqual(None, get_pool(c).opts.socket_timeout)

        self.assertRaises(ValueError,
                          rs_or_single_client, socketTimeoutMS=0)

        self.assertRaises(ValueError,
                          rs_or_single_client, socketTimeoutMS=-1)

        self.assertRaises(ValueError,
                          rs_or_single_client, socketTimeoutMS=1e10)

        self.assertRaises(ValueError,
                          rs_or_single_client, socketTimeoutMS='foo')

    def test_socket_timeout(self):
        no_timeout = self.client
        timeout_sec = 1
        timeout = rs_or_single_client(socketTimeoutMS=1000 * timeout_sec)

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
        self.assertAlmostEqual(0.1, client.server_selection_timeout)

        client = MongoClient(serverSelectionTimeoutMS=0, connect=False)
        self.assertAlmostEqual(0, client.server_selection_timeout)

        self.assertRaises(ValueError, MongoClient,
                          serverSelectionTimeoutMS="foo", connect=False)
        self.assertRaises(ValueError, MongoClient,
                          serverSelectionTimeoutMS=-1, connect=False)
        self.assertRaises(ConfigurationError, MongoClient,
                          serverSelectionTimeoutMS=None, connect=False)

        client = MongoClient(
            'mongodb://localhost/?serverSelectionTimeoutMS=100', connect=False)
        self.assertAlmostEqual(0.1, client.server_selection_timeout)

        client = MongoClient(
            'mongodb://localhost/?serverSelectionTimeoutMS=0', connect=False)
        self.assertAlmostEqual(0, client.server_selection_timeout)

        # Test invalid timeout in URI ignored and set to default.
        client = MongoClient(
            'mongodb://localhost/?serverSelectionTimeoutMS=-1', connect=False)
        self.assertAlmostEqual(30, client.server_selection_timeout)

        client = MongoClient(
            'mongodb://localhost/?serverSelectionTimeoutMS=', connect=False)
        self.assertAlmostEqual(30, client.server_selection_timeout)

    def test_waitQueueTimeoutMS(self):
        client = rs_or_single_client(waitQueueTimeoutMS=2000)
        self.assertEqual(get_pool(client).opts.wait_queue_timeout, 2)

    def test_waitQueueMultiple(self):
        client = rs_or_single_client(maxPoolSize=3, waitQueueMultiple=2)
        pool = get_pool(client)
        self.assertEqual(pool.opts.wait_queue_multiple, 2)
        self.assertEqual(pool._socket_semaphore.waiter_semaphore.counter, 6)

    def test_socketKeepAlive(self):
        client = rs_or_single_client(socketKeepAlive=True)
        self.assertTrue(get_pool(client).opts.socket_keepalive)

    def test_tz_aware(self):
        self.assertRaises(ValueError, MongoClient, tz_aware='foo')

        aware = rs_or_single_client(tz_aware=True)
        naive = self.client
        aware.pymongo_test.drop_collection("test")

        now = datetime.datetime.utcnow()
        aware.pymongo_test.test.insert_one({"x": now})

        self.assertEqual(None, naive.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(utc, aware.pymongo_test.test.find_one()["x"].tzinfo)
        self.assertEqual(
            aware.pymongo_test.test.find_one()["x"].replace(tzinfo=None),
            naive.pymongo_test.test.find_one()["x"])

    @client_context.require_ipv6
    def test_ipv6(self):
        if client_context.ssl:
            # http://bugs.python.org/issue13034
            if sys.version_info[:2] == (2, 6):
                raise SkipTest("Python 2.6 can't parse SANs")
            if not HAVE_IPADDRESS:
                raise SkipTest("Need the ipaddress module to test with SSL")

        if client_context.auth_enabled:
            auth_str = "%s:%s@" % (db_user, db_pwd)
        else:
            auth_str = ""

        uri = "mongodb://%s[::1]:%d" % (auth_str, client_context.port)
        if client_context.is_rs:
            uri += '/?replicaSet=' + client_context.replica_set_name

        client = rs_or_single_client_noauth(uri)
        client.pymongo_test.test.insert_one({"dummy": u"object"})
        client.pymongo_test_bernie.test.insert_one({"dummy": u"object"})

        dbs = client.database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)

    @client_context.require_no_mongos
    def test_fsync_lock_unlock(self):
        if (server_is_master_with_slave(client_context.client) and
                client_context.version.at_least(2, 3, 0)):
            raise SkipTest('SERVER-7714')

        self.assertFalse(self.client.is_locked)
        # async flushing not supported on windows...
        if sys.platform not in ('cygwin', 'win32'):
            self.client.fsync(async=True)
            self.assertFalse(self.client.is_locked)
        self.client.fsync(lock=True)
        self.assertTrue(self.client.is_locked)
        locked = True
        self.client.unlock()
        for _ in range(5):
            locked = self.client.is_locked
            if not locked:
                break
            time.sleep(1)
        self.assertFalse(locked)

    def test_contextlib(self):
        client = rs_or_single_client()
        client.pymongo_test.drop_collection("test")
        client.pymongo_test.test.insert_one({"foo": "bar"})

        # The socket used for the previous commands has been returned to the
        # pool
        self.assertEqual(1, len(get_pool(client).sockets))

        with contextlib.closing(client):
            self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
            self.assertEqual(1, len(get_pool(client).sockets))
        self.assertEqual(0, len(get_pool(client).sockets))

        with client as client:
            self.assertEqual("bar", client.pymongo_test.test.find_one()["foo"])
        self.assertEqual(0, len(get_pool(client).sockets))

    def test_interrupt_signal(self):
        if sys.platform.startswith('java'):
            # We can't figure out how to raise an exception on a thread that's
            # blocked on a socket, whether that's the main thread or a worker,
            # without simply killing the whole thread in Jython. This suggests
            # PYTHON-294 can't actually occur in Jython.
            raise SkipTest("Can't test interrupts in Jython")

        # Test fix for PYTHON-294 -- make sure MongoClient closes its
        # socket if it gets an interrupt while waiting to recv() from it.
        db = self.client.pymongo_test

        # A $where clause which takes 1.5 sec to execute
        where = delay(1.5)

        # Need exactly 1 document so find() will execute its $where clause once
        db.drop_collection('foo')
        db.foo.insert_one({'_id': 1})

        old_signal_handler = None
        try:
            # Platform-specific hacks for raising a KeyboardInterrupt on the
            # main thread while find() is in-progress: On Windows, SIGALRM is
            # unavailable so we use a second thread. In our Evergreen setup on
            # Linux, the thread technique causes an error in the test at
            # sock.recv(): TypeError: 'int' object is not callable
            # We don't know what causes this, so we hack around it.

            if sys.platform == 'win32':
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
                next(db.foo.find({'$where': where}))
            except KeyboardInterrupt:
                raised = True

            # Can't use self.assertRaises() because it doesn't catch system
            # exceptions
            self.assertTrue(raised, "Didn't raise expected KeyboardInterrupt")

            # Raises AssertionError due to PYTHON-294 -- Mongo's response to
            # the previous find() is still waiting to be read on the socket,
            # so the request id's don't match.
            self.assertEqual(
                {'_id': 1},
                next(db.foo.find())
            )
        finally:
            if old_signal_handler:
                signal.signal(signal.SIGALRM, old_signal_handler)

    def test_operation_failure(self):
        # Ensure MongoClient doesn't close socket after it gets an error
        # response to getLastError. PYTHON-395. We need a new client here
        # to avoid race conditions caused by replica set failover or idle
        # socket reaping.
        client = single_client()
        client.pymongo_test.test.find_one()
        pool = get_pool(client)
        socket_count = len(pool.sockets)
        self.assertGreaterEqual(socket_count, 1)
        old_sock_info = next(iter(pool.sockets))
        client.pymongo_test.test.drop()
        client.pymongo_test.test.insert_one({'_id': 'foo'})
        self.assertRaises(
            OperationFailure,
            client.pymongo_test.test.insert_one, {'_id': 'foo'})

        self.assertEqual(socket_count, len(pool.sockets))
        new_sock_info = next(iter(pool.sockets))
        self.assertEqual(old_sock_info, new_sock_info)

    def test_lazy_connect_w0(self):
        # Ensure that connect-on-demand works when the first operation is
        # an unacknowledged write. This exercises _writable_max_wire_version().

        # Use a separate collection to avoid races where we're still
        # completing an operation on a collection while the next test begins.
        client = rs_or_single_client(connect=False, w=0)
        client.test_lazy_connect_w0.test.insert_one({})

        client = rs_or_single_client(connect=False)
        client.test_lazy_connect_w0.test.update_one({}, {'$set': {'x': 1}})

        client = rs_or_single_client(connect=False)
        client.test_lazy_connect_w0.test.delete_one({})

    @client_context.require_no_mongos
    def test_exhaust_network_error(self):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = rs_or_single_client(maxPoolSize=1)
        collection = client.pymongo_test.test
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        # Ensure a socket.
        connected(client)

        # Cause a network error.
        sock_info = one(pool.sockets)
        sock_info.sock.close()
        cursor = collection.find(cursor_type=CursorType.EXHAUST)
        with self.assertRaises(ConnectionFailure):
            next(cursor)

        self.assertTrue(sock_info.closed)

        # The semaphore was decremented despite the error.
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))

    @client_context.require_auth
    def test_auth_network_error(self):
        # Make sure there's no semaphore leak if we get a network error
        # when authenticating a new socket with cached credentials.

        # Get a client with one socket so we detect if it's leaked.
        c = connected(rs_or_single_client(maxPoolSize=1,
                                          waitQueueTimeoutMS=1))

        # Simulate an authenticate() call on a different socket.
        credentials = auth._build_credentials_tuple(
            'DEFAULT', 'admin', db_user, db_pwd, {})

        c._cache_credentials('test', credentials, connect=False)

        # Cause a network error on the actual socket.
        pool = get_pool(c)
        socket_info = one(pool.sockets)
        socket_info.sock.close()

        # SocketInfo.check_auth logs in with the new credential, but gets a
        # socket.error. Should be reraised as AutoReconnect.
        self.assertRaises(AutoReconnect, c.test.collection.find_one)

        # No semaphore leak, the pool is allowed to make a new socket.
        c.test.collection.find_one()

    @client_context.require_no_replica_set
    def test_connect_to_standalone_using_replica_set_name(self):
        client = single_client(replicaSet='anything',
                               serverSelectionTimeoutMS=100)

        with self.assertRaises(AutoReconnect):
            client.test.test.find_one()

    @client_context.require_replica_set
    def test_stale_getmore(self):
        # A cursor is created, but its member goes down and is removed from
        # the topology before the getMore message is sent. Test that
        # MongoClient._send_message_with_response handles the error.
        with self.assertRaises(AutoReconnect):
            client = rs_client(connect=False,
                               serverSelectionTimeoutMS=100)
            client._send_message_with_response(
                operation=message._GetMore('pymongo_test', 'collection',
                                           101, 1234, client.codec_options),
                address=('not-a-member', 27017))

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

        def init(self, *args):
            old_init(self, *args)
            self.time = time.time()

        try:
            ServerHeartbeatStartedEvent.__init__ = init
            listener = HeartbeatStartedListener()
            uri = "mongodb://%s:%d/?heartbeatFrequencyMS=500" % (
                client_context.host, client_context.port)
            client = single_client(uri, event_listeners=[listener])
            wait_until(lambda: len(listener.results) >= 2,
                       "record two ServerHeartbeatStartedEvents")

            events = listener.results

            # Default heartbeatFrequencyMS is 10 sec. Check the interval was
            # closer to 0.5 sec with heartbeatFrequencyMS configured.
            self.assertAlmostEqual(
                events[1].time - events[0].time, 0.5, delta=2)

            client.close()
        finally:
            ServerHeartbeatStartedEvent.__init__ = old_init

    def test_small_heartbeat_frequency_ms(self):
        uri = "mongodb://example/?heartbeatFrequencyMS=499"
        with self.assertRaises(ConfigurationError) as context:
            MongoClient(uri)

        self.assertIn('heartbeatFrequencyMS', str(context.exception))


class TestExhaustCursor(IntegrationTest):
    """Test that clients properly handle errors from exhaust cursors."""

    def setUp(self):
        super(TestExhaustCursor, self).setUp()
        if client_context.is_mongos:
            raise SkipTest("mongos doesn't support exhaust, SERVER-2627")

    # mongod < 2.2.0 closes exhaust socket on error, so it behaves like
    # test_exhaust_query_network_error. Here we test that on query error
    # the client correctly keeps the socket *open* and checks it in.
    @client_context.require_version_min(2, 2, 0)
    def test_exhaust_query_server_error(self):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = connected(rs_or_single_client(maxPoolSize=1))

        collection = client.pymongo_test.test
        pool = get_pool(client)
        sock_info = one(pool.sockets)

        # This will cause OperationFailure in all mongo versions since
        # the value for $orderby must be a document.
        cursor = collection.find(
            SON([('$query', {}), ('$orderby', True)]),
            cursor_type=CursorType.EXHAUST)

        self.assertRaises(OperationFailure, cursor.next)
        self.assertFalse(sock_info.closed)

        # The socket was checked in and the semaphore was decremented.
        self.assertIn(sock_info, pool.sockets)
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))

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
        sock_info = one(pool.sockets)

        cursor = collection.find(cursor_type=CursorType.EXHAUST)

        # Initial query succeeds.
        cursor.next()

        # Cause a server error on getmore.
        def receive_message(operation, request_id):
            # Discard the actual server response.
            SocketInfo.receive_message(sock_info, operation, request_id)

            # responseFlags bit 1 is QueryFailure.
            msg = struct.pack('<iiiii', 1 << 1, 0, 0, 0, 0)
            msg += BSON.encode({'$err': 'mock err', 'code': 0})
            return msg

        saved = sock_info.receive_message
        sock_info.receive_message = receive_message
        self.assertRaises(OperationFailure, list, cursor)
        sock_info.receive_message = saved

        # The socket is returned the pool and it still works.
        self.assertEqual(200, collection.count())
        self.assertIn(sock_info, pool.sockets)

    def test_exhaust_query_network_error(self):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = connected(rs_or_single_client(maxPoolSize=1))
        collection = client.pymongo_test.test
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        # Cause a network error.
        sock_info = one(pool.sockets)
        sock_info.sock.close()

        cursor = collection.find(cursor_type=CursorType.EXHAUST)
        self.assertRaises(ConnectionFailure, cursor.next)
        self.assertTrue(sock_info.closed)

        # The socket was closed and the semaphore was decremented.
        self.assertNotIn(sock_info, pool.sockets)
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))

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
        sock_info = cursor._Cursor__exhaust_mgr.sock
        sock_info.sock.close()

        # A getmore fails.
        self.assertRaises(ConnectionFailure, list, cursor)
        self.assertTrue(sock_info.closed)

        # The socket was closed and the semaphore was decremented.
        self.assertNotIn(sock_info, pool.sockets)
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))


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
            self.assertEqual(NTHREADS, collection.count())

        lazy_client_trial(reset, insert_one, test, self._get_client)

    def test_update_one(self):
        def reset(collection):
            collection.drop()
            collection.insert_one({'i': 0})

        # Update doc 10 times.
        def update_one(collection, _):
            collection.update_one({}, {'$inc': {'i': 1}})

        def test(collection):
            self.assertEqual(NTHREADS, collection.find_one()['i'])

        lazy_client_trial(reset, update_one, test, self._get_client)

    def test_delete_one(self):
        def reset(collection):
            collection.drop()
            collection.insert_many([{'i': i} for i in range(NTHREADS)])

        def delete_one(collection, i):
            collection.delete_one({'i': i})

        def test(collection):
            self.assertEqual(0, collection.count())

        lazy_client_trial(reset, delete_one, test, self._get_client)

    def test_find_one(self):
        results = []

        def reset(collection):
            collection.drop()
            collection.insert_one({})
            results[:] = []

        def find_one(collection, _):
            results.append(collection.find_one())

        def test(collection):
            self.assertEqual(NTHREADS, len(results))

        lazy_client_trial(reset, find_one, test, self._get_client)

    def test_max_bson_size(self):
        c = self._get_client()

        # max_bson_size will cause the client to connect.
        ismaster = c.db.command('ismaster')
        self.assertEqual(ismaster['maxBsonObjectSize'], c.max_bson_size)
        if 'maxMessageSizeBytes' in ismaster:
            self.assertEqual(
                ismaster['maxMessageSizeBytes'],
                c.max_message_size)


class TestMongoClientFailover(MockClientTest):

    def test_discover_primary(self):
        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = MockClient(
                standalones=[],
                members=['a:1', 'b:2', 'c:3'],
                mongoses=[],
                host='b:2',  # Pass a secondary.
                replicaSet='rs')

            wait_until(lambda: len(c.nodes) == 3, 'connect')
            self.assertEqual(c.address, ('a', 1))

            # Fail over.
            c.kill_host('a:1')
            c.mock_primary = 'b:2'

            c.close()
            self.assertEqual(0, len(c.nodes))

            t = c._get_topology()
            t.select_servers(writable_server_selector)  # Reconnect.
            self.assertEqual(c.address, ('b', 2))

            # a:1 not longer in nodes.
            self.assertLess(len(c.nodes), 3)

            # c:3 is rediscovered.
            t.select_server_by_address(('c', 3))

    def test_reconnect(self):
        # Verify the node list isn't forgotten during a network failure.
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='b:2',  # Pass a secondary.
            replicaSet='rs')

        wait_until(lambda: len(c.nodes) == 3, 'connect')

        # Total failure.
        c.kill_host('a:1')
        c.kill_host('b:2')
        c.kill_host('c:3')

        # MongoClient discovers it's alone.
        self.assertRaises(AutoReconnect, c.db.collection.find_one)

        # But it can reconnect.
        c.revive_host('a:1')
        c._get_topology().select_servers(writable_server_selector)
        self.assertEqual(c.address, ('a', 1))

    def _test_network_error(self, operation_callback):
        # Verify only the disconnected server is reset by a network failure.

        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = MockClient(
                standalones=[],
                members=['a:1', 'b:2'],
                mongoses=[],
                host='a:1',
                replicaSet='rs',
                connect=False)

            # Set host-specific information so we can test whether it is reset.
            c.set_wire_version_range('a:1', 0, 1)
            c.set_wire_version_range('b:2', 0, 2)
            c._get_topology().select_servers(writable_server_selector)
            wait_until(lambda: len(c.nodes) == 2, 'connect')

            c.kill_host('a:1')

            # MongoClient is disconnected from the primary.
            self.assertRaises(AutoReconnect, operation_callback, c)

            # The primary's description is reset.
            server_a = c._get_topology().get_server_by_address(('a', 1))
            sd_a = server_a.description
            self.assertEqual(SERVER_TYPE.Unknown, sd_a.server_type)
            self.assertEqual(0, sd_a.min_wire_version)
            self.assertEqual(0, sd_a.max_wire_version)

            # ...but not the secondary's.
            server_b = c._get_topology().get_server_by_address(('b', 2))
            sd_b = server_b.description
            self.assertEqual(SERVER_TYPE.RSSecondary, sd_b.server_type)
            self.assertEqual(0, sd_b.min_wire_version)
            self.assertEqual(2, sd_b.max_wire_version)

    def test_network_error_on_query(self):
        callback = lambda client: client.db.collection.find_one()
        self._test_network_error(callback)

    def test_network_error_on_insert(self):
        callback = lambda client: client.db.collection.insert_one({})
        self._test_network_error(callback)

    def test_network_error_on_update(self):
        callback = lambda client: client.db.collection.update_one(
            {}, {'$unset': 'x'})
        self._test_network_error(callback)

    def test_network_error_on_replace(self):
        callback = lambda client: client.db.collection.replace_one({}, {})
        self._test_network_error(callback)

    def test_network_error_on_delete(self):
        callback = lambda client: client.db.collection.delete_many({})
        self._test_network_error(callback)


if __name__ == "__main__":
    unittest.main()

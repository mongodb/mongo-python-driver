# Copyright 2011-present MongoDB, Inc.
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

"""Test the mongo_replica_set_client module."""

import sys
import warnings
import time

sys.path[0:0] = [""]

from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo.common import MAX_SUPPORTED_WIRE_VERSION, partition_node
from pymongo.errors import (AutoReconnect,
                            ConfigurationError,
                            ConnectionFailure,
                            NetworkTimeout,
                            NotMasterError,
                            OperationFailure)
from pymongo.mongo_client import MongoClient
from pymongo.mongo_replica_set_client import MongoReplicaSetClient
from pymongo.read_preferences import ReadPreference, Secondary, Nearest
from pymongo.write_concern import WriteConcern
from test import (client_context,
                  client_knobs,
                  IntegrationTest,
                  unittest,
                  SkipTest,
                  db_pwd,
                  db_user,
                  MockClientTest,
                  HAVE_IPADDRESS)
from test.pymongo_mocks import MockClient
from test.utils import (connected,
                        delay,
                        ignore_deprecations,
                        one,
                        rs_client,
                        single_client,
                        wait_until)


class TestReplicaSetClientBase(IntegrationTest):

    @classmethod
    @client_context.require_replica_set
    def setUpClass(cls):
        super(TestReplicaSetClientBase, cls).setUpClass()
        cls.name = client_context.replica_set_name
        cls.w = client_context.w

        ismaster = client_context.ismaster
        cls.hosts = set(partition_node(h.lower()) for h in ismaster['hosts'])
        cls.arbiters = set(partition_node(h)
                           for h in ismaster.get("arbiters", []))

        repl_set_status = client_context.client.admin.command(
            'replSetGetStatus')
        primary_info = [
            m for m in repl_set_status['members']
            if m['stateStr'] == 'PRIMARY'
        ][0]

        cls.primary = partition_node(primary_info['name'].lower())
        cls.secondaries = set(
            partition_node(m['name'].lower())
            for m in repl_set_status['members']
            if m['stateStr'] == 'SECONDARY')


class TestReplicaSetClient(TestReplicaSetClientBase):
    def test_deprecated(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            with self.assertRaises(DeprecationWarning):
                MongoReplicaSetClient()

    def test_connect(self):
        client = MongoClient(
            client_context.pair,
            replicaSet='fdlksjfdslkjfd',
            serverSelectionTimeoutMS=100)

        with self.assertRaises(ConnectionFailure):
            client.test.test.find_one()

    def test_repr(self):
        with ignore_deprecations():
            client = MongoReplicaSetClient(
                client_context.host,
                client_context.port,
                replicaSet=self.name)

        self.assertIn("MongoReplicaSetClient(host=[", repr(client))
        self.assertIn(client_context.pair, repr(client))

    def test_properties(self):
        c = client_context.client
        c.admin.command('ping')

        wait_until(lambda: c.primary == self.primary, "discover primary")
        wait_until(lambda: c.arbiters == self.arbiters, "discover arbiters")
        wait_until(lambda: c.secondaries == self.secondaries,
                   "discover secondaries")

        self.assertEqual(c.primary, self.primary)
        self.assertEqual(c.secondaries, self.secondaries)
        self.assertEqual(c.arbiters, self.arbiters)
        self.assertEqual(c.max_pool_size, 100)

        # Make sure MongoClient's properties are copied to Database and
        # Collection.
        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.codec_options, CodecOptions())
            self.assertEqual(obj.read_preference, ReadPreference.PRIMARY)
            self.assertEqual(obj.write_concern, WriteConcern())

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            ReadPreference.PRIMARY, cursor._read_preference())

        tag_sets = [{'dc': 'la', 'rack': '2'}, {'foo': 'bar'}]
        secondary = Secondary(tag_sets=tag_sets)
        c = rs_client(
            maxPoolSize=25,
            document_class=SON,
            tz_aware=True,
            read_preference=secondary,
            localThresholdMS=77,
            j=True)

        self.assertEqual(c.max_pool_size, 25)

        for obj in c, c.pymongo_test, c.pymongo_test.test:
            self.assertEqual(obj.codec_options, CodecOptions(SON, True))
            self.assertEqual(obj.read_preference, secondary)
            self.assertEqual(obj.write_concern, WriteConcern(j=True))

        cursor = c.pymongo_test.test.find()
        self.assertEqual(
            secondary, cursor._read_preference())

        nearest = Nearest(tag_sets=[{'dc': 'ny'}, {}])
        cursor = c.pymongo_test.get_collection(
            "test", read_preference=nearest).find()

        self.assertEqual(nearest, cursor._read_preference())
        self.assertEqual(c.max_bson_size, 16777216)
        c.close()

    @client_context.require_secondaries_count(1)
    def test_timeout_does_not_mark_member_down(self):
        # If a query times out, the client shouldn't mark the member "down".

        # Disable background refresh.
        with client_knobs(heartbeat_frequency=999999):
            c = rs_client(socketTimeoutMS=1000, w=self.w)
            collection = c.pymongo_test.test
            collection.insert_one({})

            # Query the primary.
            self.assertRaises(
                NetworkTimeout,
                collection.find_one,
                {'$where': delay(1.5)})

            self.assertTrue(c.primary)
            collection.find_one()  # No error.

            coll = collection.with_options(
                read_preference=ReadPreference.SECONDARY)

            # Query the secondary.
            self.assertRaises(
                NetworkTimeout,
                coll.find_one,
                {'$where': delay(1.5)})

            self.assertTrue(c.secondaries)

            # No error.
            coll.find_one()

    @client_context.require_ipv6
    def test_ipv6(self):
        if client_context.tls:
            if not HAVE_IPADDRESS:
                raise SkipTest("Need the ipaddress module to test with SSL")

        port = client_context.port
        c = rs_client("mongodb://[::1]:%d" % (port,))

        # Client switches to IPv4 once it has first ismaster response.
        msg = 'discovered primary with IPv4 address "%r"' % (self.primary,)
        wait_until(lambda: c.primary == self.primary, msg)

        # Same outcome with both IPv4 and IPv6 seeds.
        c = rs_client("mongodb://[::1]:%d,localhost:%d" % (port, port))

        wait_until(lambda: c.primary == self.primary, msg)

        if client_context.auth_enabled:
            auth_str = "%s:%s@" % (db_user, db_pwd)
        else:
            auth_str = ""

        uri = "mongodb://%slocalhost:%d,[::1]:%d" % (auth_str, port, port)
        client = rs_client(uri)
        client.pymongo_test.test.insert_one({"dummy": u"object"})
        client.pymongo_test_bernie.test.insert_one({"dummy": u"object"})

        dbs = client.list_database_names()
        self.assertTrue("pymongo_test" in dbs)
        self.assertTrue("pymongo_test_bernie" in dbs)
        client.close()

    def _test_kill_cursor_explicit(self, read_pref):
        with client_knobs(kill_cursor_frequency=0.01):
            c = rs_client(read_preference=read_pref, w=self.w)
            db = c.pymongo_test
            db.drop_collection("test")

            test = db.test
            test.insert_many([{"i": i} for i in range(20)])

            # Partially evaluate cursor so it's left alive, then kill it
            cursor = test.find().batch_size(10)
            next(cursor)
            self.assertNotEqual(0, cursor.cursor_id)

            if read_pref == ReadPreference.PRIMARY:
                msg = "Expected cursor's address to be %s, got %s" % (
                    c.primary, cursor.address)

                self.assertEqual(cursor.address, c.primary, msg)
            else:
                self.assertNotEqual(
                    cursor.address, c.primary,
                    "Expected cursor's address not to be primary")

            cursor_id = cursor.cursor_id

            # Cursor dead on server - trigger a getMore on the same cursor_id
            # and check that the server returns an error.
            cursor2 = cursor.clone()
            cursor2._Cursor__id = cursor_id

            if sys.platform.startswith('java') or 'PyPy' in sys.version:
                # Explicitly kill cursor.
                cursor.close()
            else:
                # Implicitly kill it in CPython.
                del cursor

            time.sleep(5)
            self.assertRaises(OperationFailure, lambda: list(cursor2))

    def test_kill_cursor_explicit_primary(self):
        self._test_kill_cursor_explicit(ReadPreference.PRIMARY)

    @client_context.require_secondaries_count(1)
    def test_kill_cursor_explicit_secondary(self):
        self._test_kill_cursor_explicit(ReadPreference.SECONDARY)

    @client_context.require_secondaries_count(1)
    def test_not_master_error(self):
        secondary_address = one(self.secondaries)
        direct_client = single_client(*secondary_address)

        with self.assertRaises(NotMasterError):
            direct_client.pymongo_test.collection.insert_one({})

        db = direct_client.get_database(
            "pymongo_test", write_concern=WriteConcern(w=0))
        with self.assertRaises(NotMasterError):
            db.collection.insert_one({})


class TestReplicaSetWireVersion(MockClientTest):

    @client_context.require_connection
    @client_context.require_no_auth
    def test_wire_version(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2', 'c:3'],
            mongoses=[],
            host='a:1',
            replicaSet='rs',
            connect=False)
        self.addCleanup(c.close)

        c.set_wire_version_range('a:1', 3, 7)
        c.set_wire_version_range('b:2', 2, 3)
        c.set_wire_version_range('c:3', 3, 4)
        c.db.command('ismaster')  # Connect.

        # A secondary doesn't overlap with us.
        c.set_wire_version_range('b:2',
                                 MAX_SUPPORTED_WIRE_VERSION + 1,
                                 MAX_SUPPORTED_WIRE_VERSION + 2)

        def raises_configuration_error():
            try:
                c.db.collection.find_one()
                return False
            except ConfigurationError:
                return True

        wait_until(raises_configuration_error,
                   'notice we are incompatible with server')

        self.assertRaises(ConfigurationError, c.db.collection.insert_one, {})


class TestReplicaSetClientInternalIPs(MockClientTest):

    @client_context.require_connection
    def test_connect_with_internal_ips(self):
        # Client is passed an IP it can reach, 'a:1', but the RS config
        # only contains unreachable IPs like 'internal-ip'. PYTHON-608.
        client = MockClient(
            standalones=[],
            members=['a:1'],
            mongoses=[],
            ismaster_hosts=['internal-ip:27017'],
            host='a:1',
            replicaSet='rs',
            serverSelectionTimeoutMS=100)
        self.addCleanup(client.close)
        with self.assertRaises(AutoReconnect) as context:
            connected(client)

        self.assertIn("Could not reach any servers in [('internal-ip', 27017)]."
            " Replica set is configured with internal hostnames or IPs?",
            str(context.exception))

class TestReplicaSetClientMaxWriteBatchSize(MockClientTest):

    @client_context.require_connection
    def test_max_write_batch_size(self):
        c = MockClient(
            standalones=[],
            members=['a:1', 'b:2'],
            mongoses=[],
            host='a:1',
            replicaSet='rs',
            connect=False)
        self.addCleanup(c.close)

        c.set_max_write_batch_size('a:1', 1)
        c.set_max_write_batch_size('b:2', 2)

        # Uses primary's max batch size.
        self.assertEqual(c.max_write_batch_size, 1)

        # b becomes primary.
        c.mock_primary = 'b:2'
        wait_until(lambda: c.max_write_batch_size == 2,
                   'update max_write_batch_size')


if __name__ == "__main__":
    unittest.main()

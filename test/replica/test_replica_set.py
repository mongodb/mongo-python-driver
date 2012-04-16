# Copyright 2009-2011 10gen, Inc.
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

"""Test replica set operations and failures."""

import time
import unittest

import replset_tools
from replset_tools import use_greenlets


from pymongo import (ReplicaSetConnection,
                     ReadPreference)
from pymongo.connection import Connection, _partition_node
from pymongo.errors import AutoReconnect, ConnectionFailure


class TestReadPreference(unittest.TestCase):

    def setUp(self):
        members = [{}, {}, {'arbiterOnly': True}]
        res = replset_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_read_preference(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        db = c.pymongo_test
        db.test.remove({}, safe=True, w=len(c.secondaries))

        # Force replication...
        w = len(c.secondaries) + 1
        db.test.insert({'foo': 'bar'}, safe=True, w=w)

        # Test direct connection to a secondary
        host, port = replset_tools.get_secondaries()[0].split(':')
        port = int(port)
        conn = Connection(
            host, port, slave_okay=True, use_greenlets=use_greenlets)
        self.assertEqual(host, conn.host)
        self.assertEqual(port, conn.port)
        self.assert_(conn.pymongo_test.test.find_one())
        conn = Connection(
            host, port,
            read_preference=ReadPreference.SECONDARY,
            use_greenlets=use_greenlets)
        self.assertEqual(host, conn.host)
        self.assertEqual(port, conn.port)
        self.assert_(conn.pymongo_test.test.find_one())

        # Test direct connection to an arbiter
        host = replset_tools.get_arbiters()[0]
        self.assertRaises(
            ConnectionFailure, Connection, host, use_greenlets=use_greenlets)

        # Test PRIMARY
        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertEqual(cursor._Cursor__connection_id, c.primary)

        # Test SECONDARY with a secondary
        db.read_preference = ReadPreference.SECONDARY
        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertTrue(cursor._Cursor__connection_id in c.secondaries)

        # Test SECONDARY_ONLY with a secondary
        db.read_preference = ReadPreference.SECONDARY_ONLY
        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertTrue(cursor._Cursor__connection_id in c.secondaries)

        # Test SECONDARY with no secondary
        killed = replset_tools.kill_all_secondaries()
        sleep(5) # Let monitor thread notice change
        self.assertTrue(bool(len(killed)))
        db.read_preference = ReadPreference.SECONDARY
        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertEqual(cursor._Cursor__connection_id, c.primary)

        # Test SECONDARY_ONLY with no secondary
        db.read_preference = ReadPreference.SECONDARY_ONLY
        for _ in xrange(10):
            cursor = db.test.find()
            self.assertRaises(AutoReconnect, cursor.next)

        replset_tools.restart_members(killed)
        # Test PRIMARY with no primary (should raise an exception)
        db.read_preference = ReadPreference.PRIMARY
        cursor = db.test.find()
        cursor.next()
        self.assertEqual(cursor._Cursor__connection_id, c.primary)
        killed = replset_tools.kill_primary()
        self.assertTrue(bool(len(killed)))
        self.assertRaises(AutoReconnect, db.test.find_one)

    def tearDown(self):
        replset_tools.kill_all_members()


class TestPassiveAndHidden(unittest.TestCase):

    def setUp(self):
        members = [{}, {'priority': 0}, {'arbiterOnly': True},
                   {'priority': 0, 'hidden': True}, {'priority': 0, 'slaveDelay': 5}]
        res = replset_tools.start_replica_set(members)
        self.seed, self.name = res

    def test_passive_and_hidden(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        db = c.pymongo_test
        db.test.remove({}, safe=True, w=len(c.secondaries))
        w = len(c.secondaries) + 1
        db.test.insert({'foo': 'bar'}, safe=True, w=w)
        db.read_preference = ReadPreference.SECONDARY

        passives = replset_tools.get_passives()
        passives = [_partition_node(member) for member in passives]
        hidden = replset_tools.get_hidden_members()
        hidden = [_partition_node(member) for member in hidden]
        self.assertEqual(c.secondaries, set(passives))

        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertTrue(cursor._Cursor__connection_id not in hidden)

        replset_tools.kill_members(replset_tools.get_passives(), 2)
        sleep(5) # Let monitor thread notice change

        for _ in xrange(10):
            cursor = db.test.find()
            cursor.next()
            self.assertEqual(cursor._Cursor__connection_id, c.primary)

    def tearDown(self):
        replset_tools.kill_all_members()

class TestHealthMonitor(unittest.TestCase):

    def setUp(self):
        res = replset_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_primary_failure(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        def primary_changed():
            for _ in xrange(30):
                if c.primary != primary:
                    return True
                sleep(1)
            return False

        killed = replset_tools.kill_primary()
        sleep(5) # Let monitor thread notice change
        self.assertTrue(bool(len(killed)))
        self.assertTrue(primary_changed())
        self.assertTrue(secondaries != c.secondaries)

    def test_secondary_failure(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        def readers_changed():
            for _ in xrange(20):
                if c.secondaries != secondaries:
                    return True

                sleep(1)
            return False

        killed = replset_tools.kill_secondary()
        sleep(5) # Let monitor thread notice change
        self.assertTrue(bool(len(killed)))
        self.assertEqual(primary, c.primary)
        self.assertTrue(readers_changed())
        secondaries = c.secondaries

        replset_tools.restart_members(killed)
        self.assertEqual(primary, c.primary)
        self.assertTrue(readers_changed())

    def test_primary_stepdown(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))
        primary = c.primary
        secondaries = c.secondaries

        def primary_changed():
            for _ in xrange(30):
                if c.primary != primary:
                    return True
                sleep(1)
            return False

        replset_tools.stepdown_primary()
        self.assertTrue(primary_changed())
        self.assertTrue(secondaries != c.secondaries)

    def tearDown(self):
        replset_tools.kill_all_members()


class TestWritesWithFailover(unittest.TestCase):

    def setUp(self):
        res = replset_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_writes_with_failover(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        primary = c.primary
        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        db.test.insert({'foo': 'bar'}, safe=True, w=w)
        self.assertEqual('bar', db.test.find_one()['foo'])

        def try_write():
            for _ in xrange(30):
                try:
                    db.test.insert({'bar': 'baz'}, safe=True)
                    return True
                except AutoReconnect:
                    sleep(1)
            return False

        killed = replset_tools.kill_primary(9)
        self.assertTrue(bool(len(killed)))
        self.assertTrue(try_write())
        self.assertTrue(primary != c.primary)
        self.assertEqual('baz', db.test.find_one({'bar': 'baz'})['bar'])

    def tearDown(self):
        replset_tools.kill_all_members()


class TestReadWithFailover(unittest.TestCase):

    def setUp(self):
        res = replset_tools.start_replica_set([{}, {}, {}])
        self.seed, self.name = res

    def test_read_with_failover(self):
        c = ReplicaSetConnection(
            self.seed, replicaSet=self.name, use_greenlets=use_greenlets)
        self.assertTrue(bool(len(c.secondaries)))

        def iter_cursor(cursor):
            for doc in cursor:
                pass
            return True

        db = c.pymongo_test
        w = len(c.secondaries) + 1
        db.test.remove({}, safe=True, w=w)
        # Force replication
        db.test.insert([{'foo': i} for i in xrange(10)],
                       safe=True, w=w)
        self.assertEqual(10, db.test.count())

        db.read_preference = ReadPreference.SECONDARY
        cursor = db.test.find().batch_size(5)
        cursor.next()
        self.assertEqual(5, cursor._Cursor__retrieved)
        killed = replset_tools.kill_primary()
        # Primary failure shouldn't interrupt the cursor
        self.assertTrue(iter_cursor(cursor))
        self.assertEqual(10, cursor._Cursor__retrieved)

    def tearDown(self):
        replset_tools.kill_all_members()

if __name__ == '__main__':
    if use_greenlets:
        print 'Using Gevent'
        import gevent
        print 'gevent version', gevent.__version__

        if gevent.__version__ == '0.13.6':
            print 'method', gevent.core.get_method()
        else:
            print gevent.get_hub()
        from gevent import monkey
        monkey.patch_socket()
        sleep = gevent.sleep
    else:
        sleep = time.sleep

    unittest.main()

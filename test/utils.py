# Copyright 2012 10gen, Inc.
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

"""Utilities for testing pymongo
"""

from pymongo.errors import AutoReconnect
from pymongo.pool import NO_REQUEST, NO_SOCKET_YET, SocketInfo


def delay(sec):
    # Javascript sleep() only available in MongoDB since version ~1.9
    return '''function() {
        var d = new Date((new Date()).getTime() + %s * 1000);
        while (d > (new Date())) { }; return true;
    }''' % sec

def get_command_line(connection):
    command_line = connection.admin.command('getCmdLineOpts')
    assert command_line['ok'] == 1, "getCmdLineOpts() failed"
    return command_line['argv']

def server_started_with_auth(connection):
    argv = get_command_line(connection)
    return '--auth' in argv or '--keyFile' in argv

def server_is_master_with_slave(connection):
    return '--master' in get_command_line(connection)

def drop_collections(db):
    for coll in db.collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)

def joinall(threads):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    for t in threads:
        t.join(300)
        assert not t.isAlive(), "Thread %s hung" % t

def is_mongos(conn):
    res = conn.admin.command('ismaster')
    return res.get('msg', '') == 'isdbgrid'

def assertRaisesExactly(cls, fn, *args, **kwargs):
    """
    Unlike the standard assertRaises, this checks that a function raises a
    specific class of exception, and not a subclass. E.g., check that
    Connection() raises ConnectionFailure but not its subclass, AutoReconnect.
    """
    try:
        fn(*args, **kwargs)
    except Exception, e:
        assert e.__class__ == cls, "got %s, expected %s" % (
            e.__class__.__name__, cls.__name__)
    else:
        raise AssertionError("%s not raised" % cls)

def read_from_which_host(
    rsc,
    mode,
    tag_sets=None,
    secondary_acceptable_latency_ms=15
):
    """Read from a ReplicaSetConnection with the given Read Preference mode,
       tags, and acceptable latency. Return the 'host:port' which was read from.

    :Parameters:
      - `rsc`: A ReplicaSetConnection
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms`: a float
    """
    db = rsc.pymongo_test
    db.read_preference = mode
    if isinstance(tag_sets, dict):
        tag_sets = [tag_sets]
    db.tag_sets = tag_sets or [{}]
    db.secondary_acceptable_latency_ms = secondary_acceptable_latency_ms

    cursor = db.test.find()
    try:
        try:
            cursor.next()
        except StopIteration:
            # No documents in collection, that's fine
            pass

        return cursor._Cursor__connection_id
    except AutoReconnect:
        return None

def assertReadFrom(testcase, rsc, member, *args, **kwargs):
    """Check that a query with the given mode, tag_sets, and
       secondary_acceptable_latency_ms reads from the expected replica-set
       member

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `rsc`: A ReplicaSetConnection
      - `member`: replica_set_connection.Member expected to be used
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms`: a float
    """
    for _ in range(10):
        testcase.assertEqual(member, read_from_which_host(rsc, *args, **kwargs))

def assertReadFromAll(testcase, rsc, members, *args, **kwargs):
    """Check that a query with the given mode, tag_sets, and
       secondary_acceptable_latency_ms can read from any of a set of
       replica-set members.

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `rsc`: A ReplicaSetConnection
      - `members`: Sequence of replica_set_connection.Member expected to be used
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms`: a float
    """
    members = set(members)
    used = set()
    for _ in range(100):
        used.add(read_from_which_host(rsc, *args, **kwargs))

    testcase.assertEqual(members, used)

class TestRequestMixin(object):
    """Inherit from this class and from unittest.TestCase to get some
    convenient methods for testing connection-pools and requests
    """
    def get_sock(self, pool):
        # Connection calls Pool.get_socket((host, port)), whereas RSC sets
        # Pool.pair at construction-time and just calls Pool.get_socket().
        # Deal with either case so we can use TestRequestMixin to test pools
        # from Connection and from RSC.
        if not pool.pair:
            # self is test_connection.TestConnection
            self.assertTrue(hasattr(self, 'host') and hasattr(self, 'port'))
            sock_info = pool.get_socket((self.host, self.port))
        else:
            sock_info = pool.get_socket()
        return sock_info

    def assertSameSock(self, pool):
        sock_info0 = self.get_sock(pool)
        sock_info1 = self.get_sock(pool)
        self.assertEqual(sock_info0, sock_info1)
        pool.maybe_return_socket(sock_info0)
        pool.maybe_return_socket(sock_info1)

    def assertDifferentSock(self, pool):
        sock_info0 = self.get_sock(pool)
        sock_info1 = self.get_sock(pool)
        self.assertNotEqual(sock_info0, sock_info1)
        pool.maybe_return_socket(sock_info0)
        pool.maybe_return_socket(sock_info1)

    def assertNoRequest(self, pool):
        self.assertEqual(NO_REQUEST, pool._get_request_state())

    def assertNoSocketYet(self, pool):
        self.assertEqual(NO_SOCKET_YET, pool._get_request_state())

    def assertRequestSocket(self, pool):
        self.assertTrue(isinstance(pool._get_request_state(), SocketInfo))

    def assertInRequestAndSameSock(self, conn, pools):
        self.assertTrue(conn.in_request())
        if not isinstance(pools, list):
            pools = [pools]
        for pool in pools:
            self.assertSameSock(pool)

    def assertNotInRequestAndDifferentSock(self, conn, pools):
        self.assertFalse(conn.in_request())
        if not isinstance(pools, list):
            pools = [pools]
        for pool in pools:
            self.assertDifferentSock(pool)

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
import threading

from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.errors import AutoReconnect
from pymongo.pool import NO_REQUEST, NO_SOCKET_YET, SocketInfo
from test import host, port


def one(s):
    """Get one element of a set"""
    return iter(s).next()

def delay(sec):
    # Javascript sleep() only available in MongoDB since version ~1.9
    return '''function() {
        var d = new Date((new Date()).getTime() + %s * 1000);
        while (d > (new Date())) { }; return true;
    }''' % sec

def get_command_line(client):
    command_line = client.admin.command('getCmdLineOpts')
    assert command_line['ok'] == 1, "getCmdLineOpts() failed"
    return command_line['argv']

def server_started_with_auth(client):
    argv = get_command_line(client)
    return '--auth' in argv or '--keyFile' in argv

def server_is_master_with_slave(client):
    return '--master' in get_command_line(client)

def drop_collections(db):
    for coll in db.collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)

def joinall(threads):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    for t in threads:
        t.join(300)
        assert not t.isAlive(), "Thread %s hung" % t

def is_mongos(client):
    res = client.admin.command('ismaster')
    return res.get('msg', '') == 'isdbgrid'

def enable_text_search(client):
    client.admin.command(
        'setParameter', textSearchEnabled=True)

    if isinstance(client, MongoReplicaSetClient):
        for host, port in client.secondaries:
            MongoClient(host, port).admin.command(
                'setParameter', textSearchEnabled=True)

def assertRaisesExactly(cls, fn, *args, **kwargs):
    """
    Unlike the standard assertRaises, this checks that a function raises a
    specific class of exception, and not a subclass. E.g., check that
    MongoClient() raises ConnectionFailure but not its subclass, AutoReconnect.
    """
    try:
        fn(*args, **kwargs)
    except Exception, e:
        assert e.__class__ == cls, "got %s, expected %s" % (
            e.__class__.__name__, cls.__name__)
    else:
        raise AssertionError("%s not raised" % cls)

def looplet(greenlets):
    """World's smallest event loop; run until all greenlets are done
    """
    while True:
        done = True

        for g in greenlets:
            if not g.dead:
                done = False
                g.switch()

        if done:
            return

class RendezvousThread(threading.Thread):
    """A thread that starts and pauses at a rendezvous point before resuming.
    To be used in tests that must ensure that N threads are all alive
    simultaneously, regardless of thread-scheduling's vagaries.

    1. Write a subclass of RendezvousThread and override before_rendezvous
      and / or after_rendezvous.
    2. Create a state with RendezvousThread.shared_state(N)
    3. Start N of your subclassed RendezvousThreads, passing the state to each
      one's __init__
    4. In the main thread, call RendezvousThread.wait_for_rendezvous
    5. Test whatever you need to test while threads are paused at rendezvous
      point
    6. In main thread, call RendezvousThread.resume_after_rendezvous
    7. Join all threads from main thread
    8. Assert that all threads' "passed" attribute is True
    9. Test post-conditions
    """
    class RendezvousState(object):
        def __init__(self, nthreads):
            # Number of threads total
            self.nthreads = nthreads
    
            # Number of threads that have arrived at rendezvous point
            self.arrived_threads = 0
            self.arrived_threads_lock = threading.Lock()
    
            # Set when all threads reach rendezvous
            self.ev_arrived = threading.Event()
    
            # Set by resume_after_rendezvous() so threads can continue.
            self.ev_resume = threading.Event()
            

    @classmethod
    def create_shared_state(cls, nthreads):
        return RendezvousThread.RendezvousState(nthreads)

    def before_rendezvous(self):
        """Overridable: Do this before the rendezvous"""
        pass

    def after_rendezvous(self):
        """Overridable: Do this after the rendezvous. If it throws no exception,
        `passed` is set to True
        """
        pass

    @classmethod
    def wait_for_rendezvous(cls, state):
        """Wait for all threads to reach rendezvous and pause there"""
        state.ev_arrived.wait(10)
        assert state.ev_arrived.isSet(), "Thread timeout"
        assert state.nthreads == state.arrived_threads

    @classmethod
    def resume_after_rendezvous(cls, state):
        """Tell all the paused threads to continue"""
        state.ev_resume.set()

    def __init__(self, state):
        """Params:
          `state`: A shared state object from RendezvousThread.shared_state()
        """
        super(RendezvousThread, self).__init__()
        self.state = state
        self.passed = False

        # If this thread fails to terminate, don't hang the whole program
        self.setDaemon(True)

    def _rendezvous(self):
        """Pause until all threads arrive here"""
        s = self.state
        s.arrived_threads_lock.acquire()
        s.arrived_threads += 1
        if s.arrived_threads == s.nthreads:
            s.arrived_threads_lock.release()
            s.ev_arrived.set()
        else:
            s.arrived_threads_lock.release()
            s.ev_arrived.wait()

    def run(self):
        try:
            self.before_rendezvous()
        finally:
            self._rendezvous()

        # all threads have passed the rendezvous, wait for
        # resume_after_rendezvous()
        self.state.ev_resume.wait()

        self.after_rendezvous()
        self.passed = True

def read_from_which_host(
    rsc,
    mode,
    tag_sets=None,
    secondary_acceptable_latency_ms=15
):
    """Read from a MongoReplicaSetClient with the given Read Preference mode,
       tags, and acceptable latency. Return the 'host:port' which was read from.

    :Parameters:
      - `rsc`: A MongoReplicaSetClient
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
      - `rsc`: A MongoReplicaSetClient
      - `member`: A host:port expected to be used
      - `mode`: A ReadPreference
      - `tag_sets` (optional): List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms` (optional): a float
    """
    for _ in range(10):
        testcase.assertEqual(member, read_from_which_host(rsc, *args, **kwargs))

def assertReadFromAll(testcase, rsc, members, *args, **kwargs):
    """Check that a query with the given mode, tag_sets, and
    secondary_acceptable_latency_ms reads from all members in a set, and
    only members in that set.

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `rsc`: A MongoReplicaSetClient
      - `members`: Sequence of host:port expected to be used
      - `mode`: A ReadPreference
      - `tag_sets` (optional): List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms` (optional): a float
    """
    members = set(members)
    used = set()
    for _ in range(100):
        used.add(read_from_which_host(rsc, *args, **kwargs))

    testcase.assertEqual(members, used)

class TestRequestMixin(object):
    """Inherit from this class and from unittest.TestCase to get some
    convenient methods for testing connection pools and requests
    """
    def get_sock(self, pool):
        # MongoClient calls Pool.get_socket((host, port)), whereas RSC sets
        # Pool.pair at construction-time and just calls Pool.get_socket().
        # Deal with either case so we can use TestRequestMixin to test pools
        # from MongoClient and from RSC.
        if not pool.pair:
            sock_info = pool.get_socket((host, port))
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

    def assertInRequestAndSameSock(self, client, pools):
        self.assertTrue(client.in_request())
        if not isinstance(pools, list):
            pools = [pools]
        for pool in pools:
            self.assertTrue(pool.in_request())
            self.assertSameSock(pool)

    def assertNotInRequestAndDifferentSock(self, client, pools):
        self.assertFalse(client.in_request())
        if not isinstance(pools, list):
            pools = [pools]
        for pool in pools:
            self.assertFalse(pool.in_request())
            self.assertDifferentSock(pool)

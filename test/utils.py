# Copyright 2012-2014 MongoDB, Inc.
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

import gc
import os
import struct
import sys
import threading
import time

from nose.plugins.skip import SkipTest

from bson.son import SON
from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.errors import AutoReconnect, ConnectionFailure, OperationFailure
from pymongo.pool import NO_REQUEST, NO_SOCKET_YET, SocketInfo
from test import host, port, version


try:
    import gevent
    has_gevent = True
except ImportError:
    has_gevent = False


# No functools in Python 2.4
def my_partial(f, *args, **kwargs):
    def _f(*new_args, **new_kwargs):
        final_kwargs = kwargs.copy()
        final_kwargs.update(new_kwargs)
        return f(*(args + new_args), **final_kwargs)

    return _f

def one(s):
    """Get one element of a set"""
    return iter(s).next()

def oid_generated_on_client(doc):
    """Is this process's PID in the document's _id?"""
    pid_from_doc = struct.unpack(">H", doc['_id'].binary[7:9])[0]
    return (os.getpid() % 0xFFFF) == pid_from_doc

def delay(sec):
    # Javascript sleep() only available in MongoDB since version ~1.9
    return '''function() {
        var d = new Date((new Date()).getTime() + %s * 1000);
        while (d > (new Date())) { }; return true;
    }''' % sec

def get_command_line(client):
    command_line = client.admin.command('getCmdLineOpts')
    assert command_line['ok'] == 1, "getCmdLineOpts() failed"
    return command_line

def server_started_with_option(client, cmdline_opt, config_opt):
    """Check if the server was started with a particular option.
    
    :Parameters:
      - `cmdline_opt`: The command line option (i.e. --nojournal)
      - `config_opt`: The config file option (i.e. nojournal)
    """
    command_line = get_command_line(client)
    if 'parsed' in command_line:
        parsed = command_line['parsed']
        if config_opt in parsed:
            return parsed[config_opt]
    argv = command_line['argv']
    return cmdline_opt in argv


def server_started_with_auth(client):
    command_line = get_command_line(client)
    # MongoDB >= 2.0
    if 'parsed' in command_line:
        parsed = command_line['parsed']
        # MongoDB >= 2.6
        if 'security' in parsed:
            security = parsed['security']
            # >= rc3
            if 'authorization' in security:
                return security['authorization'] == 'enabled'
            # < rc3
            return security.get('auth', False) or bool(security.get('keyFile'))
        return parsed.get('auth', False) or bool(parsed.get('keyFile'))
    # Legacy
    argv = command_line['argv']
    return '--auth' in argv or '--keyFile' in argv


def server_started_with_nojournal(client):
    command_line = get_command_line(client)

    # MongoDB 2.6.
    if 'parsed' in command_line:
        parsed = command_line['parsed']
        if 'storage' in parsed:
            storage = parsed['storage']
            if 'journal' in storage:
                return not storage['journal']['enabled']

    return server_started_with_option(client, '--nojournal', 'nojournal')


def server_is_master_with_slave(client):
    command_line = get_command_line(client)
    if 'parsed' in command_line:
        return command_line['parsed'].get('master', False)
    return '--master' in command_line['argv']

def drop_collections(db):
    for coll in db.collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)

def remove_all_users(db):
    if version.at_least(db.connection, (2, 5, 3, -1)):
        db.command({"dropAllUsersFromDatabase": 1})
    else:
        db.system.users.remove({})


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

def get_pool(client):
    if isinstance(client, MongoClient):
        return client._MongoClient__member.pool
    elif isinstance(client, MongoReplicaSetClient):
        rs_state = client._MongoReplicaSetClient__rs_state
        return rs_state.primary_member.pool
    else:
        raise TypeError(str(client))

def pools_from_rs_client(client):
    """Get Pool instances from a MongoReplicaSetClient or ReplicaSetConnection.
    """
    return [
        member.pool for member in
        client._MongoReplicaSetClient__rs_state.members]

class TestRequestMixin(object):
    """Inherit from this class and from unittest.TestCase to get some
    convenient methods for testing connection pools and requests
    """
    def assertSameSock(self, pool):
        sock_info0 = pool.get_socket()
        sock_info1 = pool.get_socket()
        self.assertEqual(sock_info0, sock_info1)
        pool.maybe_return_socket(sock_info0)
        pool.maybe_return_socket(sock_info1)

    def assertDifferentSock(self, pool):
        sock_info0 = pool.get_socket()
        sock_info1 = pool.get_socket()
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


# Constants for run_threads and _TestLazyConnectMixin.
NTRIALS = 5
NTHREADS = 10


def run_threads(collection, target, use_greenlets):
    """Run a target function in many threads.

    target is a function taking a Collection and an integer.
    """
    threads = []
    for i in range(NTHREADS):
        bound_target = my_partial(target, collection, i)
        if use_greenlets:
            threads.append(gevent.Greenlet(run=bound_target))
        else:
            threads.append(threading.Thread(target=bound_target))

    for t in threads:
        t.start()

    for t in threads:
        t.join(30)
        if use_greenlets:
            # bool(Greenlet) is True if it's alive.
            assert not t
        else:
            assert not t.isAlive()


def lazy_client_trial(reset, target, test, get_client, use_greenlets):
    """Test concurrent operations on a lazily-connecting client.

    `reset` takes a collection and resets it for the next trial.

    `target` takes a lazily-connecting collection and an index from
    0 to NTHREADS, and performs some operation, e.g. an insert.

    `test` takes the lazily-connecting collection and asserts a
    post-condition to prove `target` succeeded.
    """
    if use_greenlets and not has_gevent:
        raise SkipTest('Gevent not installed')

    collection = MongoClient(host, port).pymongo_test.test

    # Make concurrency bugs more likely to manifest.
    interval = None
    if not sys.platform.startswith('java'):
        if hasattr(sys, 'getswitchinterval'):
            interval = sys.getswitchinterval()
            sys.setswitchinterval(1e-6)
        else:
            interval = sys.getcheckinterval()
            sys.setcheckinterval(1)

    try:
        for i in range(NTRIALS):
            reset(collection)
            lazy_client = get_client(
                _connect=False, use_greenlets=use_greenlets)

            lazy_collection = lazy_client.pymongo_test.test
            run_threads(lazy_collection, target, use_greenlets)
            test(lazy_collection)

    finally:
        if not sys.platform.startswith('java'):
            if hasattr(sys, 'setswitchinterval'):
                sys.setswitchinterval(interval)
            else:
                sys.setcheckinterval(interval)


class _TestLazyConnectMixin(object):
    """Test concurrent operations on a lazily-connecting client.

    Inherit from this class and from unittest.TestCase, and override
    _get_client(self, **kwargs), for testing a lazily-connecting
    client, i.e. a client initialized with _connect=False.

    Set use_greenlets = True to test with Gevent.
    """
    use_greenlets = False

    NTRIALS = 5
    NTHREADS = 10

    def test_insert(self):
        def reset(collection):
            collection.drop()

        def insert(collection, _):
            collection.insert({})

        def test(collection):
            self.assertEqual(NTHREADS, collection.count())

        lazy_client_trial(
            reset, insert, test,
            self._get_client, self.use_greenlets)

    def test_save(self):
        def reset(collection):
            collection.drop()

        def save(collection, _):
            collection.save({})

        def test(collection):
            self.assertEqual(NTHREADS, collection.count())

        lazy_client_trial(
            reset, save, test,
            self._get_client, self.use_greenlets)

    def test_update(self):
        def reset(collection):
            collection.drop()
            collection.insert([{'i': 0}])

        # Update doc 10 times.
        def update(collection, i):
            collection.update({}, {'$inc': {'i': 1}})

        def test(collection):
            self.assertEqual(NTHREADS, collection.find_one()['i'])

        lazy_client_trial(
            reset, update, test,
            self._get_client, self.use_greenlets)

    def test_remove(self):
        def reset(collection):
            collection.drop()
            collection.insert([{'i': i} for i in range(NTHREADS)])

        def remove(collection, i):
            collection.remove({'i': i})

        def test(collection):
            self.assertEqual(0, collection.count())

        lazy_client_trial(
            reset, remove, test,
            self._get_client, self.use_greenlets)

    def test_find_one(self):
        results = []

        def reset(collection):
            collection.drop()
            collection.insert({})
            results[:] = []

        def find_one(collection, _):
            results.append(collection.find_one())

        def test(collection):
            self.assertEqual(NTHREADS, len(results))

        lazy_client_trial(
            reset, find_one, test,
            self._get_client, self.use_greenlets)

    def test_max_bson_size(self):
        # Client should have sane defaults before connecting, and should update
        # its configuration once connected.
        c = self._get_client(_connect=False)
        self.assertEqual(16 * (1024 ** 2), c.max_bson_size)
        self.assertEqual(2 * c.max_bson_size, c.max_message_size)

        # Make the client connect, so that it sets its max_bson_size and
        # max_message_size attributes.
        ismaster = c.db.command('ismaster')
        self.assertEqual(ismaster['maxBsonObjectSize'], c.max_bson_size)
        if 'maxMessageSizeBytes' in ismaster:
            self.assertEqual(
                ismaster['maxMessageSizeBytes'],
                c.max_message_size)


class _TestExhaustCursorMixin(object):
    """Test that clients properly handle errors from exhaust cursors.

    Inherit from this class and from unittest.TestCase, and override
    _get_client(self, **kwargs).
    """
    def test_exhaust_query_server_error(self):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = self._get_client(max_pool_size=1)
        if is_mongos(client):
            raise SkipTest("Can't use exhaust cursors with mongos")
        if not version.at_least(client, (2, 2, 0)):
            raise SkipTest("mongod < 2.2.0 closes exhaust socket on error")

        collection = client.pymongo_test.test
        pool = get_pool(client)

        sock_info = one(pool.sockets)
        # This will cause OperationFailure in all mongo versions since
        # the value for $orderby must be a document.
        cursor = collection.find(
            SON([('$query', {}), ('$orderby', True)]), exhaust=True)
        self.assertRaises(OperationFailure, cursor.next)
        self.assertFalse(sock_info.closed)

        # The semaphore was decremented despite the error.
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))

    def test_exhaust_getmore_server_error(self):
        # When doing a getmore on an exhaust cursor, the socket stays checked
        # out on success but must be checked in on error to avoid semaphore
        # leaks.
        client = self._get_client(max_pool_size=1)
        if is_mongos(client):
            raise SkipTest("Can't use exhaust cursors with mongos")

        # A separate client that doesn't affect the test client's pool.
        client2 = self._get_client()

        collection = client.pymongo_test.test
        collection.remove()

        # Enough data to ensure it streams down for a few milliseconds.
        long_str = 'a' * (256 * 1024)
        collection.insert([{'a': long_str} for _ in range(200)])

        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.
        sock_info = one(pool.sockets)

        cursor = collection.find(exhaust=True)

        # Initial query succeeds.
        cursor.next()

        # Cause a server error on getmore.
        client2.pymongo_test.test.drop()
        self.assertRaises(OperationFailure, list, cursor)

        # Make sure the socket is still valid
        self.assertEqual(0, collection.count())

    def test_exhaust_query_network_error(self):
        # When doing an exhaust query, the socket stays checked out on success
        # but must be checked in on error to avoid semaphore leaks.
        client = self._get_client(max_pool_size=1)
        if is_mongos(client):
            raise SkipTest("Can't use exhaust cursors with mongos")

        collection = client.pymongo_test.test
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        # Cause a network error.
        sock_info = one(pool.sockets)
        sock_info.sock.close()
        cursor = collection.find(exhaust=True)
        self.assertRaises(ConnectionFailure, cursor.next)
        self.assertTrue(sock_info.closed)

        # The semaphore was decremented despite the error.
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))

    def test_exhaust_getmore_network_error(self):
        # When doing a getmore on an exhaust cursor, the socket stays checked
        # out on success but must be checked in on error to avoid semaphore
        # leaks.
        client = self._get_client(max_pool_size=1)
        if is_mongos(client):
            raise SkipTest("Can't use exhaust cursors with mongos")

        collection = client.pymongo_test.test
        collection.remove()
        collection.insert([{} for _ in range(200)])  # More than one batch.
        pool = get_pool(client)
        pool._check_interval_seconds = None  # Never check.

        cursor = collection.find(exhaust=True)

        # Initial query succeeds.
        cursor.next()

        # Cause a network error.
        sock_info = cursor._Cursor__exhaust_mgr.sock
        sock_info.sock.close()

        # A getmore fails.
        self.assertRaises(ConnectionFailure, list, cursor)
        self.assertTrue(sock_info.closed)

        # The semaphore was decremented despite the error.
        self.assertTrue(pool._socket_semaphore.acquire(blocking=False))


# Backport of WarningMessage from python 2.6, with fixed syntax for python 2.4.
class WarningMessage(object):

    """Holds the result of a single showwarning() call."""

    _WARNING_DETAILS = ("message", "category", "filename", "lineno", "file",
                        "line")

    def __init__(self, message, category,
                 filename, lineno, file=None, line=None):
        local_values = locals()
        for attr in self._WARNING_DETAILS:
            setattr(self, attr, local_values[attr])
        self._category_name = None
        if category:
            self._category_name = category.__name__

    def __str__(self):
        return ("{message : %r, category : %r, filename : %r, lineno : %s, "
                    "line : %r}" % (self.message, self._category_name,
                                    self.filename, self.lineno, self.line))


# Rough backport of warnings.catch_warnings from python 2.6,
# with changes to support python 2.4.
class CatchWarnings(object):
    """A non-context manager version of warnings.catch_warnings.

    The 'record' argument specifies whether warnings should be captured by a
    custom implementation of warnings.showwarning() and be appended to a list
    accessed through the `log` property. The objects appended to the list are
    arguments whose attributes mirror the arguments to showwarning().

    The 'module' argument is to specify an alternative module to the module
    named 'warnings' and imported under that name. This argument is only useful
    when testing the warnings module itself.
    """

    def __init__(self, record=False, module=None):
        self._record = record
        if module is None:
            self._module = sys.modules['warnings']
        else:
            self._module = module

        # No __enter__ so do that work here
        self._filters = self._module.filters
        self._module.filters = self._filters[:]
        self._showwarning = self._module.showwarning
        self._log = []
        if self._record:
            def showwarning(*args, **kwargs):
                self._log.append(WarningMessage(*args, **kwargs))
            self._module.showwarning = showwarning

    @property
    def log(self):
        """A list of any warnings recorded when using record=True."""
        return self._log

    def __repr__(self):
        args = []
        if self._record:
            args.append("record=True")
        if self._module is not sys.modules['warnings']:
            args.append("module=%r" % self._module)
        name = type(self).__name__
        return "%s(%s)" % (name, ", ".join(args))

    def exit(self):
        """Revert changes to the warnings module."""
        self._module.filters = self._filters
        self._module.showwarning = self._showwarning


def catch_warnings(record=False, module=None):
    """Helper for use with CatchWarnings."""
    return CatchWarnings(record, module)

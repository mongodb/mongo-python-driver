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

"""Base classes to test built-in connection-pooling with threads or greenlets.
"""

import gc
import random
import socket
import sys
import thread
import threading
import time

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

import pymongo.pool
from pymongo.mongo_client import MongoClient
from pymongo.pool import Pool, NO_REQUEST, NO_SOCKET_YET, SocketInfo
from pymongo.errors import ConfigurationError, ConnectionFailure
from pymongo.errors import ExceededMaxWaiters
from test import version, host, port
from test.test_client import get_client
from test.utils import delay, is_mongos, one, get_pool

N = 10
DB = "pymongo-pooling-tests"


if sys.version_info[0] >= 3:
    from imp import reload


try:
    import gevent
    from gevent import Greenlet, monkey, hub
    import gevent.coros, gevent.event
    has_gevent = True
except ImportError:
    has_gevent = False


def gc_collect_until_done(threads, timeout=60):
    start = time.time()
    running = list(threads)
    while running:
        assert (time.time() - start) < timeout, "Threads timed out"
        for t in running:
            t.thread.join(0.1)
            if not t.alive:
                running.remove(t)
        gc.collect()


class MongoThread(object):
    """A thread, or a greenlet, that uses a MongoClient"""
    def __init__(self, test_case):
        self.use_greenlets = test_case.use_greenlets
        self.client = test_case.c
        self.db = self.client[DB]
        self.ut = test_case
        self.passed = False

    def start(self):
        if self.use_greenlets:
            # A Gevent extended Greenlet
            self.thread = Greenlet(self.run)
        else:
            self.thread = threading.Thread(target=self.run)
            self.thread.setDaemon(True)  # Don't hang whole test if thread hangs

        self.thread.start()

    @property
    def alive(self):
        if self.use_greenlets:
            return not self.thread.dead
        else:
            return self.thread.isAlive()

    def join(self):
        self.thread.join(20)
        if self.use_greenlets:
            msg = "Greenlet timeout"
        else:
            msg = "Thread timeout"
        assert not self.alive, msg
        self.thread = None

    def run(self):
        self.run_mongo_thread()

        # No exceptions thrown
        self.passed = True

    def run_mongo_thread(self):
        raise NotImplementedError()


class SaveAndFind(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            rand = random.randint(0, N)
            _id = self.db.sf.save({"x": rand})
            self.ut.assertEqual(rand, self.db.sf.find_one(_id)["x"])


class Unique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.client.start_request()
            self.db.unique.insert({})  # no error
            self.client.end_request()


class NonUnique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.client.start_request()
            self.db.unique.insert({"_id": "jesse"}, w=0)
            self.ut.assertNotEqual(None, self.db.error())
            self.client.end_request()


class Disconnect(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.client.disconnect()


class NoRequest(MongoThread):

    def run_mongo_thread(self):
        self.client.start_request()
        errors = 0
        for _ in xrange(N):
            self.db.unique.insert({"_id": "jesse"}, w=0)
            if not self.db.error():
                errors += 1

        self.client.end_request()
        self.ut.assertEqual(0, errors)


def run_cases(ut, cases):
    threads = []
    nruns = 10
    if (
        ut.use_greenlets and sys.platform == 'darwin'
        and gevent.version_info[0] < 1
    ):
        # Gevent 0.13.6 bug on Mac, Greenlet.join() hangs if more than
        # about 35 Greenlets share a MongoClient. Apparently fixed in
        # recent Gevent development.
        nruns = 5

    for case in cases:
        for i in range(nruns):
            t = case(ut)
            t.start()
            threads.append(t)

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed, "%s.run_mongo_thread() threw an exception" % repr(t)


class OneOp(MongoThread):

    def __init__(self, ut):
        super(OneOp, self).__init__(ut)

    def run_mongo_thread(self):
        pool = get_pool(self.client)
        assert len(pool.sockets) == 1, "Expected 1 socket, found %d" % (
            len(pool.sockets)
        )

        sock_info = one(pool.sockets)

        self.client.start_request()

        # start_request() hasn't yet moved the socket from the general pool into
        # the request
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock_info

        self.client[DB].test.find_one()

        # find_one() causes the socket to be used in the request, so now it's
        # bound to this thread
        assert len(pool.sockets) == 0
        assert pool._get_request_state() == sock_info
        self.client.end_request()

        # The socket is back in the pool
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock_info


class CreateAndReleaseSocket(MongoThread):
    """A thread or greenlet that acquires a socket, waits for all other threads
    to reach rendezvous point, then terminates.
    """
    class Rendezvous(object):
        def __init__(self, nthreads, use_greenlets):
            self.nthreads = nthreads
            self.nthreads_run = 0
            self.use_greenlets = use_greenlets
            if use_greenlets:
                self.lock = gevent.coros.RLock()
            else:
                self.lock = threading.Lock()
            self.reset_ready()

        def reset_ready(self):
            if self.use_greenlets:
                self.ready = gevent.event.Event()
            else:
                self.ready = threading.Event()

    def __init__(self, ut, client, start_request, end_request, rendezvous):
        super(CreateAndReleaseSocket, self).__init__(ut)
        self.client = client
        self.start_request = start_request
        self.end_request = end_request
        self.rendezvous = rendezvous

    def run_mongo_thread(self):
        # Do an operation that requires a socket.
        # test_max_pool_size uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        for i in range(self.start_request):
            self.client.start_request()

        # Use a socket
        self.client[DB].test.find_one()

        # Don't finish until all threads reach this point
        r = self.rendezvous
        r.lock.acquire()
        r.nthreads_run += 1
        if r.nthreads_run == r.nthreads:
            # Everyone's here, let them finish
            r.ready.set()
            r.lock.release()
        else:
            r.lock.release()
            r.ready.wait(30)  # Wait thirty seconds....
            assert r.ready.isSet(), "Rendezvous timed out"

        for i in range(self.end_request):
            self.client.end_request()


class CreateAndReleaseSocketNoRendezvous(MongoThread):
    """A thread or greenlet that acquires a socket and terminates without
    waiting for other threads to reach rendezvous point.
    """
    class Rendezvous(object):
        def __init__(self, nthreads, use_greenlets):
            self.nthreads = nthreads
            self.nthreads_run = 0
            if use_greenlets:
                self.lock = gevent.coros.RLock()
                self.ready = gevent.event.Event()
            else:
                self.lock = threading.Lock()
                self.ready = threading.Event()

    def __init__(self, ut, client, start_request, end_request):
        super(CreateAndReleaseSocketNoRendezvous, self).__init__(ut)
        self.client = client
        self.start_request = start_request
        self.end_request = end_request

    def run_mongo_thread(self):
        # Do an operation that requires a socket.
        # test_max_pool_size uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        for i in range(self.start_request):
            self.client.start_request()

        # Use a socket
        self.client[DB].test.find_one()
        for i in range(self.end_request):
            self.client.end_request()


class _TestPoolingBase(object):
    """Base class for all client-pool tests. Doesn't inherit from
    unittest.TestCase, and its name is prefixed with "_" to avoid being
    run by nose. Real tests double-inherit from this base and from TestCase.
    """
    use_greenlets = False

    def setUp(self):
        if self.use_greenlets:
            if not has_gevent:
                raise SkipTest("Gevent not installed")

            # Note we don't do patch_thread() or patch_all() - we're
            # testing here that patch_thread() is unnecessary for
            # the client pool to work properly.
            monkey.patch_socket()

        self.c = self.get_client(auto_start_request=False)

        # reset the db
        db = self.c[DB]
        db.unique.drop()
        db.test.drop()
        db.unique.insert({"_id": "jesse"})

        db.test.insert([{} for i in range(10)])

    def tearDown(self):
        self.c.close()
        self.c = None
        if self.use_greenlets:
            # Undo patch
            reload(socket)

    def get_client(self, *args, **kwargs):
        opts = kwargs.copy()
        opts['use_greenlets'] = self.use_greenlets
        return get_client(*args, **opts)

    def get_pool(self, *args, **kwargs):
        kwargs['use_greenlets'] = self.use_greenlets
        return Pool(*args, **kwargs)

    def sleep(self, seconds):
        if self.use_greenlets:
            gevent.sleep(seconds)
        else:
            time.sleep(seconds)

    def assert_no_request(self):
        self.assertTrue(
            self.c._MongoClient__member is None or
            NO_REQUEST == get_pool(self.c)._get_request_state()
        )

    def assert_request_without_socket(self):
        self.assertEqual(
            NO_SOCKET_YET, get_pool(self.c)._get_request_state()
        )

    def assert_request_with_socket(self):
        self.assertTrue(isinstance(
            get_pool(self.c)._get_request_state(), SocketInfo
        ))

    def assert_pool_size(self, pool_size):
        if pool_size == 0:
            self.assertTrue(
                self.c._MongoClient__member is None
                or not get_pool(self.c).sockets
            )
        else:
            self.assertEqual(
                pool_size, len(get_pool(self.c).sockets)
            )


class _TestPooling(_TestPoolingBase):
    """Basic pool tests, to be run both with threads and with greenlets."""
    def test_max_pool_size_validation(self):
        self.assertRaises(
            ConfigurationError, MongoClient, host=host, port=port,
            max_pool_size=-1
        )

        self.assertRaises(
            ConfigurationError, MongoClient, host=host, port=port,
            max_pool_size='foo'
        )

        c = MongoClient(host=host, port=port, max_pool_size=100)
        self.assertEqual(c.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_simple_disconnect(self):
        # MongoClient just created, expect 1 free socket
        self.assert_pool_size(1)
        self.assert_no_request()

        self.c.start_request()
        self.assert_request_without_socket()
        cursor = self.c[DB].stuff.find()

        # Cursor hasn't actually caused a request yet, so there's still 1 free
        # socket.
        self.assert_pool_size(1)
        self.assert_request_without_socket()

        # Actually make a request to server, triggering a socket to be
        # allocated to the request
        list(cursor)
        self.assert_pool_size(0)
        self.assert_request_with_socket()

        # Pool returns to its original state
        self.c.end_request()
        self.assert_no_request()
        self.assert_pool_size(1)

        self.c.disconnect()
        self.assert_pool_size(0)
        self.assert_no_request()

    def test_disconnect(self):
        run_cases(self, [SaveAndFind, Disconnect, Unique])

    def test_independent_pools(self):
        # Test for regression of very early PyMongo bug: separate pools shared
        # state.
        p = self.get_pool((host, port), 10, None, None, False)
        self.c.start_request()
        self.c.pymongo_test.test.find_one()
        self.assertEqual(set(), p.sockets)
        self.c.end_request()
        self.assert_pool_size(1)
        self.assertEqual(set(), p.sockets)

    def test_dependent_pools(self):
        self.assert_pool_size(1)
        self.c.start_request()
        self.assert_request_without_socket()
        self.c.pymongo_test.test.find_one()
        self.assert_request_with_socket()
        self.assert_pool_size(0)
        self.c.end_request()
        self.assert_pool_size(1)

        t = OneOp(self)
        t.start()
        t.join()
        self.assertTrue(t.passed, "OneOp.run() threw exception")

        self.assert_pool_size(1)
        self.c.pymongo_test.test.find_one()
        self.assert_pool_size(1)

    def test_multiple_connections(self):
        a = self.get_client(auto_start_request=False)
        b = self.get_client(auto_start_request=False)
        self.assertEqual(1, len(get_pool(a).sockets))
        self.assertEqual(1, len(get_pool(b).sockets))

        a.start_request()
        a.pymongo_test.test.find_one()
        self.assertEqual(0, len(get_pool(a).sockets))
        a.end_request()
        self.assertEqual(1, len(get_pool(a).sockets))
        self.assertEqual(1, len(get_pool(b).sockets))
        a_sock = one(get_pool(a).sockets)

        b.end_request()
        self.assertEqual(1, len(get_pool(a).sockets))
        self.assertEqual(1, len(get_pool(b).sockets))

        b.start_request()
        b.pymongo_test.test.find_one()
        self.assertEqual(1, len(get_pool(a).sockets))
        self.assertEqual(0, len(get_pool(b).sockets))

        b.end_request()
        b_sock = one(get_pool(b).sockets)
        b.pymongo_test.test.find_one()
        a.pymongo_test.test.find_one()

        self.assertEqual(b_sock,
                         get_pool(b).get_socket())
        self.assertEqual(a_sock,
                         get_pool(a).get_socket())

        a_sock.close()
        b_sock.close()

    def test_request(self):
        # Check that Pool gives two different sockets in two calls to
        # get_socket() -- doesn't automatically put us in a request any more
        cx_pool = self.get_pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False
        )

        sock0 = cx_pool.get_socket()
        sock1 = cx_pool.get_socket()

        self.assertNotEqual(sock0, sock1)

        # Now in a request, we'll get the same socket both times
        cx_pool.start_request()

        sock2 = cx_pool.get_socket()
        sock3 = cx_pool.get_socket()
        self.assertEqual(sock2, sock3)

        # Pool didn't keep reference to sock0 or sock1; sock2 and 3 are new
        self.assertNotEqual(sock0, sock2)
        self.assertNotEqual(sock1, sock2)

        # Return the request sock to pool
        cx_pool.end_request()

        sock4 = cx_pool.get_socket()
        sock5 = cx_pool.get_socket()

        # Not in a request any more, we get different sockets
        self.assertNotEqual(sock4, sock5)

        # end_request() returned sock2 to pool
        self.assertEqual(sock4, sock2)

        for s in [sock0, sock1, sock2, sock3, sock4, sock5]:
            s.close()

    def test_reset_and_request(self):
        # reset() is called after a fork, or after a socket error. Ensure that
        # a new request is begun if a request was in progress when the reset()
        # occurred, otherwise no request is begun.
        p = self.get_pool((host, port), 10, None, None, False)
        self.assertFalse(p.in_request())
        p.start_request()
        self.assertTrue(p.in_request())
        p.reset()
        self.assertTrue(p.in_request())
        p.end_request()
        self.assertFalse(p.in_request())
        p.reset()
        self.assertFalse(p.in_request())

    def test_pool_reuses_open_socket(self):
        # Test Pool's _check_closed() method doesn't close a healthy socket
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool._check_interval_seconds = 0  # Always check.
        sock_info = cx_pool.get_socket()
        cx_pool.maybe_return_socket(sock_info)

        new_sock_info = cx_pool.get_socket()
        self.assertEqual(sock_info, new_sock_info)
        cx_pool.maybe_return_socket(new_sock_info)
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_socket(self):
        # Test that Pool removes dead socket and the socket doesn't return
        # itself PYTHON-344
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool._check_interval_seconds = 0  # Always check.
        sock_info = cx_pool.get_socket()

        # Simulate a closed socket without telling the SocketInfo it's closed
        sock_info.sock.close()
        self.assertTrue(pymongo.pool._closed(sock_info.sock))
        cx_pool.maybe_return_socket(sock_info)
        new_sock_info = cx_pool.get_socket()
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertNotEqual(sock_info, new_sock_info)
        cx_pool.maybe_return_socket(new_sock_info)
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_request_socket_after_check(self):
        # Test that Pool keeps request going even if a socket dies in request
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool._check_interval_seconds = 0  # Always check.
        cx_pool.start_request()

        # Get the request socket
        sock_info = cx_pool.get_socket()
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertEqual(sock_info, cx_pool._get_request_state())
        sock_info.sock.close()
        cx_pool.maybe_return_socket(sock_info)

        # Although the request socket died, we're still in a request with a
        # new socket
        new_sock_info = cx_pool.get_socket()
        self.assertTrue(cx_pool.in_request())
        self.assertNotEqual(sock_info, new_sock_info)
        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        cx_pool.maybe_return_socket(new_sock_info)
        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        self.assertEqual(0, len(cx_pool.sockets))

        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_request_socket(self):
        # Test that Pool keeps request going even if a socket dies in request
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool.start_request()

        # Get the request socket
        sock_info = cx_pool.get_socket()
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertEqual(sock_info, cx_pool._get_request_state())

        # Unlike in test_pool_removes_dead_request_socket_after_check, we
        # set sock_info.closed and *don't* wait for it to be checked.
        sock_info.close()
        cx_pool.maybe_return_socket(sock_info)

        # Although the request socket died, we're still in a request with a
        # new socket
        new_sock_info = cx_pool.get_socket()
        self.assertTrue(cx_pool.in_request())
        self.assertNotEqual(sock_info, new_sock_info)
        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        cx_pool.maybe_return_socket(new_sock_info)
        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        self.assertEqual(0, len(cx_pool.sockets))

        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_socket_after_request(self):
        # Test that Pool handles a socket dying that *used* to be the request
        # socket.
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool._check_interval_seconds = 0  # Always check.
        cx_pool.start_request()

        # Get the request socket
        sock_info = cx_pool.get_socket()
        self.assertEqual(sock_info, cx_pool._get_request_state())
        cx_pool.maybe_return_socket(sock_info)

        # End request
        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

        # Kill old request socket
        sock_info.sock.close()

        # Dead socket detected and removed
        new_sock_info = cx_pool.get_socket()
        self.assertFalse(cx_pool.in_request())
        self.assertNotEqual(sock_info, new_sock_info)
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertFalse(pymongo.pool._closed(new_sock_info.sock))
        cx_pool.maybe_return_socket(new_sock_info)
        self.assertEqual(1, len(cx_pool.sockets))

    def test_dead_request_socket_with_max_size(self):
        # When a pool replaces a dead request socket, the semaphore it uses
        # to enforce max_size should remain unaffected.
        cx_pool = self.get_pool(
            (host, port), 1, None, None, False, wait_queue_timeout=1)

        cx_pool._check_interval_seconds = 0  # Always check.
        cx_pool.start_request()

        # Get and close the request socket.
        request_sock_info = cx_pool.get_socket()
        request_sock_info.sock.close()
        cx_pool.maybe_return_socket(request_sock_info)

        # Detects closed socket and creates new one, semaphore value still 0.
        request_sock_info_2 = cx_pool.get_socket()
        self.assertNotEqual(request_sock_info, request_sock_info_2)
        cx_pool.maybe_return_socket(request_sock_info_2)
        cx_pool.end_request()

        # Semaphore value now 1; we can get a socket.
        sock_info = cx_pool.get_socket()

        # Clean up.
        cx_pool.maybe_return_socket(sock_info)

    def test_socket_reclamation(self):
        if sys.platform.startswith('java'):
            raise SkipTest("Jython can't do socket reclamation")

        # Check that if a thread starts a request and dies without ending
        # the request, that the socket is reclaimed into the pool.
        cx_pool = self.get_pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False,
        )

        self.assertEqual(0, len(cx_pool.sockets))

        lock = None
        the_sock = [None]

        def leak_request():
            self.assertEqual(NO_REQUEST, cx_pool._get_request_state())
            cx_pool.start_request()
            self.assertEqual(NO_SOCKET_YET, cx_pool._get_request_state())
            sock_info = cx_pool.get_socket()
            self.assertEqual(sock_info, cx_pool._get_request_state())
            the_sock[0] = id(sock_info.sock)
            cx_pool.maybe_return_socket(sock_info)

            if not self.use_greenlets:
                lock.release()

        if self.use_greenlets:
            g = Greenlet(leak_request)
            g.start()
            g.join(1)
            self.assertTrue(g.ready(), "Greenlet is hung")

            # In Gevent after 0.13.8, join() returns before the Greenlet.link
            # callback fires. Give it a moment to reclaim the socket.
            gevent.sleep(0.1)
        else:
            lock = thread.allocate_lock()
            lock.acquire()

            # Start a thread WITHOUT a threading.Thread - important to test that
            # Pool can deal with primitive threads.
            thread.start_new_thread(leak_request, ())

            # Join thread
            acquired = lock.acquire()
            self.assertTrue(acquired, "Thread is hung")

            # Make sure thread is really gone
            time.sleep(1)

            if 'PyPy' in sys.version:
                gc.collect()

            # Access the thread local from the main thread to trigger the
            # ThreadVigil's delete callback, returning the request socket to
            # the pool.
            # In Python 2.7.0 and lesser, a dead thread's locals are deleted
            # and those locals' weakref callbacks are fired only when another
            # thread accesses the locals and finds the thread state is stale,
            # see http://bugs.python.org/issue1868. Accessing the thread
            # local from the main thread is a necessary part of this test, and
            # realistic: in a multithreaded web server a new thread will access
            # Pool._ident._local soon after an old thread has died.
            cx_pool._ident.get()

        # Pool reclaimed the socket
        self.assertEqual(1, len(cx_pool.sockets))
        self.assertEqual(the_sock[0], id(one(cx_pool.sockets).sock))
        self.assertEqual(0, len(cx_pool._tid_to_sock))


class _TestMaxPoolSize(_TestPoolingBase):
    """Test that connection pool keeps proper number of idle sockets open,
    no matter how start/end_request are called. To be run both with threads and
    with greenlets.
    """
    def _test_max_pool_size(
            self, start_request, end_request, max_pool_size=4, nthreads=10):
        """Start `nthreads` threads. Each calls start_request `start_request`
        times, then find_one and waits at a barrier; once all reach the barrier
        each calls end_request `end_request` times. The test asserts that the
        pool ends with min(max_pool_size, nthreads) sockets or, if
        start_request wasn't called, at least one socket.

        This tests both max_pool_size enforcement and that leaked request
        sockets are eventually returned to the pool when their threads end.

        You may need to increase ulimit -n on Mac.

        If you increase nthreads over about 35, note a
        Gevent 0.13.6 bug on Mac: Greenlet.join() hangs if more than
        about 35 Greenlets share a MongoClient. Apparently fixed in
        recent Gevent development.
        """
        if start_request:
            if max_pool_size is not None and max_pool_size < nthreads:
                raise AssertionError("Deadlock")

        c = self.get_client(
            max_pool_size=max_pool_size, auto_start_request=False)

        rendezvous = CreateAndReleaseSocket.Rendezvous(
            nthreads, self.use_greenlets)

        threads = []
        for i in range(nthreads):
            t = CreateAndReleaseSocket(
                self, c, start_request, end_request, rendezvous)
            threads.append(t)

        for t in threads:
            t.start()

        if 'PyPy' in sys.version:
            # With PyPy we need to kick off the gc whenever the threads hit the
            # rendezvous since nthreads > max_pool_size.
            gc_collect_until_done(threads)
        else:
            for t in threads:
                t.join()

        # join() returns before the thread state is cleared; give it time.
        self.sleep(1)

        for t in threads:
            self.assertTrue(t.passed)

        # Socket-reclamation doesn't work in Jython
        if not sys.platform.startswith('java'):
            cx_pool = get_pool(c)

            # Socket-reclamation depends on timely garbage-collection
            if 'PyPy' in sys.version:
                gc.collect()

            if self.use_greenlets:
                # Wait for Greenlet.link() callbacks to execute
                the_hub = hub.get_hub()
                if hasattr(the_hub, 'join'):
                    # Gevent 1.0
                    the_hub.join()
                else:
                    # Gevent 0.13 and less
                    the_hub.shutdown()

            if start_request:
                # Trigger final cleanup in Python <= 2.7.0.
                cx_pool._ident.get()
                expected_idle = min(max_pool_size, nthreads)
                message = (
                    '%d idle sockets (expected %d) and %d request sockets'
                    ' (expected 0)' % (
                        len(cx_pool.sockets), expected_idle,
                        len(cx_pool._tid_to_sock)))

                self.assertEqual(
                    expected_idle, len(cx_pool.sockets), message)
            else:
                # Without calling start_request(), threads can safely share
                # sockets; the number running concurrently, and hence the
                # number of sockets needed, is between 1 and 10, depending
                # on thread-scheduling.
                self.assertTrue(len(cx_pool.sockets) >= 1)

            # thread.join completes slightly *before* thread locals are
            # cleaned up, so wait up to 5 seconds for them.
            self.sleep(0.1)
            cx_pool._ident.get()
            start = time.time()

            while (
                not cx_pool.sockets
                and cx_pool._socket_semaphore.counter < max_pool_size
                and (time.time() - start) < 5
            ):
                self.sleep(0.1)
                cx_pool._ident.get()

            if max_pool_size is not None:
                self.assertEqual(
                    max_pool_size,
                    cx_pool._socket_semaphore.counter)

            self.assertEqual(0, len(cx_pool._tid_to_sock))

    def _test_max_pool_size_no_rendezvous(self, start_request, end_request):
        max_pool_size = 5
        c = self.get_client(
            max_pool_size=max_pool_size, auto_start_request=False)

        # If you increase nthreads over about 35, note a
        # Gevent 0.13.6 bug on Mac, Greenlet.join() hangs if more than
        # about 35 Greenlets share a MongoClient. Apparently fixed in
        # recent Gevent development.

        # On the other hand, nthreads had better be much larger than
        # max_pool_size to ensure that max_pool_size sockets are actually
        # required at some point in this test's execution.
        nthreads = 10

        if (sys.platform.startswith('java')
                and start_request > end_request
                and nthreads > max_pool_size):

            # Since Jython can't reclaim the socket and release the semaphore
            # after a thread leaks a request, we'll exhaust the semaphore and
            # deadlock.
            raise SkipTest("Jython can't do socket reclamation")

        threads = []
        for i in range(nthreads):
            t = CreateAndReleaseSocketNoRendezvous(
                self, c, start_request, end_request)
            threads.append(t)

        for t in threads:
            t.start()

        if 'PyPy' in sys.version:
            # With PyPy we need to kick off the gc whenever the threads hit the
            # rendezvous since nthreads > max_pool_size.
            gc_collect_until_done(threads)
        else:
            for t in threads:
                t.join()

        for t in threads:
            self.assertTrue(t.passed)

        cx_pool = get_pool(c)

        # Socket-reclamation depends on timely garbage-collection
        if 'PyPy' in sys.version:
            gc.collect()

        if self.use_greenlets:
            # Wait for Greenlet.link() callbacks to execute
            the_hub = hub.get_hub()
            if hasattr(the_hub, 'join'):
                # Gevent 1.0
                the_hub.join()
            else:
                # Gevent 0.13 and less
                the_hub.shutdown()

        # thread.join completes slightly *before* thread locals are
        # cleaned up, so wait up to 5 seconds for them.
        self.sleep(0.1)
        cx_pool._ident.get()
        start = time.time()

        while (
            not cx_pool.sockets
            and cx_pool._socket_semaphore.counter < max_pool_size
            and (time.time() - start) < 5
        ):
            self.sleep(0.1)
            cx_pool._ident.get()

        self.assertTrue(len(cx_pool.sockets) >= 1)
        self.assertEqual(max_pool_size, cx_pool._socket_semaphore.counter)

    def test_max_pool_size(self):
        self._test_max_pool_size(
            start_request=0, end_request=0, nthreads=10, max_pool_size=4)

    def test_max_pool_size_none(self):
        self._test_max_pool_size(
            start_request=0, end_request=0, nthreads=10, max_pool_size=None)

    def test_max_pool_size_with_request(self):
        self._test_max_pool_size(
            start_request=1, end_request=1, nthreads=10, max_pool_size=10)

    def test_max_pool_size_with_multiple_request(self):
        self._test_max_pool_size(
            start_request=10, end_request=10, nthreads=10, max_pool_size=10)

    def test_max_pool_size_with_redundant_request(self):
        self._test_max_pool_size(
            start_request=2, end_request=1, nthreads=10, max_pool_size=10)

    def test_max_pool_size_with_redundant_request2(self):
        self._test_max_pool_size(
            start_request=20, end_request=1, nthreads=10, max_pool_size=10)

    def test_max_pool_size_with_redundant_request_no_rendezvous(self):
        self._test_max_pool_size_no_rendezvous(2, 1)

    def test_max_pool_size_with_redundant_request_no_rendezvous2(self):
        self._test_max_pool_size_no_rendezvous(20, 1)

    def test_max_pool_size_with_leaked_request(self):
        # Call start_request() but not end_request() -- when threads die, they
        # should return their request sockets to the pool.
        self._test_max_pool_size(
            start_request=1, end_request=0, nthreads=10, max_pool_size=10)

    def test_max_pool_size_with_leaked_request_no_rendezvous(self):
        self._test_max_pool_size_no_rendezvous(1, 0)

    def test_max_pool_size_with_end_request_only(self):
        # Call end_request() but not start_request()
        self._test_max_pool_size(0, 1)

    def test_max_pool_size_with_connection_failure(self):
        # The pool acquires its semaphore before attempting to connect; ensure
        # it releases the semaphore on connection failure.
        class TestPool(Pool):
            def connect(self):
                raise socket.error()

        test_pool = TestPool(
            pair=('example.com', 27017),
            max_size=1,
            net_timeout=1,
            conn_timeout=1,
            use_ssl=False,
            wait_queue_timeout=1,
            use_greenlets=self.use_greenlets)

        # First call to get_socket fails; if pool doesn't release its semaphore
        # then the second call raises "ConnectionFailure: Timed out waiting for
        # socket from pool" instead of the socket.error.
        for i in range(2):
            self.assertRaises(socket.error, test_pool.get_socket)


class SocketGetter(MongoThread):
    """Utility for _TestMaxOpenSockets and _TestWaitQueueMultiple"""
    def __init__(self, test_case, pool):
        super(SocketGetter, self).__init__(test_case)
        self.state = 'init'
        self.pool = pool
        self.sock = None

    def run(self):
        self.state = 'get_socket'
        self.sock = self.pool.get_socket()
        self.state = 'sock'


class _TestMaxOpenSockets(_TestPoolingBase):
    """Test that connection pool doesn't open more than max_size sockets.
    To be run both with threads and with greenlets.
    """
    def get_pool_with_wait_queue_timeout(self, wait_queue_timeout):
        return self.get_pool((host, port),
                             1, None, None,
                             False,
                             wait_queue_timeout=wait_queue_timeout,
                             wait_queue_multiple=None)

    def test_wait_queue_timeout(self):
        wait_queue_timeout = 2  # Seconds
        pool = self.get_pool_with_wait_queue_timeout(wait_queue_timeout)
        sock_info = pool.get_socket()
        start = time.time()
        self.assertRaises(ConnectionFailure, pool.get_socket)
        duration = time.time() - start
        self.assertTrue(
            abs(wait_queue_timeout - duration) < 1,
            "Waited %.2f seconds for a socket, expected %f" % (
                duration, wait_queue_timeout))

        sock_info.close()

    def test_blocking(self):
        # Verify get_socket() with no wait_queue_timeout blocks forever.
        pool = self.get_pool_with_wait_queue_timeout(None)

        # Reach max_size.
        s1 = pool.get_socket()
        t = SocketGetter(self, pool)
        t.start()
        while t.state != 'get_socket':
            self.sleep(0.1)

        self.sleep(1)
        self.assertEqual(t.state, 'get_socket')
        pool.maybe_return_socket(s1)
        while t.state != 'sock':
            self.sleep(0.1)

        self.assertEqual(t.state, 'sock')
        self.assertEqual(t.sock, s1)
        s1.close()


class _TestWaitQueueMultiple(_TestPoolingBase):
    """Test that connection pool doesn't allow more than
    waitQueueMultiple * max_size waiters.
    To be run both with threads and with greenlets.
    """
    def get_pool_with_wait_queue_multiple(self, wait_queue_multiple):
        return self.get_pool((host, port),
                             2, None, None,
                             False,
                             wait_queue_timeout=None,
                             wait_queue_multiple=wait_queue_multiple)

    def test_wait_queue_multiple(self):
        pool = self.get_pool_with_wait_queue_multiple(3)

        # Reach max_size sockets.
        socket_info_0 = pool.get_socket()
        socket_info_1 = pool.get_socket()

        # Reach max_size * wait_queue_multiple waiters.
        threads = []
        for _ in xrange(6):
            t = SocketGetter(self, pool)
            t.start()
            threads.append(t)

        self.sleep(1)
        for t in threads:
            self.assertEqual(t.state, 'get_socket')

        self.assertRaises(ExceededMaxWaiters, pool.get_socket)
        socket_info_0.close()
        socket_info_1.close()

    def test_wait_queue_multiple_unset(self):
        pool = self.get_pool_with_wait_queue_multiple(None)
        socks = []
        for _ in xrange(2):
            sock = pool.get_socket()
            socks.append(sock)
        threads = []
        for _ in xrange(30):
            t = SocketGetter(self, pool)
            t.start()
            threads.append(t)
        self.sleep(1)
        for t in threads:
            self.assertEqual(t.state, 'get_socket')

        for socket_info in socks:
            socket_info.close()


class _TestPoolSocketSharing(_TestPoolingBase):
    """Directly test that two simultaneous operations don't share a socket. To
    be run both with threads and with greenlets.
    """
    def _test_pool(self, use_request):
        """
        Test that the connection pool prevents both threads and greenlets from
        using a socket at the same time.

        Sequence:
        gr0: start a slow find()
        gr1: start a fast find()
        gr1: get results
        gr0: get results
        """
        cx = get_client(
            use_greenlets=self.use_greenlets,
            auto_start_request=False
        )

        db = cx.pymongo_test
        db.test.remove()
        db.test.insert({'_id': 1})

        history = []

        def find_fast():
            if use_request:
                cx.start_request()

            history.append('find_fast start')

            # With greenlets and the old connection._Pool, this would throw
            # AssertionError: "This event is already used by another
            # greenlet"
            self.assertEqual({'_id': 1}, db.test.find_one())
            history.append('find_fast done')

            if use_request:
                cx.end_request()

        def find_slow():
            if use_request:
                cx.start_request()

            history.append('find_slow start')

            # Javascript function that pauses N seconds per document
            fn = delay(10)
            if (is_mongos(db.connection) or not
                version.at_least(db.connection, (1, 7, 2))):
                # mongos doesn't support eval so we have to use $where
                # which is less reliable in this context.
                self.assertEqual(1, db.test.find({"$where": fn}).count())
            else:
                # 'nolock' allows find_fast to start and finish while we're
                # waiting for this to complete.
                self.assertEqual({'ok': 1.0, 'retval': True},
                                 db.command('eval', fn, nolock=True))

            history.append('find_slow done')

            if use_request:
                cx.end_request()

        if self.use_greenlets:
            gr0, gr1 = Greenlet(find_slow), Greenlet(find_fast)
            gr0.start()
            gr1.start_later(.1)
        else:
            gr0 = threading.Thread(target=find_slow)
            gr0.setDaemon(True)
            gr1 = threading.Thread(target=find_fast)
            gr1.setDaemon(True)

            gr0.start()
            time.sleep(.1)
            gr1.start()

        gr0.join()
        gr1.join()

        self.assertEqual([
            'find_slow start',
            'find_fast start',
            'find_fast done',
            'find_slow done',
        ], history)

    def test_pool(self):
        self._test_pool(use_request=False)

    def test_pool_request(self):
        self._test_pool(use_request=True)

# Copyright 2009-2014 MongoDB, Inc.
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

"""Test built in connection-pooling with threads."""

import gc
import random
import socket
import sys
import threading
import time

from pymongo import MongoClient
from pymongo.errors import ConfigurationError, ConnectionFailure, \
    ExceededMaxWaiters
from pymongo.server_selectors import writable_server_selector

sys.path[0:0] = [""]

from bson.py3compat import thread
from pymongo.pool import (NO_REQUEST,
                          NO_SOCKET_YET,
                          Pool,
                          PoolOptions,
                          SocketInfo, _closed)
from test import host, port, SkipTest, unittest, client_context
from test.utils import get_pool, get_client, one


@client_context.require_connection
def setUpModule():
    pass

N = 10
DB = "pymongo-pooling-tests"


def gc_collect_until_done(threads, timeout=60):
    start = time.time()
    running = list(threads)
    while running:
        assert (time.time() - start) < timeout, "Threads timed out"
        for t in running:
            t.join(0.1)
            if not t.isAlive():
                running.remove(t)
        gc.collect()


class MongoThread(threading.Thread):
    """A thread that uses a MongoClient."""
    def __init__(self, client):
        super(MongoThread, self).__init__()
        self.daemon = True  # Don't hang whole test if thread hangs.
        self.client = client
        self.db = self.client[DB]
        self.passed = False

    def run(self):
        self.run_mongo_thread()
        self.passed = True

    def run_mongo_thread(self):
        raise NotImplementedError


class SaveAndFind(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            rand = random.randint(0, N)
            _id = self.db.sf.save({"x": rand})
            assert rand == self.db.sf.find_one(_id)["x"]


class Unique(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            self.client.start_request()
            self.db.unique.insert({})  # no error
            self.client.end_request()


class NonUnique(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            self.client.start_request()
            self.db.unique.insert({"_id": "jesse"}, w=0)
            assert self.db.error() is not None
            self.client.end_request()


class Disconnect(MongoThread):
    def run_mongo_thread(self):
        for _ in range(N):
            self.client.disconnect()


class NoRequest(MongoThread):
    def run_mongo_thread(self):
        self.client.start_request()
        errors = 0
        for _ in range(N):
            self.db.unique.insert({"_id": "jesse"}, w=0)
            if not self.db.error():
                errors += 1

        self.client.end_request()
        assert errors == 0


class SocketGetter(MongoThread):
    """Utility for _TestMaxOpenSockets and _TestWaitQueueMultiple"""
    def __init__(self, client, pool):
        super(SocketGetter, self).__init__(client)
        self.state = 'init'
        self.pool = pool
        self.sock = None

    def run_mongo_thread(self):
        self.state = 'get_socket'
        self.sock = self.pool.get_socket({}, 0, 0)
        self.state = 'sock'


def run_cases(client, cases):
    threads = []
    n_runs = 5

    for case in cases:
        for i in range(n_runs):
            t = case(client)
            t.start()
            threads.append(t)

    for t in threads:
        t.join()

    for t in threads:
        assert t.passed, "%s.run() threw an exception" % repr(t)


class CreateAndReleaseSocket(MongoThread):
    """Gets a socket, waits for all threads to reach rendezvous, and quits."""
    class Rendezvous(object):
        def __init__(self, nthreads):
            self.nthreads = nthreads
            self.nthreads_run = 0
            self.lock = threading.Lock()
            self.reset_ready()

        def reset_ready(self):
            self.ready = threading.Event()

    def __init__(self, client, start_request, end_request, rendezvous):
        super(CreateAndReleaseSocket, self).__init__(client)
        self.start_request = start_request
        self.end_request = end_request
        self.rendezvous = rendezvous

    def run_mongo_thread(self):
        # Do an operation that requires a socket.
        # TestPoolMaxSize uses this to spin up lots of threads requiring
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
            # Everyone's here, let them finish.
            r.ready.set()
            r.lock.release()
        else:
            r.lock.release()
            r.ready.wait(30)  # Wait thirty seconds....
            assert r.ready.isSet(), "Rendezvous timed out"

        for i in range(self.end_request):
            self.client.end_request()


class CreateAndReleaseSocketNoRendezvous(MongoThread):
    """Gets a socket and quits. No synchronization with other threads."""
    def __init__(self, client, start_request, end_request):
        super(CreateAndReleaseSocketNoRendezvous, self).__init__(client)
        self.start_request = start_request
        self.end_request = end_request

    def run_mongo_thread(self):
        # Do an operation that requires a socket.
        # TestPoolMaxSize uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        for i in range(self.start_request):
            self.client.start_request()

        # Use a socket
        self.client[DB].test.find_one()
        for i in range(self.end_request):
            self.client.end_request()


class _TestPoolingBase(unittest.TestCase):
    """Base class for all connection-pool tests."""

    def setUp(self):
        self.c = get_client()
        db = self.c[DB]
        db.unique.drop()
        db.test.drop()
        db.unique.insert({"_id": "jesse"})
        db.test.insert([{} for _ in range(10)])

    def create_pool(self, pair=(host, port), *args, **kwargs):
        return Pool(pair, PoolOptions(*args, **kwargs))

    def assert_no_request(self):
        try:
            server = self.c._topology.select_server(
                writable_server_selector,
                server_wait_time=0)

            self.assertEqual(NO_REQUEST, server.pool._get_request_state())
        except ConnectionFailure:
            # Success: we're asserting that we're not in a request, but there's
            # no pool at all so the assertion is true.
            pass

    def assert_request_without_socket(self):
        self.assertEqual(NO_SOCKET_YET, get_pool(self.c)._get_request_state())

    def assert_request_with_socket(self):
        self.assertTrue(isinstance(
            get_pool(self.c)._get_request_state(), SocketInfo))

    def assert_pool_size(self, pool_size):
        if pool_size == 0:
            try:
                server = self.c._topology.select_server(
                    writable_server_selector,
                    server_wait_time=0)

                self.assertEqual(0, len(server.pool.sockets))
            except ConnectionFailure:
                # Success: we're asserting that pool size is 0, and there's no
                # pool at all so the assertion is true.
                pass
        else:
            self.assertEqual(pool_size, len(get_pool(self.c).sockets))


class TestPooling(_TestPoolingBase):
    def test_max_pool_size_validation(self):
        self.assertRaises(
            ConfigurationError, MongoClient, host=host, port=port,
            max_pool_size=-1)

        self.assertRaises(
            ConfigurationError, MongoClient, host=host, port=port,
            max_pool_size='foo')

        c = MongoClient(host=host, port=port, max_pool_size=100)
        self.assertEqual(c.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self.c, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_simple_disconnect(self):
        # MongoClient just created, expect 1 free socket.
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
        # allocated to the request.
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
        run_cases(self.c, [SaveAndFind, Disconnect, Unique])

    def test_request(self):
        # Check that Pool gives two different sockets in two calls to
        # get_socket().
        cx_pool = self.create_pool(
            pair=(host, port),
            max_pool_size=10,
            connect_timeout=1000,
            socket_timeout=1000)

        sock0 = cx_pool.get_socket({}, 0, 0)
        sock1 = cx_pool.get_socket({}, 0, 0)

        self.assertNotEqual(sock0, sock1)

        # Now in a request, we'll get the same socket both times
        cx_pool.start_request()

        sock2 = cx_pool.get_socket({}, 0, 0)
        sock3 = cx_pool.get_socket({}, 0, 0)
        self.assertEqual(sock2, sock3)

        # Pool didn't keep reference to sock0 or sock1; sock2 and 3 are new
        self.assertNotEqual(sock0, sock2)
        self.assertNotEqual(sock1, sock2)

        # Return the request sock to pool
        cx_pool.end_request()

        sock4 = cx_pool.get_socket({}, 0, 0)
        sock5 = cx_pool.get_socket({}, 0, 0)

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
        p = self.create_pool(max_pool_size=10)
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
        # Test Pool's _check_closed() method doesn't close a healthy socket.
        cx_pool = self.create_pool(max_pool_size=10)
        cx_pool._check_interval_seconds = 0  # Always check.
        sock_info = cx_pool.get_socket({}, 0, 0)
        cx_pool.maybe_return_socket(sock_info)

        new_sock_info = cx_pool.get_socket({}, 0, 0)
        self.assertEqual(sock_info, new_sock_info)
        cx_pool.maybe_return_socket(new_sock_info)
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_socket(self):
        # Test that Pool removes dead socket and the socket doesn't return
        # itself PYTHON-344
        cx_pool = self.create_pool(max_pool_size=10)
        cx_pool._check_interval_seconds = 0  # Always check.

        with cx_pool.get_socket({}, 0, 0) as sock_info:
            # Simulate a closed socket without telling the SocketInfo it's
            # closed.
            sock_info.sock.close()
            self.assertTrue(_closed(sock_info.sock))

        with cx_pool.get_socket({}, 0, 0) as new_sock_info:
            self.assertEqual(0, len(cx_pool.sockets))
            self.assertNotEqual(sock_info, new_sock_info)

        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_request_socket_after_check(self):
        # Test that Pool keeps request going even if a socket dies in request.
        cx_pool = self.create_pool(max_pool_size=10)
        cx_pool._check_interval_seconds = 0  # Always check.
        cx_pool.start_request()

        # Get the request socket.
        with cx_pool.get_socket({}, 0, 0) as sock_info:
            self.assertEqual(0, len(cx_pool.sockets))
            self.assertEqual(sock_info, cx_pool._get_request_state())
            sock_info.sock.close()

        # Although the request socket died, we're still in a request with a
        # new socket.
        with cx_pool.get_socket({}, 0, 0) as new_sock_info:
            self.assertTrue(cx_pool.in_request())
            self.assertNotEqual(sock_info, new_sock_info)
            self.assertEqual(new_sock_info, cx_pool._get_request_state())

        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        self.assertEqual(0, len(cx_pool.sockets))

        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_request_socket(self):
        # Test that Pool keeps request going even if a socket dies in request.
        cx_pool = self.create_pool(max_pool_size=10)
        cx_pool.start_request()

        # Get the request socket
        with cx_pool.get_socket({}, 0, 0) as sock_info:
            self.assertEqual(0, len(cx_pool.sockets))
            self.assertEqual(sock_info, cx_pool._get_request_state())

            # Unlike in test_pool_removes_dead_request_socket_after_check, we
            # set sock_info.closed and *don't* wait for it to be checked.
            sock_info.close()

        # Although the request socket died, we're still in a request with a
        # new socket
        with cx_pool.get_socket({}, 0, 0) as new_sock_info:
            self.assertTrue(cx_pool.in_request())
            self.assertNotEqual(sock_info, new_sock_info)
            self.assertEqual(new_sock_info, cx_pool._get_request_state())

        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        self.assertEqual(0, len(cx_pool.sockets))

        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_socket_after_request(self):
        # Test that Pool handles a socket dying that *used* to be the request
        # socket.
        cx_pool = self.create_pool(max_pool_size=10)
        cx_pool._check_interval_seconds = 0  # Always check.
        cx_pool.start_request()

        # Get the request socket.
        with cx_pool.get_socket({}, 0, 0) as sock_info:
            self.assertEqual(sock_info, cx_pool._get_request_state())

        # End request.
        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

        # Kill old request socket.
        sock_info.sock.close()

        # Dead socket detected and removed.
        with cx_pool.get_socket({}, 0, 0) as new_sock_info:
            self.assertFalse(cx_pool.in_request())
            self.assertNotEqual(sock_info, new_sock_info)
            self.assertEqual(0, len(cx_pool.sockets))
            self.assertFalse(_closed(new_sock_info.sock))

        self.assertEqual(1, len(cx_pool.sockets))

    def test_dead_request_socket_with_max_size(self):
        # When a pool replaces a dead request socket, the semaphore it uses
        # to enforce max_size should remain unaffected.
        cx_pool = self.create_pool(max_pool_size=1, wait_queue_timeout=1)
        cx_pool._check_interval_seconds = 0  # Always check.
        cx_pool.start_request()

        # Get and close the request socket.
        with cx_pool.get_socket({}, 0, 0) as request_sock_info:
            request_sock_info.sock.close()

        # Detects closed socket and creates new one, semaphore value still 0.
        with cx_pool.get_socket({}, 0, 0) as request_sock_info_2:
            self.assertNotEqual(request_sock_info, request_sock_info_2)

        cx_pool.end_request()

        # Semaphore value now 1; we can get a socket.
        sock = cx_pool.get_socket({}, 0, 0)
        sock.close()

    def test_socket_reclamation(self):
        if sys.platform.startswith('java'):
            raise SkipTest("Jython can't do socket reclamation")

        # Check that if a thread starts a request and dies without ending
        # the request, that the socket is reclaimed into the pool.
        cx_pool = self.create_pool(
            max_pool_size=10,
            connect_timeout=1000,
            socket_timeout=1000)

        self.assertEqual(0, len(cx_pool.sockets))

        lock = None
        the_sock = [None]

        def leak_request():
            self.assertEqual(NO_REQUEST, cx_pool._get_request_state())
            cx_pool.start_request()
            self.assertEqual(NO_SOCKET_YET, cx_pool._get_request_state())
            with cx_pool.get_socket({}, 0, 0) as sock_info:
                self.assertEqual(sock_info, cx_pool._get_request_state())
                the_sock[0] = id(sock_info.sock)

            lock.release()

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

    def test_request_with_fork(self):
        if sys.platform == "win32":
            raise SkipTest("Can't test forking on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        coll = self.c.pymongo_test.test
        coll.remove()
        coll.insert({'_id': 1})
        coll.find_one()
        self.assert_pool_size(1)
        self.c.start_request()
        self.assert_pool_size(1)
        coll.find_one()
        self.assert_pool_size(0)
        self.assert_request_with_socket()

        def f(pipe):
            # We can still query server without error
            self.assertEqual({'_id':1}, coll.find_one())

            # Pool has detected that we forked, but resumed the request
            self.assert_request_with_socket()
            self.assert_pool_size(0)
            pipe.send("success")

        parent_conn, child_conn = Pipe()
        p = Process(target=f, args=(child_conn,))
        p.start()
        p.join(1)
        p.terminate()
        child_conn.close()
        self.assertEqual("success", parent_conn.recv())

    def test_primitive_thread(self):
        p = self.create_pool(max_pool_size=10)

        # Test that start/end_request work with a thread begun from thread
        # module, rather than threading module
        lock = thread.allocate_lock()
        lock.acquire()

        sock_ids = []

        def run_in_request():
            p.start_request()
            sock0 = p.get_socket({}, 0, 0)
            sock1 = p.get_socket({}, 0, 0)
            sock_ids.extend([id(sock0), id(sock1)])
            p.maybe_return_socket(sock0)
            p.maybe_return_socket(sock1)
            p.end_request()
            lock.release()

        thread.start_new_thread(run_in_request, ())

        # Join thread
        acquired = False
        for i in range(30):
            time.sleep(0.5)
            acquired = lock.acquire(0)
            if acquired:
                break

        self.assertTrue(acquired, "Thread is hung")
        self.assertEqual(sock_ids[0], sock_ids[1])

    def test_pool_with_fork(self):
        # Test that separate MongoClients have separate Pools, and that the
        # driver can create a new MongoClient after forking
        if sys.platform == "win32":
            raise SkipTest("Can't test forking on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        a = get_client()
        a.pymongo_test.test.remove()
        a.pymongo_test.test.insert({'_id':1})
        a.pymongo_test.test.find_one()
        self.assertEqual(1, len(get_pool(a).sockets))
        a_sock = one(get_pool(a).sockets)

        def loop(pipe):
            c = get_client()
            c.pymongo_test.test.find_one()
            self.assertEqual(1, len(get_pool(c).sockets))
            pipe.send(one(get_pool(c).sockets).sock.getsockname())

        cp1, cc1 = Pipe()
        cp2, cc2 = Pipe()

        p1 = Process(target=loop, args=(cc1,))
        p2 = Process(target=loop, args=(cc2,))

        p1.start()
        p2.start()

        p1.join(1)
        p2.join(1)

        p1.terminate()
        p2.terminate()

        p1.join()
        p2.join()

        cc1.close()
        cc2.close()

        b_sock = cp1.recv()
        c_sock = cp2.recv()
        self.assertTrue(a_sock.sock.getsockname() != b_sock)
        self.assertTrue(a_sock.sock.getsockname() != c_sock)
        self.assertTrue(b_sock != c_sock)

        # a_sock, created by parent process, is still in the pool
        d_sock = get_pool(a).get_socket({}, 0, 0)
        self.assertEqual(a_sock, d_sock)
        d_sock.close()

    def test_wait_queue_timeout(self):
        wait_queue_timeout = 2  # Seconds
        pool = self.create_pool(
            max_pool_size=1, wait_queue_timeout=wait_queue_timeout)
        
        sock_info = pool.get_socket({}, 0, 0)
        start = time.time()
        with self.assertRaises(ConnectionFailure):
            pool.get_socket({}, 0, 0)

        duration = time.time() - start
        self.assertTrue(
            abs(wait_queue_timeout - duration) < 1,
            "Waited %.2f seconds for a socket, expected %f" % (
                duration, wait_queue_timeout))

        sock_info.close()

    def test_no_wait_queue_timeout(self):
        # Verify get_socket() with no wait_queue_timeout blocks forever.
        pool = self.create_pool(max_pool_size=1)
        
        # Reach max_size.
        with pool.get_socket({}, 0, 0) as s1:
            t = SocketGetter(self.c, pool)
            t.start()
            while t.state != 'get_socket':
                time.sleep(0.1)

            time.sleep(1)
            self.assertEqual(t.state, 'get_socket')

        while t.state != 'sock':
            time.sleep(0.1)

        self.assertEqual(t.state, 'sock')
        self.assertEqual(t.sock, s1)
        s1.close()

    def test_wait_queue_multiple(self):
        wait_queue_multiple = 3
        pool = self.create_pool(
            max_pool_size=2, wait_queue_multiple=wait_queue_multiple)

        # Reach max_size sockets.
        socket_info_0 = pool.get_socket({}, 0, 0)
        socket_info_1 = pool.get_socket({}, 0, 0)

        # Reach max_size * wait_queue_multiple waiters.
        threads = []
        for _ in range(6):
            t = SocketGetter(self.c, pool)
            t.start()
            threads.append(t)

        time.sleep(1)
        for t in threads:
            self.assertEqual(t.state, 'get_socket')

        with self.assertRaises(ExceededMaxWaiters):
            pool.get_socket({}, 0, 0)
        socket_info_0.close()
        socket_info_1.close()

    def test_no_wait_queue_multiple(self):
        pool = self.create_pool(max_pool_size=2)

        socks = []
        for _ in range(2):
            sock = pool.get_socket({}, 0, 0)
            socks.append(sock)
        threads = []
        for _ in range(30):
            t = SocketGetter(self.c, pool)
            t.start()
            threads.append(t)
        time.sleep(1)
        for t in threads:
            self.assertEqual(t.state, 'get_socket')

        for socket_info in socks:
            socket_info.close()


class TestPoolMaxSize(_TestPoolingBase):
    """Keep right number of sockets in various start/end_request sequences. 
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
        """
        if start_request:
            if max_pool_size is not None and max_pool_size < nthreads:
                raise AssertionError("Deadlock")

        c = get_client(max_pool_size=max_pool_size)
        rendezvous = CreateAndReleaseSocket.Rendezvous(nthreads)

        threads = []
        for i in range(nthreads):
            t = CreateAndReleaseSocket(
                c, start_request, end_request, rendezvous)
            
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
        time.sleep(1)

        for t in threads:
            self.assertTrue(t.passed)

        # Socket-reclamation doesn't work in Jython
        if not sys.platform.startswith('java'):
            cx_pool = get_pool(c)

            # Socket-reclamation depends on timely garbage-collection
            if 'PyPy' in sys.version:
                gc.collect()

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
            time.sleep(0.1)
            cx_pool._ident.get()
            start = time.time()

            while (
                not cx_pool.sockets
                and cx_pool._socket_semaphore.counter < max_pool_size
                and (time.time() - start) < 5
            ):
                time.sleep(0.1)
                cx_pool._ident.get()

            if max_pool_size is not None:
                self.assertEqual(
                    max_pool_size,
                    cx_pool._socket_semaphore.counter)

            self.assertEqual(0, len(cx_pool._tid_to_sock))

    def _test_max_pool_size_no_rendezvous(self, start_request, end_request):
        max_pool_size = 5
        c = get_client(max_pool_size=max_pool_size)

        # nthreads had better be much larger than max_pool_size to ensure that
        # max_pool_size sockets are actually required at some point in this
        # test's execution.
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
                c, start_request, end_request)
            
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

        # thread.join completes slightly *before* thread locals are
        # cleaned up, so wait up to 5 seconds for them.
        time.sleep(0.1)
        cx_pool._ident.get()
        start = time.time()

        while (
            not cx_pool.sockets
            and cx_pool._socket_semaphore.counter < max_pool_size
            and (time.time() - start) < 5
        ):
            time.sleep(0.1)
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
            ('example.com', 27017),
            PoolOptions(
                max_pool_size=1,
                connect_timeout=1,
                socket_timeout=1,
                wait_queue_timeout=1))

        # First call to get_socket fails; if pool doesn't release its semaphore
        # then the second call raises "ConnectionFailure: Timed out waiting for
        # socket from pool" instead of the socket.error.
        for i in range(2):
            with self.assertRaises(socket.error):
                test_pool.get_socket({}, 0, 0)


if __name__ == "__main__":
    unittest.main()

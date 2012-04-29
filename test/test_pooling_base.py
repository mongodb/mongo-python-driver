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

"""Base classes to test built-in connection-pooling with threads or greenlets.
"""

import gc
import random
import socket
import sys
import thread
import threading
import time
import bson

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

import pymongo.pool
from pymongo.connection import Connection
from pymongo.pool import (
    Pool, GreenletPool, NO_REQUEST, NO_SOCKET_YET, SocketInfo)
from pymongo.errors import ConfigurationError
from test import version
from test.test_connection import get_connection, host, port
from test.utils import delay

N = 50
DB = "pymongo-pooling-tests"


if sys.version_info[0] >= 3:
    from imp import reload


try:
    import gevent
    from gevent import Greenlet, monkey
    has_gevent = True
except ImportError:
    has_gevent = False


def one(s):
    """Get one element of a set"""
    return iter(s).next()


def force_reclaim_sockets(cx_pool, n_expected):
    # When a thread dies without ending its request, the SocketInfo it was
    # using is deleted, and in its __del__ it returns the socket to the
    # pool. However, when exactly that happens is unpredictable. Try
    # various ways of forcing the issue.

    if sys.platform.startswith('java'):
        raise SkipTest("Jython can't reclaim sockets")

    if 'PyPy' in sys.version:
        raise SkipTest("Socket reclamation happens at unpredictable time in PyPy")

    # Bizarre behavior in CPython 2.4, and possibly other CPython versions
    # less than 2.7: the last dead thread's locals aren't cleaned up until
    # the local attribute with the same name is accessed from a different
    # thread. This assert checks that the thread-local is indeed local, and
    # also triggers the cleanup so the socket is reclaimed.
    if isinstance(cx_pool, Pool):
        assert cx_pool.local.sock_info is None

    # Try for a while to make garbage-collection call SocketInfo.__del__
    start = time.time()
    while len(cx_pool.sockets) < n_expected and time.time() - start < 5:
        try:
            gc.collect(2)
        except TypeError:
            # collect() didn't support 'generation' arg until 2.5
            gc.collect()

        time.sleep(0.5)


class MongoThread(object):
    """A thread, or a greenlet, that uses a Connection"""
    def __init__(self, test_case):
        self.use_greenlets = test_case.use_greenlets
        self.connection = test_case.c
        self.db = self.connection[DB]
        self.ut = test_case
        self.passed = False

    def start(self):
        if self.use_greenlets:
            # A Gevent extended Greenlet
            self.thread = Greenlet(self.run)
        else:
            self.thread = threading.Thread(target=self.run)
            self.thread.setDaemon(True) # Don't hang whole test if thread hangs


        self.thread.start()

    def join(self):
        self.thread.join(300)
        if self.use_greenlets:
            assert self.thread.dead, "Greenlet timeout"
        else:
            assert not self.thread.isAlive(), "Thread timeout"

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
            _id = self.db.sf.save({"x": rand}, safe=True)
            self.ut.assertEqual(rand, self.db.sf.find_one(_id)["x"])


class Unique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.start_request()
            self.db.unique.insert({})
            self.ut.assertEqual(None, self.db.error())
            self.connection.end_request()


class NonUnique(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.start_request()
            self.db.unique.insert({"_id": "jesse"})
            self.ut.assertNotEqual(None, self.db.error())
            self.connection.end_request()


class Disconnect(MongoThread):

    def run_mongo_thread(self):
        for _ in xrange(N):
            self.connection.disconnect()


class NoRequest(MongoThread):

    def run_mongo_thread(self):
        self.connection.start_request()
        errors = 0
        for _ in xrange(N):
            self.db.unique.insert({"_id": "jesse"})
            if not self.db.error():
                errors += 1

        self.connection.end_request()
        self.ut.assertEqual(0, errors)


def run_cases(ut, cases):
    threads = []
    nruns = 10
    if (
        ut.use_greenlets and sys.platform == 'darwin'
        and gevent.version_info[0] < 1
    ):
        # Gevent 0.13.6 bug on Mac, Greenlet.join() hangs if more than
        # about 35 Greenlets share a Connection. Apparently fixed in
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
        pool = self.connection._Connection__pool
        assert len(pool.sockets) == 1, "Expected 1 socket, found %d" % (
            len(pool.sockets)
        )

        sock_info = one(pool.sockets)

        self.connection.start_request()

        # start_request() hasn't yet moved the socket from the general pool into
        # the request
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock_info

        self.connection[DB].test.find_one()

        # find_one() causes the socket to be used in the request, so now it's
        # bound to this thread
        assert len(pool.sockets) == 0
        assert pool._get_request_state() == sock_info
        self.connection.end_request()

        # The socket is back in the pool
        assert len(pool.sockets) == 1
        assert one(pool.sockets) == sock_info


class CreateAndReleaseSocket(MongoThread):

    def __init__(self, ut, connection, start_request, end_request):
        super(CreateAndReleaseSocket, self).__init__(ut)
        self.connection = connection
        self.start_request = start_request
        self.end_request = end_request

    def run_mongo_thread(self):
        # Do an operation that requires a socket.
        # test_max_pool_size uses this to spin up lots of threads requiring
        # lots of simultaneous connections, to ensure that Pool obeys its
        # max_size configuration and closes extra sockets as they're returned.
        # We need a delay here to ensure that more than max_size sockets are
        # needed at once.
        for i in range(self.start_request):
            self.connection.start_request()

        self.connection[DB].test.find_one({'$where': delay(0.1)})
        for i in range(self.end_request):
            self.connection.end_request()


class _TestPoolingBase(object):
    """Base class for all connection-pool tests. Doesn't inherit from
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
            # the connection pool to work properly.
            monkey.patch_socket()

        self.c = self.get_connection(auto_start_request=False)

        # reset the db
        db = self.c[DB]
        db.unique.drop()
        db.test.drop()
        db.unique.insert({"_id": "jesse"})

        db.test.insert([{} for i in range(10)])

    def tearDown(self):
        self.c.close()
        if self.use_greenlets:
            # Undo patch
            reload(socket)

    def get_connection(self, *args, **kwargs):
        opts = kwargs.copy()
        opts['use_greenlets'] = self.use_greenlets
        return get_connection(*args, **opts)

    def get_pool(self, *args, **kwargs):
        if self.use_greenlets:
            klass = GreenletPool
        else:
            klass = Pool

        return klass(*args, **kwargs)

    def assert_no_request(self):
        self.assertEqual(
            NO_REQUEST, self.c._Connection__pool._get_request_state()
        )

    def assert_request_without_socket(self):
        self.assertEqual(
            NO_SOCKET_YET, self.c._Connection__pool._get_request_state()
        )

    def assert_request_with_socket(self):
        self.assertTrue(isinstance(
            self.c._Connection__pool._get_request_state(), SocketInfo
        ))

    def assert_pool_size(self, pool_size):
        self.assertEqual(
            pool_size, len(self.c._Connection__pool.sockets)
        )


class _TestPooling(_TestPoolingBase):
    """Basic pool tests, to be applied both to Pool and GreenletPool"""
    def test_max_pool_size_validation(self):
        self.assertRaises(
            ConfigurationError, Connection, host=host, port=port,
            max_pool_size=-1
        )

        self.assertRaises(
            ConfigurationError, Connection, host=host, port=port,
            max_pool_size='foo'
        )

        c = Connection(host=host, port=port, max_pool_size=100)
        self.assertEqual(c.max_pool_size, 100)

    def test_no_disconnect(self):
        run_cases(self, [NoRequest, NonUnique, Unique, SaveAndFind])

    def test_simple_disconnect(self):
        # Connection just created, expect 1 free socket
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
        p = self.get_pool((host, port), 10, None, None, False)
        self.c.start_request()
        self.assertEqual(set(), p.sockets)
        self.c.end_request()
        self.assertEqual(set(), p.sockets)

    def test_dependent_pools(self):
        self.assert_pool_size(1)
        self.c.start_request()
        self.assert_request_without_socket()
        self.c.test.test.find_one()
        self.assert_request_with_socket()
        self.assert_pool_size(0)
        self.c.end_request()
        self.assert_pool_size(1)

        t = OneOp(self)
        t.start()
        t.join()
        self.assertTrue(t.passed, "OneOp.run() threw exception")

        self.assert_pool_size(1)
        self.c.test.test.find_one()
        self.assert_pool_size(1)

    def test_multiple_connections(self):
        a = self.get_connection(auto_start_request=False)
        b = self.get_connection(auto_start_request=False)
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        a.start_request()
        a.test.test.find_one()
        self.assertEqual(0, len(a._Connection__pool.sockets))
        a.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))
        a_sock = one(a._Connection__pool.sockets)

        b.end_request()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(1, len(b._Connection__pool.sockets))

        b.start_request()
        b.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        self.assertEqual(0, len(b._Connection__pool.sockets))

        b.end_request()
        b_sock = one(b._Connection__pool.sockets)
        b.test.test.find_one()
        a.test.test.find_one()

        self.assertEqual(b_sock,
                         b._Connection__pool.get_socket((b.host, b.port)))
        self.assertEqual(a_sock,
                         a._Connection__pool.get_socket((a.host, a.port)))

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
        sock_info = cx_pool.get_socket()
        cx_pool.return_socket(sock_info)

        # trigger _check_closed, which only runs on sockets that haven't been
        # used in a second
        time.sleep(1.1)
        new_sock_info = cx_pool.get_socket()
        self.assertEqual(sock_info, new_sock_info)
        del sock_info, new_sock_info

        # Assert sock_info was returned to the pool *once*
        force_reclaim_sockets(cx_pool, 1)
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_socket(self):
        # Test that Pool removes dead socket and the socket doesn't return
        # itself PYTHON-344
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        sock_info = cx_pool.get_socket()

        # Simulate a closed socket without telling the SocketInfo it's closed
        sock_info.sock.close()
        self.assertTrue(pymongo.pool._closed(sock_info.sock))
        cx_pool.return_socket(sock_info)
        time.sleep(1.1) # trigger _check_closed
        new_sock_info = cx_pool.get_socket()
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertNotEqual(sock_info, new_sock_info)
        del sock_info, new_sock_info

        # new_sock_info returned to the pool, but not the closed sock_info
        force_reclaim_sockets(cx_pool, 1)
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_request_socket(self):
        # Test that Pool keeps request going even if a socket dies in request
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool.start_request()

        # Get the request socket
        sock_info = cx_pool.get_socket()
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertEqual(sock_info, cx_pool._get_request_state())
        sock_info.sock.close()
        cx_pool.return_socket(sock_info)
        time.sleep(1.1) # trigger _check_closed

        # Although the request socket died, we're still in a request with a
        # new socket
        new_sock_info = cx_pool.get_socket()
        self.assertNotEqual(sock_info, new_sock_info)
        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        cx_pool.return_socket(new_sock_info)
        self.assertEqual(new_sock_info, cx_pool._get_request_state())
        self.assertEqual(0, len(cx_pool.sockets))

        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

    def test_pool_removes_dead_socket_after_request(self):
        # Test that Pool handles a socket dying that *used* to be the request
        # socket.
        cx_pool = self.get_pool((host,port), 10, None, None, False)
        cx_pool.start_request()

        # Get the request socket
        sock_info = cx_pool.get_socket()
        self.assertEqual(sock_info, cx_pool._get_request_state())

        # End request
        cx_pool.end_request()
        self.assertEqual(1, len(cx_pool.sockets))

        # Kill old request socket
        sock_info.sock.close()
        old_sock_info_id = id(sock_info)
        del sock_info
        time.sleep(1.1) # trigger _check_closed

        # Dead socket detected and removed
        new_sock_info = cx_pool.get_socket()
        self.assertNotEqual(id(new_sock_info), old_sock_info_id)
        self.assertEqual(0, len(cx_pool.sockets))
        self.assertFalse(pymongo.pool._closed(new_sock_info.sock))

    def test_socket_reclamation(self):
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

            if not self.use_greenlets:
                lock.release()

        if self.use_greenlets:
            g = Greenlet(leak_request)
            g.start()
            g.join(1)
            self.assertTrue(g.ready(), "Greenlet is hung")
        else:
            lock = thread.allocate_lock()
            lock.acquire()

            # Start a thread WITHOUT a threading.Thread - important to test that
            # Pool can deal with primitive threads.
            thread.start_new_thread(leak_request, ())

            # Join thread
            acquired = lock.acquire()
            self.assertTrue(acquired, "Thread is hung")

        force_reclaim_sockets(cx_pool, 1)

        # Pool reclaimed the socket
        self.assertEqual(1, len(cx_pool.sockets))
        self.assertEqual(the_sock[0], id(one(cx_pool.sockets).sock))


class _TestMaxPoolSize(_TestPoolingBase):
    """Test that connection pool keeps proper number of idle sockets open,
    no matter how start/end_request are called. To be applied both to Pool and
    GreenletPool.
    """
    def _test_max_pool_size(self, start_request, end_request):
        c = self.get_connection(max_pool_size=4, auto_start_request=False)
        nthreads = 10

        if (
            self.use_greenlets and sys.platform == 'darwin'
            and gevent.version_info[0] < 1
        ):
            # Gevent 0.13.6 bug on Mac, Greenlet.join() hangs if more than
            # about 35 Greenlets share a Connection. Apparently fixed in
            # recent Gevent development.
            nthreads = 30

        threads = []
        for i in range(nthreads):
            t = CreateAndReleaseSocket(self, c, start_request, end_request)
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        for t in threads:
            self.assertTrue(t.passed)

        # Critical: release refs to threads, so SocketInfo.__del__() executes
        # and reclaims sockets.
        del threads
        t = None

        cx_pool = c._Connection__pool
        force_reclaim_sockets(cx_pool, 4)

        nsock = len(cx_pool.sockets)

        # Socket-reclamation depends on timely garbage-collection, so be lenient
        self.assertTrue(2 <= nsock <= 4,
            msg="Expected between 2 and 4 sockets in the pool, got %d" % nsock)

    def test_max_pool_size(self):
        self._test_max_pool_size(0, 0)

    def test_max_pool_size_with_request(self):
        self._test_max_pool_size(1, 1)

    def test_max_pool_size_with_redundant_request(self):
        self._test_max_pool_size(2, 1)
        self._test_max_pool_size(20, 1)

    def test_max_pool_size_with_leaked_request(self):
        # Call start_request() but not end_request() -- when threads die, they
        # should return their request sockets to the pool.
        self._test_max_pool_size(1, 0)

    def test_max_pool_size_with_end_request_only(self):
        # Call end_request() but not start_request()
        self._test_max_pool_size(0, 1)


class _TestPoolSocketSharing(_TestPoolingBase):
    """Directly test that two simultaneous operations don't share a socket. To
    be applied both to Pool and GreenletPool.
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
        cx = get_connection(
            use_greenlets=self.use_greenlets,
            auto_start_request=False
        )

        db = cx.pymongo_test
        if not version.at_least(db.connection, (1, 7, 2)):
            raise SkipTest("Need at least MongoDB version 1.7.2 to use"
                           " db.eval(nolock=True)")
        
        db.test.remove(safe=True)
        db.test.insert({'_id': 1}, safe=True)

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

            # Javascript function that pauses 5 sec. 'nolock' allows find_fast
            # to start and finish while we're waiting for this.
            fn = delay(5)
            self.assertEqual(
                {'ok': 1.0, 'retval': True},
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

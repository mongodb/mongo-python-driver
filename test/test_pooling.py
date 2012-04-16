# Copyright 2009-2010 10gen, Inc.
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

import sys
import thread
import time
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from test.test_connection import host, port
from test.test_pooling_base import (
    _TestPooling, _TestMaxPoolSize, _TestPoolSocketSharing, one)


class TestPoolingThreads(_TestPooling, unittest.TestCase):
    use_greenlets = False

    def test_request_with_fork(self):
        if sys.platform == "win32":
            raise SkipTest("Can't test forking on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        coll = self.c.test.test
        coll.remove(safe=True)
        coll.insert({'_id': 1}, safe=True)
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
        p = self.get_pool((host, port), 10, None, None, False)

        # Test that start/end_request work with a thread begun from thread
        # module, rather than threading module
        lock = thread.allocate_lock()
        lock.acquire()

        def run_in_request():
            p.start_request()
            p.get_socket()
            p.end_request()
            lock.release()

        thread.start_new_thread(run_in_request, ())

        # Join thread
        acquired = False
        for i in range(10):
            time.sleep(0.1)
            acquired = lock.acquire(0)
            if acquired:
                break

        self.assertTrue(acquired, "Thread is hung")

    def test_pool_with_fork(self):
        # Test that separate Connections have separate Pools, and that the
        # driver can create a new Connection after forking
        if sys.platform == "win32":
            raise SkipTest("Can't test forking on Windows")

        try:
            from multiprocessing import Process, Pipe
        except ImportError:
            raise SkipTest("No multiprocessing module")

        a = self.get_connection(auto_start_request=False)
        a.test.test.remove(safe=True)
        a.test.test.insert({'_id':1}, safe=True)
        a.test.test.find_one()
        self.assertEqual(1, len(a._Connection__pool.sockets))
        a_sock = one(a._Connection__pool.sockets)

        def loop(pipe):
            c = self.get_connection(auto_start_request=False)
            self.assertEqual(1,len(c._Connection__pool.sockets))
            c.test.test.find_one()
            self.assertEqual(1,len(c._Connection__pool.sockets))
            pipe.send(one(c._Connection__pool.sockets).sock.getsockname())

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
        self.assertEqual(a_sock,
                         a._Connection__pool.get_socket((a.host, a.port)))


class TestMaxPoolSizeThreads(_TestMaxPoolSize, unittest.TestCase):
    use_greenlets = False


class TestPoolSocketSharingThreads(_TestPoolSocketSharing, unittest.TestCase):
    use_greenlets = False


if __name__ == "__main__":
    unittest.main()

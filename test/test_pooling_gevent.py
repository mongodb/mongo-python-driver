# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Tests for connection-pooling with greenlets and Gevent"""

import unittest

from nose.plugins.skip import SkipTest

from pymongo import pool
from test import host, port
from test.utils import looplet
from test.test_pooling_base import (
    _TestPooling, _TestMaxPoolSize, _TestMaxOpenSockets,
    _TestPoolSocketSharing, _TestWaitQueueMultiple)


class TestPoolingGevent(_TestPooling, unittest.TestCase):
    """Apply all the standard pool tests with greenlets and Gevent"""
    use_greenlets = True


class TestPoolingGeventSpecial(unittest.TestCase):
    """Do a few special greenlet tests that don't use TestPoolingBase"""
    def test_greenlet_sockets(self):
        # Check that Pool gives two sockets to two greenlets
        try:
            import greenlet
        except ImportError:
            raise SkipTest('greenlet not installed')

        cx_pool = pool.Pool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False,
            use_greenlets=True)

        socks = []

        def get_socket():
            cx_pool.start_request()
            socks.append(cx_pool.get_socket())

        looplet([
            greenlet.greenlet(get_socket),
            greenlet.greenlet(get_socket),
        ])

        self.assertEqual(2, len(socks))
        self.assertNotEqual(socks[0], socks[1])

    def test_greenlet_sockets_with_request(self):
        # Verify two assumptions: that start_request() with two greenlets but
        # not use_greenlets fails, meaning that the two greenlets will
        # share one socket. Also check that start_request() with use_greenlets
        # succeeds, meaning that two greenlets will get different sockets.

        try:
            import greenlet
        except ImportError:
            raise SkipTest('greenlet not installed')

        pool_args = dict(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False,
        )

        for use_greenlets, use_request, expect_success in [
            (True, True, True),
            (True, False, False),
            (False, True, False),
            (False, False, False),
        ]:
            pool_args_cp = pool_args.copy()
            pool_args_cp['use_greenlets'] = use_greenlets
            cx_pool = pool.Pool(**pool_args_cp)

            # Map: greenlet -> socket
            greenlet2socks = {}
            main = greenlet.getcurrent()

            def get_socket_in_request():
                # Get a socket from the pool twice, switching contexts each time
                if use_request:
                    cx_pool.start_request()

                main.switch()

                for _ in range(2):
                    sock = cx_pool.get_socket()
                    cx_pool.maybe_return_socket(sock)
                    greenlet2socks.setdefault(
                        greenlet.getcurrent(), []
                    ).append(id(sock))

                    main.switch()

                cx_pool.end_request()

            greenlets = [
                greenlet.greenlet(get_socket_in_request),
                greenlet.greenlet(get_socket_in_request),
            ]

            # Run both greenlets to completion
            looplet(greenlets)

            socks_for_gr0 = greenlet2socks[greenlets[0]]
            socks_for_gr1 = greenlet2socks[greenlets[1]]

            # Whether we expect requests to work or not, we definitely expect
            # greenlet2socks to have the same number of keys and values
            self.assertEqual(2, len(greenlet2socks))
            self.assertEqual(2, len(socks_for_gr0))
            self.assertEqual(2, len(socks_for_gr1))

            # If we started a request, then there was a point at which we had
            # 2 active sockets, otherwise we always used one.
            if use_request and use_greenlets:
                self.assertEqual(2, len(cx_pool.sockets))
            else:
                self.assertEqual(1, len(cx_pool.sockets))

            # Again, regardless of whether requests work, a greenlet will get
            # the same socket each time it calls get_socket() within a request.
            # What we're really testing is that the two *different* greenlets
            # get *different* sockets from each other.
            self.assertEqual(
                socks_for_gr0[0], socks_for_gr0[1],
                "Expected greenlet 0 to get the same socket for each call "
                "to get_socket()"
            )

            self.assertEqual(
                socks_for_gr1[0], socks_for_gr1[1],
                "Expected greenlet 1 to get the same socket for each call "
                "to get_socket()"
            )

            if expect_success:
                # We passed use_greenlets=True, so start_request successfully
                # distinguished between the two greenlets.
                self.assertNotEqual(
                    socks_for_gr0[0], socks_for_gr1[0],
                    "Expected two greenlets to get two different sockets"
                )

            else:
                # We passed use_greenlets=False, so start_request didn't
                # distinguish between the two greenlets, and it gave them both
                # the same socket.
                self.assertEqual(
                    socks_for_gr0[0], socks_for_gr1[0],
                    "Expected two greenlets to get same socket"
                )


class TestMaxPoolSizeGevent(_TestMaxPoolSize, unittest.TestCase):
    use_greenlets = True


class TestPoolSocketSharingGevent(_TestPoolSocketSharing, unittest.TestCase):
    use_greenlets = True


class TestMaxOpenSocketsGevent(_TestMaxOpenSockets, unittest.TestCase):
    use_greenlets = True


class TestWaitQueueMultipleGevent(_TestWaitQueueMultiple, unittest.TestCase):
    use_greenlets = True


if __name__ == '__main__':
    unittest.main()

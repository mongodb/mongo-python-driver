import os
import threading
import unittest

from nose.plugins.skip import SkipTest

from pymongo import pool
from test_connection import get_connection
from test.utils import delay

host = os.environ.get("DB_IP", "localhost")
port = int(os.environ.get("DB_PORT", 27017))

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


class GeventTest(unittest.TestCase):
    def _test_pool(self, use_greenlets, use_request):
        """
        Test that the connection pool prevents both threads and greenlets from
        using a socket at the same time.

        Sequence:
        gr0: start a slow find()
        gr1: start a fast find()
        gr1: get results
        gr0: get results
        """
        NOT_STARTED = 0
        SUCCESS = 1
        SKIP = 2

        try:
            from multiprocessing import Value, Process
        except ImportError:
            # Python < 2.6
            raise SkipTest('No multiprocessing module')
        
        outcome = Value('i', NOT_STARTED)

        results = {
            'find_fast_result': None,
            'find_slow_result': None,
        }

        # Do test in separate process so patch_socket() doesn't affect all
        # subsequent unittests
        def do_test():
            if use_greenlets:
                try:
                    from gevent import Greenlet
                    from gevent import monkey

                    # Note we don't do patch_thread() or patch_all() - we're
                    # testing here that patch_thread() is unnecessary for
                    # the connection pool to work properly.
                    monkey.patch_socket()
                except ImportError:
                    outcome.value = SKIP
                    return
    
            cx = get_connection(
                use_greenlets=use_greenlets,
                auto_start_request=False
            )

            db = cx.pymongo_test
            db.test.remove(safe=True)
            db.test.insert({'_id': 1})

            history = []

            def find_fast():
                if use_request:
                    cx.start_request()

                history.append('find_fast start')

                # With the old connection._Pool, this would throw
                # AssertionError: "This event is already used by another
                # greenlet"
                results['find_fast_result'] = list(db.test.find())
                history.append('find_fast done')

                if use_request:
                    cx.end_request()

            def find_slow():
                if use_request:
                    cx.start_request()

                history.append('find_slow start')

                # Javascript function that pauses for half a second
                where = delay(0.5)
                results['find_slow_result'] = list(db.test.find(
                    {'$where': where}
                ))

                history.append('find_slow done')

                if use_request:
                    cx.end_request()

            if use_greenlets:
                gr0, gr1 = Greenlet(find_slow), Greenlet(find_fast)
                gr0.start()
                gr1.start_later(.1)
            else:
                gr0 = threading.Thread(target=find_slow)
                gr1 = threading.Thread(target=find_fast)
                gr0.start()
                gr1.start()

            gr0.join()
            gr1.join()

            self.assertEqual([{'_id': 1}], results['find_slow_result'])

            # Fails, since find_fast doesn't complete
            self.assertEqual([{'_id': 1}], results['find_fast_result'])

            self.assertEqual([
                'find_slow start',
                'find_fast start',
                'find_fast done',
                'find_slow done',
            ], history)

            outcome.value = SUCCESS

        proc = Process(target=do_test)
        proc.start()
        proc.join()

        if outcome.value == SKIP:
            raise SkipTest('gevent not installed')

        self.assertEqual(
            SUCCESS,
            outcome.value,
            'test failed'
        )

    def test_threads_pool(self):
        # Test the same sequence of calls as the gevent tests to ensure my test
        # is ok.
        self._test_pool(use_greenlets=False, use_request=False)

    def test_threads_pool_request(self):
        # Test the same sequence of calls as the gevent tests to ensure my test
        # is ok.
        self._test_pool(use_greenlets=False, use_request=True)

    def test_greenlets_pool(self):
        self._test_pool(use_greenlets=True, use_request=False)

    def test_greenlets_pool_request(self):
        self._test_pool(use_greenlets=True, use_request=True)

    def test_greenlet_sockets(self):
        # Check that Pool gives two sockets to two greenlets
        try:
            import greenlet
        except ImportError:
            raise SkipTest('greenlet not installed')

        cx_pool = pool.GreenletPool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False
        )

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
        # Verify two assumptions: that start_request() with two greenlets and
        # the regular pool will fail, meaning that the two greenlets will
        # share one socket. Also check that start_request() with GreenletPool
        # succeeds, meaning that two greenlets will get different sockets (this
        # is exactly the reason for creating GreenletPool).

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

        for pool_class, use_request, expect_success in [
            (pool.GreenletPool, True, True),
            (pool.GreenletPool, False, False),
            (pool.Pool, True, False),
            (pool.Pool, False, False),
        ]:
            cx_pool = pool_class(**pool_args)

            # Map: greenlet -> socket
            greenlet2socks = {}
            main = greenlet.getcurrent()

            def get_socket_in_request():
                # Get a socket from the pool twice, switching contexts each time
                if use_request:
                    cx_pool.start_request()

                main.switch()

                for i in range(2):
                    sock = cx_pool.get_socket()
                    cx_pool.return_socket(sock)
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
            if use_request and pool_class is pool.GreenletPool:
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
                # We used the proper pool class, so start_request successfully
                # distinguished between the two greenlets.
                self.assertNotEqual(
                    socks_for_gr0[0], socks_for_gr1[0],
                    "Expected two greenlets to get two different sockets"
                )

            else:
                # We used the wrong pool class, so start_request didn't
                # distinguish between the two greenlets, and it gave them both
                # the same socket.
                self.assertEqual(
                    socks_for_gr0[0], socks_for_gr1[0],
                    "Expected two greenlets to get same socket"
                )

    def test_socket_reclamation(self):
        try:
            import greenlet
        except ImportError:
            raise SkipTest('greenlet not installed')

        # Check that if a greenlet starts a request and dies without ending
        # the request, that the socket is reclaimed
        cx_pool = pool.GreenletPool(
            pair=(host,port),
            max_size=10,
            net_timeout=1000,
            conn_timeout=1000,
            use_ssl=False,
        )

        self.assertEqual(0, len(cx_pool.sockets))

        the_sock = [None]

        def leak_request():
            cx_pool.start_request()
            sock_info = cx_pool.get_socket()
            the_sock[0] = id(sock_info.sock)

        # Run the greenlet to completion
        gr = greenlet.greenlet(leak_request)
        gr.switch()

        # Cause greenlet to be garbage-collected
        del gr

        # Pool reclaimed the socket
        self.assertEqual(1, len(cx_pool.sockets))
        self.assertEqual(the_sock[0], id(next(iter(cx_pool.sockets)).sock))


if __name__ == '__main__':
    unittest.main()

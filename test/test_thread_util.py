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

"""Test the thread_util module."""

import gc
import sys
import threading
import time
import unittest

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo import thread_util
if thread_util.have_gevent:
    import greenlet         # Plain greenlets.
    import gevent.greenlet  # Gevent's enhanced Greenlets.
    import gevent.hub

from test.utils import looplet, my_partial, RendezvousThread


class TestIdent(unittest.TestCase):
    """Ensure thread_util.Ident works for threads and greenlets. This has
    gotten intricate from refactoring: we have classes, Watched and Unwatched,
    that implement the logic for the two child threads / greenlets. For the
    greenlet case it's easy to ensure the two children are alive at once, so
    we run the Watched and Unwatched logic directly. For the thread case we
    mix in the RendezvousThread class so we're sure both children are alive
    when they call Ident.get().

    1. Store main thread's / greenlet's id
    2. Start 2 child threads / greenlets
    3. Store their values for Ident.get()
    4. Children reach rendezvous point
    5. Children call Ident.watch()
    6. One of the children calls Ident.unwatch()
    7. Children terminate
    8. Assert that children got different ids from each other and from main,
      and assert watched child's callback was executed, and that unwatched
      child's callback was not
    """
    def _test_ident(self, use_greenlets):
        if 'java' in sys.platform:
            raise SkipTest("Can't rely on weakref callbacks in Jython")

        ident = thread_util.create_ident(use_greenlets)

        ids = set([ident.get()])
        unwatched_id = []
        done = set([ident.get()])  # Start with main thread's / greenlet's id.
        died = set()

        class Watched(object):
            def __init__(self, ident):
                self._my_ident = ident

            def before_rendezvous(self):
                self.my_id = self._my_ident.get()
                ids.add(self.my_id)

            def after_rendezvous(self):
                assert not self._my_ident.watching()
                self._my_ident.watch(lambda ref: died.add(self.my_id))
                assert self._my_ident.watching()
                done.add(self.my_id)

        class Unwatched(Watched):
            def before_rendezvous(self):
                Watched.before_rendezvous(self)
                unwatched_id.append(self.my_id)

            def after_rendezvous(self):
                Watched.after_rendezvous(self)
                self._my_ident.unwatch(self.my_id)
                assert not self._my_ident.watching()

        if use_greenlets:
            class WatchedGreenlet(Watched):
                def run(self):
                    self.before_rendezvous()
                    self.after_rendezvous()

            class UnwatchedGreenlet(Unwatched):
                def run(self):
                    self.before_rendezvous()
                    self.after_rendezvous()

            t_watched = greenlet.greenlet(WatchedGreenlet(ident).run)
            t_unwatched = greenlet.greenlet(UnwatchedGreenlet(ident).run)
            looplet([t_watched, t_unwatched])
        else:
            class WatchedThread(Watched, RendezvousThread):
                def __init__(self, ident, state):
                    Watched.__init__(self, ident)
                    RendezvousThread.__init__(self, state)

            class UnwatchedThread(Unwatched, RendezvousThread):
                def __init__(self, ident, state):
                    Unwatched.__init__(self, ident)
                    RendezvousThread.__init__(self, state)

            state = RendezvousThread.create_shared_state(2)
            t_watched = WatchedThread(ident, state)
            t_watched.start()

            t_unwatched = UnwatchedThread(ident, state)
            t_unwatched.start()

            RendezvousThread.wait_for_rendezvous(state)
            RendezvousThread.resume_after_rendezvous(state)

            t_watched.join()
            t_unwatched.join()

            self.assertTrue(t_watched.passed)
            self.assertTrue(t_unwatched.passed)

        # Remove references, let weakref callbacks run
        del t_watched
        del t_unwatched

        # Trigger final cleanup in Python <= 2.7.0.
        # http://bugs.python.org/issue1868
        ident.get()
        self.assertEqual(3, len(ids))
        self.assertEqual(3, len(done))

        # Make sure thread is really gone
        slept = 0
        while not died and slept < 10:
            time.sleep(1)
            gc.collect()
            slept += 1

        self.assertEqual(1, len(died))
        self.assertFalse(unwatched_id[0] in died)

    def test_thread_ident(self):
        self._test_ident(False)

    def test_greenlet_ident(self):
        if not thread_util.have_gevent:
            raise SkipTest('greenlet not installed')

        self._test_ident(True)


class TestGreenletIdent(unittest.TestCase):
    def setUp(self):
        if not thread_util.have_gevent:
            raise SkipTest("need Gevent")

    def test_unwatch_cleans_up(self):
        # GreenletIdent.unwatch() should remove the on_thread_died callback
        # from an enhanced Gevent Greenlet's list of links.
        callback_ran = [False]

        def on_greenlet_died(_):
            callback_ran[0] = True

        ident = thread_util.create_ident(use_greenlets=True)

        def watch_and_unwatch():
            ident.watch(on_greenlet_died)
            ident.unwatch(ident.get())

        g = gevent.greenlet.Greenlet(run=watch_and_unwatch)
        g.start()
        g.join(10)
        the_hub = gevent.hub.get_hub()
        if hasattr(the_hub, 'join'):
            # Gevent 1.0
            the_hub.join()
        else:
            # Gevent 0.13 and less
            the_hub.shutdown()

        self.assertTrue(g.successful())

        # unwatch() canceled the callback.
        self.assertFalse(callback_ran[0])


class TestCounter(unittest.TestCase):
    def _test_counter(self, use_greenlets):
        counter = thread_util.Counter(use_greenlets)

        self.assertEqual(0, counter.dec())
        self.assertEqual(0, counter.get())
        self.assertEqual(0, counter.dec())
        self.assertEqual(0, counter.get())

        done = set()

        def f(n):
            for i in xrange(n):
                self.assertEqual(i, counter.get())
                self.assertEqual(i + 1, counter.inc())

            for i in xrange(n, 0, -1):
                self.assertEqual(i, counter.get())
                self.assertEqual(i - 1, counter.dec())

            self.assertEqual(0, counter.get())

            # Extra decrements have no effect
            self.assertEqual(0, counter.dec())
            self.assertEqual(0, counter.get())
            self.assertEqual(0, counter.dec())
            self.assertEqual(0, counter.get())

            done.add(n)

        if use_greenlets:
            greenlets = [
                greenlet.greenlet(my_partial(f, i)) for i in xrange(10)]
            looplet(greenlets)
        else:
            threads = [
                threading.Thread(target=my_partial(f, i)) for i in xrange(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(10, len(done))

    def test_thread_counter(self):
        self._test_counter(False)

    def test_greenlet_counter(self):
        if not thread_util.have_gevent:
            raise SkipTest('greenlet not installed')

        self._test_counter(True)

if __name__ == "__main__":
    unittest.main()

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

sys.path[0:0] = [""]

from pymongo import thread_util
from test import SkipTest, unittest
from test.utils import my_partial, RendezvousThread


class TestIdent(unittest.TestCase):
    def test_thread_ident(self):
        # 1. Store main thread's id.
        # 2. Start 2 child threads.
        # 3. Store their values for Ident.get().
        # 4. Children reach rendezvous point.
        # 5. Children call Ident.watch().
        # 6. One of the children calls Ident.unwatch().
        # 7. Children terminate.
        # 8. Assert that children got different ids from each other and from
        #    main, and assert watched child's callback was executed, and that
        #    unwatched child's callback was not.

        if 'java' in sys.platform:
            raise SkipTest("Can't rely on weakref callbacks in Jython")

        ident = thread_util.ThreadIdent()

        ids = set([ident.get()])
        unwatched_id = []
        done = set([ident.get()])  # Start with main thread's id.
        died = set()

        class WatchedThread(RendezvousThread):
            def __init__(self, ident, state):
                super(WatchedThread, self).__init__(state)
                self._my_ident = ident

            def before_rendezvous(self):
                self.my_id = self._my_ident.get()
                ids.add(self.my_id)

            def after_rendezvous(self):
                assert not self._my_ident.watching()
                self._my_ident.watch(lambda ref: died.add(self.my_id))
                assert self._my_ident.watching()
                done.add(self.my_id)

        class UnwatchedThread(WatchedThread):
            def before_rendezvous(self):
                super(UnwatchedThread, self).before_rendezvous()
                unwatched_id.append(self.my_id)

            def after_rendezvous(self):
                super(UnwatchedThread, self).after_rendezvous()
                self._my_ident.unwatch(self.my_id)
                assert not self._my_ident.watching()

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


class TestCounter(unittest.TestCase):
    def test_counter(self):
        counter = thread_util.Counter()

        self.assertEqual(0, counter.dec())
        self.assertEqual(0, counter.get())
        self.assertEqual(0, counter.dec())
        self.assertEqual(0, counter.get())

        done = set()

        def f(n):
            for i in range(n):
                self.assertEqual(i, counter.get())
                self.assertEqual(i + 1, counter.inc())

            for i in range(n, 0, -1):
                self.assertEqual(i, counter.get())
                self.assertEqual(i - 1, counter.dec())

            self.assertEqual(0, counter.get())

            # Extra decrements have no effect
            self.assertEqual(0, counter.dec())
            self.assertEqual(0, counter.get())
            self.assertEqual(0, counter.dec())
            self.assertEqual(0, counter.get())

            done.add(n)

        threads = [threading.Thread(target=my_partial(f, i))
                   for i in range(10)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(10, len(done))


if __name__ == "__main__":
    unittest.main()

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

"""Test the thread_util module."""

import gc
import sys
import threading
import time
import unittest
from functools import partial

sys.path[0:0] = [""]

from nose.plugins.skip import SkipTest

from pymongo import thread_util
if thread_util.have_greenlet:
    import greenlet

from test.utils import looplet


class TestIdent(unittest.TestCase):
    def _test_ident(self, use_greenlets):
        ident = thread_util.create_ident(use_greenlets)

        ids = set([ident.get()])
        unwatched_id = []
        done = set([ident.get()]) # Start with main thread's id
        died = set()

        def watched_thread():
            my_id = ident.get()
            time.sleep(.1) # Ensure other thread starts so we don't recycle ids
            ids.add(my_id)
            self.assertFalse(ident.watching())

            def on_died(ref):
                died.add(my_id)

            ident.watch(on_died)
            self.assertTrue(ident.watching())
            done.add(my_id)

        def unwatched_thread():
            my_id = ident.get()
            time.sleep(.1) # Ensure other thread starts so we don't recycle ids
            unwatched_id.append(my_id)
            ids.add(my_id)
            self.assertFalse(ident.watching())

            def on_died(ref):
                died.add(my_id)

            ident.watch(on_died)
            self.assertTrue(ident.watching())
            ident.unwatch()
            self.assertFalse(ident.watching())
            done.add(my_id)

        if use_greenlets:
            t_watched = greenlet.greenlet(watched_thread)
            t_unwatched = greenlet.greenlet(unwatched_thread)
            looplet([t_watched, t_unwatched])
        else:
            t_watched = threading.Thread(target=watched_thread)
            t_watched.setDaemon(True)
            t_watched.start()

            t_unwatched = threading.Thread(target=unwatched_thread)
            t_unwatched.setDaemon(True)
            t_unwatched.start()

            t_watched.join()
            t_unwatched.join()

        # Remove references, let weakref callbacks run
        del t_watched
        del t_unwatched

        # Accessing the thread-local triggers cleanup in Python <= 2.6
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
        if not thread_util.have_greenlet:
            raise SkipTest('greenlet not installed')

        self._test_ident(True)


class TestCounter(unittest.TestCase):
    def _test_counter(self, use_greenlets):
        counter = thread_util.Counter(use_greenlets)

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

        if use_greenlets:
            greenlets = [
                greenlet.greenlet(partial(f, i)) for i in range(10)]
            looplet(greenlets)
        else:
            threads = [
                threading.Thread(target=partial(f, i)) for i in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        self.assertEqual(10, len(done))

    def test_thread_counter(self):
        self._test_counter(False)

    def test_greenlet_counter(self):
        if not thread_util.have_greenlet:
            raise SkipTest('greenlet not installed')

        self._test_counter(True)

if __name__ == "__main__":
    unittest.main()

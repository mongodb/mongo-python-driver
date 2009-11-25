# Copyright 2009 10gen, Inc.
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

"""Test some utilities for threading."""

import unittest
import threading
import time
import sys

sys.path[0:0] = [""]

from pymongo.thread_util import TimeoutableLock


class AcquireWithTimeout(threading.Thread):

    def __init__(self, lock, timeout):
        threading.Thread.__init__(self)
        self.__lock = lock
        self.__timeout = timeout

    def run(self):
        assert self.__lock.acquire(timeout=self.__timeout)


class AcquireHoldRelease(threading.Thread):

    def __init__(self, lock, hold_time, timeout, counter):
        threading.Thread.__init__(self)
        self.__lock = lock
        self.__hold_time = hold_time
        self.__timeout = timeout
        self.__counter = counter

    def run(self):
        if not self.__lock.acquire(timeout=self.__timeout):
            self.__counter.assert_count += 1
            return
        time.sleep(self.__hold_time)
        self.__lock.release()


class TestTimeoutableLock(unittest.TestCase):

    def test_basic(self):
        lock = TimeoutableLock()

        self.assert_(lock.acquire())
        self.failIf(lock.acquire(False))

        lock.release()
        self.assert_(lock.acquire())

        lock.release()
        self.assertRaises(RuntimeError, lock.release)

    def test_timeout(self):
        lock = TimeoutableLock()
        self.assert_(lock.acquire())
        before = time.time()
        self.failIf(lock.acquire(timeout=0.25))
        self.assertAlmostEqual(0.25, time.time() - before, 1)

    def test_blocking_acquire(self):
        lock = TimeoutableLock()
        self.assert_(lock.acquire())
        t = AcquireWithTimeout(lock, None)
        t.start()
        lock.release()
        t.join()
        self.failIf(lock.acquire(False))

    def test_acquire_early(self):
        lock = TimeoutableLock()
        self.assert_(lock.acquire())
        t = AcquireWithTimeout(lock, 10)

        before = time.time()
        t.start()
        lock.release()
        t.join()
        self.assert_(time.time() - before < 1)
        self.failIf(lock.acquire(False))

    def test_multiple_threads(self):
        lock = TimeoutableLock()

        class Foo:
            pass
        counter = Foo()
        counter.assert_count = 0

        def start_and_join(thread_count):
            threads = [AcquireHoldRelease(lock, 0.1, 0.31, counter)
                       for _ in range(thread_count)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        start_and_join(3)
        self.failIf(counter.assert_count)
        start_and_join(4)
        self.assertEqual(counter.assert_count, 1)
        start_and_join(3)
        self.assertEqual(counter.assert_count, 1)

if __name__ == "__main__":
    unittest.main()

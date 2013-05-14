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

"""Utilities to abstract the differences between threads and greenlets."""

class IdentBase(object):
    def __init__(self):
        self._refs = {}

    def watching(self):
        """Is the current thread or greenlet being watched for death?"""
        return self.get() in self._refs

    def unwatch(self, tid=None):
        if tid is None:
            tid = self.get()
        self._refs.pop(tid, None)

    def get(self):
        """An id for this thread or greenlet"""
        raise NotImplementedError

    def watch(self, callback):
        """Run callback when this thread or greenlet dies. callback takes
        one meaningless argument.
        """
        raise NotImplementedError


class Counter(object):
    """A thread- or greenlet-local counter.
    """
    def __init__(self, thread_support_module):
        self.ident = thread_support_module.Ident()
        self._counters = {}

    def inc(self):
        tid = self.ident.get()
        self._counters.setdefault(tid, 0)
        self._counters[tid] += 1
        return self._counters[tid]

    def dec(self):
        tid = self.ident.get()
        if self._counters.get(tid, 0) > 0:
            self._counters[tid] -= 1
            return self._counters[tid]
        else:
            return 0

    def get(self):
        return self._counters.get(self.ident.get(), 0)


class DummySemaphore(object):
    def __init__(self, value=None):
        pass

    def acquire(self, blocking=True, timeout=None):
        return True

    def release(self):
        pass


class ExceededMaxWaiters(Exception):
    pass


class MaxWaitersBoundedSemaphore(object):
    def __init__(self, semaphore_class, value=1, max_waiters=1):
        self.waiter_semaphore = semaphore_class(max_waiters)
        self.semaphore = semaphore_class(value)

    def acquire(self, blocking=True, timeout=None):
        if not self.waiter_semaphore.acquire(False):
            raise ExceededMaxWaiters()
        try:
            return self.semaphore.acquire(blocking, timeout)
        finally:
            self.waiter_semaphore.release()

    def __getattr__(self, name):
        return getattr(self.semaphore, name)

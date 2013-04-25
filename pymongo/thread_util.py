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

import threading
import sys
import weakref

have_greenlet = True
try:
    import greenlet
except ImportError:
    have_greenlet = False


# Do we have to work around http://bugs.python.org/issue1868?
issue1868 = (sys.version_info[:3] <= (2, 7, 0))


class Ident(object):
    def __init__(self):
        self._refs = {}

    def watching(self):
        """Is the current thread or greenlet being watched for death?"""
        return self.get() in self._refs

    def unwatch(self):
        self._refs.pop(self.get(), None)

    def get(self):
        """An id for this thread or greenlet"""
        raise NotImplementedError

    def watch(self, callback):
        """Run callback when this thread or greenlet dies. callback takes
        one meaningless argument.
        """
        raise NotImplementedError


class ThreadIdent(Ident):
    class _DummyLock(object):
        def acquire(self):
            pass

        def release(self):
            pass

    def __init__(self):
        super(ThreadIdent, self).__init__()
        self._local = threading.local()
        if issue1868:
            self._lock = threading.Lock()
        else:
            self._lock = ThreadIdent._DummyLock()

    # We watch for thread-death using a weakref callback to a thread local.
    # Weakrefs are permitted on subclasses of object but not object() itself.
    class ThreadVigil(object):
        pass

    def _make_vigil(self):
        # Threadlocals in Python <= 2.7.0 have race conditions when setting
        # attributes and possibly when getting them, too, leading to weakref
        # callbacks not getting called later.
        self._lock.acquire()
        try:
            vigil = getattr(self._local, 'vigil', None)
            if not vigil:
                self._local.vigil = vigil = ThreadIdent.ThreadVigil()
        finally:
            self._lock.release()

        return vigil

    def get(self):
        return id(self._make_vigil())

    def watch(self, callback):
        vigil = self._make_vigil()
        self._refs[id(vigil)] = weakref.ref(vigil, callback)


class GreenletIdent(Ident):
    def get(self):
        return id(greenlet.getcurrent())

    def watch(self, callback):
        current = greenlet.getcurrent()
        tid = self.get()

        if hasattr(current, 'link'):
            # This is a Gevent Greenlet (capital G), which inherits from
            # greenlet and provides a 'link' method to detect when the
            # Greenlet exits.
            current.link(callback)
            self._refs[tid] = None
        else:
            # This is a non-Gevent greenlet (small g), or it's the main
            # greenlet.
            self._refs[tid] = weakref.ref(current, callback)


def create_ident(use_greenlets):
    if use_greenlets:
        return GreenletIdent()
    else:
        return ThreadIdent()


class Counter(object):
    """A thread- or greenlet-local counter.
    """
    def __init__(self, use_greenlets):
        self.ident = create_ident(use_greenlets)
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

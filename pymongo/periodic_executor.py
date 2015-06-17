# Copyright 2014-2015 MongoDB, Inc.
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

"""Run a target function on a background thread."""

import atexit
import threading
import time
import weakref

from pymongo import thread_util
from pymongo.monotonic import time as _time


class PeriodicExecutor(object):
    def __init__(self, condition_class, interval, min_interval, target):
        """"Run a target function periodically on a background thread.

        If the target's return value is false, the executor stops.

        :Parameters:
          - `condition_class`: A class like threading.Condition.
          - `interval`: Seconds between calls to `target`.
          - `min_interval`: Minimum seconds between calls if `wake` is
            called very often.
          - `target`: A function.
        """
        self._event = thread_util.Event(condition_class)
        self._interval = interval
        self._min_interval = min_interval
        self._target = target
        self._stopped = False
        self._thread = None

    def open(self):
        """Start. Multiple calls have no effect.

        Not safe to call from multiple threads at once.
        """
        self._stopped = False
        started = False
        try:
            started = self._thread and self._thread.is_alive()
        except ReferenceError:
            # Thread terminated.
            pass

        if not started:
            thread = threading.Thread(target=self._run)
            thread.daemon = True
            self._thread = weakref.proxy(thread)
            _register_executor(self)
            thread.start()

    def close(self, dummy=None):
        """Stop. To restart, call open().

        The dummy parameter allows an executor's close method to be a weakref
        callback; see monitor.py.

        Since this can be called from a weakref callback during garbage
        collection it must take no locks! That means it cannot call wake().
        """
        self._stopped = True

    def join(self, timeout=None):
        if self._thread is not None:
            try:
                self._thread.join(timeout)
            except ReferenceError:
                # Thread already terminated.
                pass

    def wake(self):
        """Execute the target function soon."""
        self._event.set()

    def _run(self):
        while not self._stopped:
            try:
                if not self._target():
                    self._stopped = True
                    break
            except:
                self._stopped = True
                raise

            deadline = _time() + self._interval

            # Avoid running too frequently if wake() is called very often.
            time.sleep(self._min_interval)

            # Until the deadline, wake often to check if close() was called.
            while not self._stopped and _time() < deadline:
                # Our Event's wait returns True if set, else False.
                if self._event.wait(0.1):
                    # Someone called wake().
                    break

            self._event.clear()


# _EXECUTORS has a weakref to each running PeriodicExecutor. Once started,
# an executor is kept alive by a strong reference from its thread and perhaps
# from other objects. When the thread dies and all other referrers are freed,
# the executor is freed and removed from _EXECUTORS. If any threads are
# running when the interpreter begins to shut down, we try to halt and join
# them to avoid spurious errors.
_EXECUTORS = set()


def _register_executor(executor):
    ref = weakref.ref(executor, _on_executor_deleted)
    _EXECUTORS.add(ref)


def _on_executor_deleted(ref):
    _EXECUTORS.remove(ref)


def _shutdown_executors():
    # Copy the set. Stopping threads has the side effect of removing executors.
    executors = list(_EXECUTORS)

    # First signal all executors to close...
    for ref in executors:
        executor = ref()
        if executor:
            executor.close()

    # ...then try to join them.
    for ref in executors:
        executor = ref()
        if executor:
            executor.join(1)

    executor = None

atexit.register(_shutdown_executors)

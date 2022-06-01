# Copyright 2022-present MongoDB, Inc.
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

"""Internal helpers for CSOT."""

import time
from contextvars import ContextVar
from typing import ContextManager, Optional


class _Vars:
    """Singleton for managing various ContextVars."""

    def __init__(self):
        self._timeout: ContextVar[Optional[float]] = ContextVar("timeout", default=None)
        self._rtt: ContextVar[float] = ContextVar("rtt", default=0.0)
        self._deadline: ContextVar[float] = ContextVar("deadline", default=float("inf"))

    def get_timeout(self) -> Optional[float]:
        return self._timeout.get(None)

    def get_rtt(self) -> float:
        return self._rtt.get()

    def get_deadline(self) -> float:
        return self._deadline.get()

    def set_rtt(self, rtt: float) -> None:
        self._rtt.set(rtt)

    def set_timeout(self, timeout: Optional[float]) -> None:
        self._timeout.set(timeout)
        self._deadline.set(time.monotonic() + timeout if timeout else float("inf"))

    def remaining(self) -> Optional[float]:
        if not self.get_timeout():
            return None
        return self._deadline.get() - time.monotonic()

    def clamp_remaining(self, max_timeout: float) -> float:
        """Return the remaining timeout clamped to a max value."""
        remaining = self.remaining()
        if remaining is None:
            return max_timeout
        return min(remaining, max_timeout)

    @staticmethod
    def with_timeout(timeout: Optional[float]) -> ContextManager:
        """Set a timeout context for pymongo.timeout()."""
        return _TimeoutContext(timeout)

    def enter(self, timeout: Optional[float]) -> None:
        self.set_timeout(timeout)

    def exit(self) -> None:
        self.set_timeout(None)


class _TimeoutContext(object):
    """Internal timeout context manager.

    Use :func:`pymongo.timeout` instead::

      with client.timeout(0.5):
          client.test.test.insert_one({})
    """

    __slots__ = ("_timeout",)

    def __init__(self, timeout: Optional[float]):
        self._timeout = timeout

    def __enter__(self):
        _VARS.enter(self._timeout)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _VARS.exit()


_VARS = _Vars()

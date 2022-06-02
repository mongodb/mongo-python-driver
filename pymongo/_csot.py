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
from typing import Optional

TIMEOUT: ContextVar[Optional[float]] = ContextVar("TIMEOUT", default=None)
RTT: ContextVar[float] = ContextVar("RTT", default=0.0)
DEADLINE: ContextVar[float] = ContextVar("DEADLINE", default=float("inf"))


def get_timeout() -> Optional[float]:
    return TIMEOUT.get(None)


def get_rtt() -> float:
    return RTT.get()


def get_deadline() -> float:
    return DEADLINE.get()


def set_rtt(rtt: float) -> None:
    RTT.set(rtt)


def set_timeout(timeout: Optional[float]) -> None:
    TIMEOUT.set(timeout)
    DEADLINE.set(time.monotonic() + timeout if timeout else float("inf"))


def remaining() -> Optional[float]:
    if not get_timeout():
        return None
    return DEADLINE.get() - time.monotonic()


def clamp_remaining(max_timeout: float) -> float:
    """Return the remaining timeout clamped to a max value."""
    timeout = remaining()
    if timeout is None:
        return max_timeout
    return min(timeout, max_timeout)


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
        set_timeout(self._timeout)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        set_timeout(None)

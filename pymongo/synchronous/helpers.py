# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Miscellaneous pieces that need to be synchronized."""
from __future__ import annotations

import asyncio
import builtins
import functools
import random
import socket
import sys
import time as time  # noqa: PLC0414 # needed in sync version
from typing import (
    Any,
    Callable,
    TypeVar,
    cast,
)

from pymongo import _csot
from pymongo.common import MAX_ADAPTIVE_RETRIES
from pymongo.errors import (
    OperationFailure,
)
from pymongo.helpers_shared import _REAUTHENTICATION_REQUIRED_CODE

_IS_SYNC = True

# See https://mypy.readthedocs.io/en/stable/generics.html?#decorator-factories
F = TypeVar("F", bound=Callable[..., Any])


def _handle_reauth(func: F) -> F:
    @functools.wraps(func)
    def inner(*args: Any, **kwargs: Any) -> Any:
        no_reauth = kwargs.pop("no_reauth", False)
        from pymongo.message import _BulkWriteContext
        from pymongo.synchronous.pool import Connection

        try:
            return func(*args, **kwargs)
        except OperationFailure as exc:
            if no_reauth:
                raise
            if exc.code == _REAUTHENTICATION_REQUIRED_CODE:
                # Look for an argument that either is a Connection
                # or has a connection attribute, so we can trigger
                # a reauth.
                conn = None
                for arg in args:
                    if isinstance(arg, Connection):
                        conn = arg
                        break
                    if isinstance(arg, _BulkWriteContext):
                        conn = arg.conn  # type: ignore[assignment]
                        break
                if conn:
                    conn.authenticate(reauthenticate=True)
                else:
                    raise
                return func(*args, **kwargs)
            raise

    return cast(F, inner)


_BACKOFF_INITIAL = 0.1
_BACKOFF_MAX = 10


def _backoff(
    attempt: int, initial_delay: float = _BACKOFF_INITIAL, max_delay: float = _BACKOFF_MAX
) -> float:
    jitter = random.random()  # noqa: S311
    return jitter * min(initial_delay * (2**attempt), max_delay)


class _RetryPolicy:
    """A retry limiter that performs exponential backoff with jitter."""

    def __init__(
        self,
        attempts: int = MAX_ADAPTIVE_RETRIES,
        backoff_initial: float = _BACKOFF_INITIAL,
        backoff_max: float = _BACKOFF_MAX,
    ):
        self.attempts = attempts
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max

    def backoff(self, attempt: int) -> float:
        """Return the backoff duration for the given attempt."""
        return _backoff(max(0, attempt - 1), self.backoff_initial, self.backoff_max)

    def should_retry(self, attempt: int, delay: float) -> bool:
        """Return if we have retry attempts remaining and the next backoff would not exceed a timeout."""
        if attempt > self.attempts:
            return False

        if _csot.get_timeout():
            if time.monotonic() + delay > _csot.get_deadline():
                return False

        return True


def _getaddrinfo(
    host: Any, port: Any, **kwargs: Any
) -> list[
    tuple[
        socket.AddressFamily,
        socket.SocketKind,
        int,
        str,
        tuple[str, int] | tuple[str, int, int, int] | tuple[int, bytes],
    ]
]:
    if not _IS_SYNC:
        loop = asyncio.get_running_loop()
        return loop.getaddrinfo(host, port, **kwargs)  # type: ignore[return-value]
    else:
        return socket.getaddrinfo(host, port, **kwargs)


if sys.version_info >= (3, 10):
    next = builtins.next
    iter = builtins.iter
else:

    def next(cls: Any) -> Any:
        """Compatibility function until we drop 3.9 support: https://docs.python.org/3/library/functions.html#next."""
        return cls.__next__()

    def iter(cls: Any) -> Any:
        """Compatibility function until we drop 3.9 support: https://docs.python.org/3/library/functions.html#next."""
        return cls.__iter__()

# Copyright 2026-present MongoDB, Inc.
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

"""KMS connection support shared by the synchronous and asynchronous APIs.

The per-API connection logic lives in ``pymongo.asynchronous._kms_connect``
and its generated synchronous mirror.
"""

from __future__ import annotations

import contextlib
import socket
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class KMSConnectContext:
    """Information about a pending KMS connection.

    Passed to ``kms_connect_callback``, which must return a plain, unwrapped
    :class:`socket.socket`. The driver performs the KMS TLS handshake over it,
    verifying against ``host`` rather than the peer actually reached.

    :param host: Hostname of the KMS server, and the TLS verification target.
    :param port: Port of the KMS server.
    :param timeout: Seconds allowed for the connection: the default KMS
        connect timeout, capped by the remaining time of an active operation
        timeout (``timeoutMS``).

    .. note:: ``timeoutMS`` does not constrain KMS requests for explicit
       encryption, so ``timeout`` is always the default there. Automatic
       encryption passes the remaining budget. This deviates from the Client
       Side Operations Timeout specification; see PYTHON-6037.

    .. versionadded:: 4.19
    """

    host: str
    port: int
    timeout: float


# A callback that opens a connection to a KMS host.
AsyncKMSConnectCallback = Callable[[KMSConnectContext], Awaitable[socket.socket]]
KMSConnectCallback = Callable[[KMSConnectContext], socket.socket]


def _close_rejected_kms_socket(obj: Any) -> None:
    """Close a callback return value that failed validation, best effort.

    ``_connect_kms`` raises on a contract violation instead of returning the
    value, so no caller ever takes ownership of it. Close it here, tolerating
    non-socket values and ``close()`` failures.
    """
    close = getattr(obj, "close", None)
    if callable(close):
        with contextlib.suppress(Exception):
            close()


# Sphinx documents this class under pymongo.encryption_options, the public
# import path, so the definition must claim that module name.
KMSConnectContext.__module__ = "pymongo.encryption_options"

# Copyright 2026-present MongoDB, Inc.
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

"""Internal helpers for the APM operation id.

The retryable read/write logic sets OP_ID for the duration of each attempt so
that every attempt of one logical operation publishes the same operation_id.
Commands run outside that scope (handshake, auth, killCursors, pinned-cursor
getMores) read the default None and fall back to their request_id.
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import Optional

OP_ID: ContextVar[Optional[int]] = ContextVar("OP_ID", default=None)

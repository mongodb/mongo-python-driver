# Copyright 2024-present MongoDB, Inc.
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
"""Constants used by the driver that don't really fit elsewhere."""

# From the SDAM spec, the "node is shutting down" codes.
from __future__ import annotations

_SHUTDOWN_CODES: frozenset = frozenset(
    [
        11600,  # InterruptedAtShutdown
        91,  # ShutdownInProgress
    ]
)
# From the SDAM spec, the "not primary" error codes are combined with the
# "node is recovering" error codes (of which the "node is shutting down"
# errors are a subset).
_NOT_PRIMARY_CODES: frozenset = (
    frozenset(
        [
            10058,  # LegacyNotPrimary <=3.2 "not primary" error code
            10107,  # NotWritablePrimary
            13435,  # NotPrimaryNoSecondaryOk
            11602,  # InterruptedDueToReplStateChange
            13436,  # NotPrimaryOrSecondary
            189,  # PrimarySteppedDown
        ]
    )
    | _SHUTDOWN_CODES
)
# From the retryable writes spec.
_RETRYABLE_ERROR_CODES: frozenset = _NOT_PRIMARY_CODES | frozenset(
    [
        7,  # HostNotFound
        6,  # HostUnreachable
        89,  # NetworkTimeout
        9001,  # SocketException
        262,  # ExceededTimeLimit
        134,  # ReadConcernMajorityNotAvailableYet
    ]
)

# Server code raised when re-authentication is required
_REAUTHENTICATION_REQUIRED_CODE: int = 391

# Server code raised when authentication fails.
_AUTHENTICATION_FAILURE_CODE: int = 18

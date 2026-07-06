# Copyright 2026-present MongoDB, Inc.
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

"""Test the sans-I/O SDAM error-handling core.

These tests exercise pymongo._sdam_error directly. They perform no I/O and
require no MongoDB server: the module is pure decision logic.
"""

from __future__ import annotations

import sys

sys.path[0:0] = [""]

from pymongo._sdam_error import (
    _NO_ACTION,
    _SDAMAction,
    decide_error_action,
    error_topology_version,
    is_stale_error_topology_version,
)
from pymongo.errors import (
    ConnectionFailure,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    WaitQueueTimeoutError,
    WriteError,
)
from test import unittest

# Baseline context: an error that occurred after a completed handshake on a
# modern (non-load-balanced) server.
_BASE = dict(
    load_balanced=False,
    has_service_id=False,
    completed_handshake=True,
    max_wire_version=21,
)


def _decide(error, **overrides):
    return decide_error_action(error, **{**_BASE, **overrides})


class TestDecideErrorAction(unittest.TestCase):
    def test_network_timeout_after_handshake_is_ignored(self):
        # SDAM: a network timeout after handshake does not reset the server.
        self.assertEqual(_decide(NetworkTimeout("x")), _NO_ACTION)

    def test_network_timeout_during_handshake_is_connection_failure(self):
        # NetworkTimeout is a ConnectionFailure; before handshake it is not the
        # "ignore" case, so it marks Unknown, resets, and cancels the check.
        self.assertEqual(
            _decide(NetworkTimeout("x"), completed_handshake=False),
            _SDAMAction(mark_unknown=True, reset_pool=True, cancel_check=True),
        )

    def test_write_error_is_ignored(self):
        self.assertEqual(_decide(WriteError("x", 1)), _NO_ACTION)

    def test_not_primary_keeps_pool_and_requests_check(self):
        # Code 10107 is a "not primary" code; on a modern server we keep the
        # pool, mark Unknown, and request an immediate check.
        err = NotPrimaryError("not primary", {"code": 10107, "errmsg": "not primary"})
        self.assertEqual(
            _decide(err),
            _SDAMAction(mark_unknown=True, request_check=True),
        )

    def test_shutdown_code_also_resets_pool(self):
        # Code 91 (ShutdownInProgress) additionally clears the pool.
        err = NotPrimaryError("shutting down", {"code": 91, "errmsg": "shutting down"})
        self.assertEqual(
            _decide(err),
            _SDAMAction(mark_unknown=True, reset_pool=True, request_check=True),
        )

    def test_not_primary_on_old_server_resets_pool(self):
        # max_wire_version <= 7 forces a pool reset even for non-shutdown codes.
        err = NotPrimaryError("not primary", {"code": 10107, "errmsg": "not primary"})
        self.assertEqual(
            _decide(err, max_wire_version=7),
            _SDAMAction(mark_unknown=True, reset_pool=True, request_check=True),
        )

    def test_operation_failure_during_handshake_resets_pool(self):
        # A non-"not primary" command error during handshake marks Unknown and
        # resets the pool, but does not request a check.
        err = OperationFailure("boom", 59, {"code": 59})
        self.assertEqual(
            _decide(err, completed_handshake=False),
            _SDAMAction(mark_unknown=True, reset_pool=True),
        )

    def test_operation_failure_after_handshake_is_ignored(self):
        # A non-"not primary" command error after handshake is ignored.
        err = OperationFailure("boom", 59, {"code": 59})
        self.assertEqual(_decide(err), _NO_ACTION)

    def test_connection_failure_marks_unknown_resets_and_cancels(self):
        self.assertEqual(
            _decide(ConnectionFailure("x")),
            _SDAMAction(mark_unknown=True, reset_pool=True, cancel_check=True),
        )

    def test_wait_queue_timeout_is_ignored(self):
        self.assertEqual(_decide(WaitQueueTimeoutError("x")), _NO_ACTION)

    def test_system_overloaded_label_is_ignored(self):
        err = ConnectionFailure("x")
        err._add_error_label("SystemOverloadedError")
        self.assertEqual(_decide(err), _NO_ACTION)

    def test_load_balanced_handshake_without_service_id_is_ignored(self):
        # Behind a load balancer with no known service ID, a handshake error
        # cannot be attributed to a pool and is ignored.
        self.assertEqual(
            _decide(
                ConnectionFailure("x"),
                load_balanced=True,
                has_service_id=False,
                completed_handshake=False,
            ),
            _NO_ACTION,
        )

    def test_load_balanced_suppresses_mark_unknown(self):
        # In load-balanced mode we never mark the server Unknown, but we still
        # reset the pool and cancel the check for a connection failure.
        self.assertEqual(
            _decide(
                ConnectionFailure("x"),
                load_balanced=True,
                has_service_id=True,
                completed_handshake=True,
            ),
            _SDAMAction(mark_unknown=False, reset_pool=True, cancel_check=True),
        )


class TestStaleErrorHelpers(unittest.TestCase):
    def test_error_topology_version_absent(self):
        self.assertIsNone(error_topology_version(None))
        self.assertIsNone(error_topology_version(ConnectionFailure("x")))
        self.assertIsNone(error_topology_version(OperationFailure("x", 1, {"code": 1})))

    def test_error_topology_version_present(self):
        tv = {"processId": "abc", "counter": 3}
        err = OperationFailure("x", 1, {"code": 1, "topologyVersion": tv})
        self.assertEqual(error_topology_version(err), tv)

    def test_stale_when_current_counter_not_older(self):
        cur = {"processId": "p", "counter": 5}
        older = {"processId": "p", "counter": 4}
        same = {"processId": "p", "counter": 5}
        newer = {"processId": "p", "counter": 6}
        self.assertTrue(is_stale_error_topology_version(cur, older))
        self.assertTrue(is_stale_error_topology_version(cur, same))
        self.assertFalse(is_stale_error_topology_version(cur, newer))

    def test_not_stale_when_process_differs_or_missing(self):
        cur = {"processId": "p", "counter": 5}
        other = {"processId": "q", "counter": 1}
        self.assertFalse(is_stale_error_topology_version(cur, other))
        self.assertFalse(is_stale_error_topology_version(None, cur))
        self.assertFalse(is_stale_error_topology_version(cur, None))


if __name__ == "__main__":
    unittest.main()

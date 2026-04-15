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

"""Test the pymongo daemon module."""
from __future__ import annotations

import subprocess
import sys
import warnings
from unittest.mock import MagicMock, patch

sys.path[0:0] = [""]

from test import unittest

import pymongo.daemon as daemon_module
from pymongo.daemon import _popen_wait, _silence_resource_warning, _spawn_daemon


class TestPopenWait(unittest.TestCase):
    def test_returns_returncode_on_success(self):
        mock_popen = MagicMock()
        mock_popen.wait.return_value = 0
        self.assertEqual(0, _popen_wait(mock_popen, timeout=5))
        mock_popen.wait.assert_called_once_with(timeout=5)

    def test_returns_none_on_timeout_expired(self):
        mock_popen = MagicMock()
        mock_popen.wait.side_effect = subprocess.TimeoutExpired(cmd="foo", timeout=5)
        self.assertIsNone(_popen_wait(mock_popen, timeout=5))

    def test_none_timeout_passes_through(self):
        mock_popen = MagicMock()
        mock_popen.wait.return_value = 1
        self.assertEqual(1, _popen_wait(mock_popen, timeout=None))
        mock_popen.wait.assert_called_once_with(timeout=None)


class TestSilenceResourceWarning(unittest.TestCase):
    def test_sets_returncode_to_zero(self):
        mock_popen = MagicMock()
        mock_popen.returncode = None
        _silence_resource_warning(mock_popen)
        self.assertEqual(0, mock_popen.returncode)

    def test_no_op_for_none(self):
        # Should not raise when popen is None (mongocryptd spawn failed).
        _silence_resource_warning(None)


@unittest.skipIf(sys.platform == "win32", "Unix only")
class TestSpawnUnix(unittest.TestCase):
    def setUp(self):
        from pymongo.daemon import _spawn

        self._spawn = _spawn

    def test_returns_popen_on_success(self):
        mock_popen = MagicMock()
        with patch("subprocess.Popen", return_value=mock_popen):
            result = self._spawn(["somecommand"])
        self.assertIs(mock_popen, result)

    def test_filenotfound_warns_and_returns_none(self):
        with patch("subprocess.Popen", side_effect=FileNotFoundError("not found")):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                result = self._spawn(["nonexistent_command"])
        self.assertIsNone(result)
        self.assertEqual(1, len(w))
        self.assertIs(RuntimeWarning, w[0].category)
        self.assertIn("nonexistent_command", str(w[0].message))


@unittest.skipIf(sys.platform == "win32", "Unix only")
class TestSpawnDaemonDoublePopen(unittest.TestCase):
    def setUp(self):
        from pymongo.daemon import _spawn_daemon_double_popen

        self._spawn_daemon_double_popen = _spawn_daemon_double_popen

    def test_spawns_this_file_as_intermediate(self):
        mock_popen = MagicMock()
        mock_popen.wait.return_value = 0
        with patch("subprocess.Popen", return_value=mock_popen) as mock_cls:
            self._spawn_daemon_double_popen(["somecommand", "--arg"])
        spawner_args = mock_cls.call_args[0][0]
        self.assertEqual(sys.executable, spawner_args[0])
        self.assertIn("daemon.py", spawner_args[1])
        self.assertIn("somecommand", spawner_args)

    def test_waits_for_intermediate_process(self):
        mock_popen = MagicMock()
        with patch("subprocess.Popen", return_value=mock_popen):
            self._spawn_daemon_double_popen(["somecommand"])
        mock_popen.wait.assert_called_once_with(timeout=daemon_module._WAIT_TIMEOUT)

    def test_continues_on_timeout(self):
        # _popen_wait swallows TimeoutExpired — double Popen must not raise.
        mock_popen = MagicMock()
        mock_popen.wait.side_effect = subprocess.TimeoutExpired(cmd="foo", timeout=10)
        with patch("subprocess.Popen", return_value=mock_popen):
            self._spawn_daemon_double_popen(["somecommand"])  # must not raise


@unittest.skipIf(sys.platform == "win32", "Unix only")
class TestSpawnDaemonUnix(unittest.TestCase):
    def test_uses_double_popen_when_executable_set(self):
        with patch("pymongo.daemon._spawn_daemon_double_popen") as mock_double:
            _spawn_daemon(["somecommand"])
        mock_double.assert_called_once_with(["somecommand"])

    def test_fallback_to_spawn_when_no_executable(self):
        with patch("pymongo.daemon._spawn") as mock_spawn:
            with patch.object(sys, "executable", ""):
                _spawn_daemon(["somecommand"])
        mock_spawn.assert_called_once_with(["somecommand"])


@unittest.skipUnless(sys.platform == "win32", "Windows only")
class TestSpawnDaemonWindows(unittest.TestCase):
    def test_silences_resource_warning_on_success(self):
        mock_popen = MagicMock()
        with patch("subprocess.Popen", return_value=mock_popen):
            _spawn_daemon(["somecommand"])
        self.assertEqual(0, mock_popen.returncode)

    def test_filenotfound_warns(self):
        with patch("subprocess.Popen", side_effect=FileNotFoundError("not found")):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                _spawn_daemon(["nonexistent_command"])
        self.assertEqual(1, len(w))
        self.assertIs(RuntimeWarning, w[0].category)
        self.assertIn("nonexistent_command", str(w[0].message))


@unittest.skipIf(sys.platform == "win32", "Unix only")
class TestMainBlock(unittest.TestCase):
    def test_exits_with_zero(self):
        # Run daemon.py as a script with a no-op subprocess; verify it exits cleanly.
        result = subprocess.run(
            [sys.executable, "-m", "pymongo.daemon", sys.executable, "-c", "pass"],
            timeout=15,
        )
        self.assertEqual(0, result.returncode)


if __name__ == "__main__":
    unittest.main()

"""Tests for SOCKS5 test setup, execution, and teardown."""

from __future__ import annotations

import os
import runpy
import signal
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import run_tests
import setup_tests
from utils import TEST_SUITE_MAP


@pytest.fixture
def proxy_setup(monkeypatch, tmp_path):
    monkeypatch.setenv("MONGODB_URI", "mongodb://localhost:27017/")
    monkeypatch.setattr(setup_tests, "HERE", tmp_path)
    first = mock.Mock(pid=11)
    second = mock.Mock(pid=22)
    for process in (first, second):
        process.poll.return_value = None
    with (
        mock.patch.object(setup_tests, "check_drivers_tools"),
        mock.patch.object(setup_tests, "write_env") as write_env,
        mock.patch.object(setup_tests.socket, "create_connection") as connect,
        mock.patch.object(setup_tests.subprocess, "Popen", side_effect=[first, second]) as popen,
    ):
        yield first, second, popen, connect, write_env


def test_setup_starts_and_checks_both_proxies(proxy_setup):
    first, second, popen, connect, write_env = proxy_setup
    setup_tests.setup_socks5()
    script = str((Path(setup_tests.DRIVERS_TOOLS) / ".evergreen" / "socks5srv.py").absolute())
    args = [call.args[0] for call in popen.call_args_list]
    assert args == [
        [
            sys.executable,
            script,
            "--port",
            "1080",
            "--map",
            "localhost:12345 to localhost:27017",
            "--auth",
            "username:p4ssw0rd",
        ],
        [
            sys.executable,
            script,
            "--port",
            "1081",
            "--map",
            "localhost:12345 to localhost:27017",
        ],
    ]
    assert connect.call_args_list == [
        mock.call(("127.0.0.1", 1080), timeout=0.1),
        mock.call(("127.0.0.1", 1081), timeout=0.1),
    ]
    assert write_env.call_args_list == [
        mock.call("SOCKS5_PROXY_PIDS", "11 22"),
        mock.call("SOCKS5_PROXIES_STARTED"),
    ]
    first.terminate.assert_not_called()
    second.terminate.assert_not_called()


def test_setup_cleans_up_after_second_proxy_fails(proxy_setup, monkeypatch):
    first, second, _popen, connect, write_env = proxy_setup
    connect.side_effect = [mock.MagicMock(), ConnectionRefusedError("not ready")]
    monkeypatch.setattr(setup_tests, "time", mock.Mock(monotonic=mock.Mock(side_effect=[0, 0, 11])))
    with pytest.raises(RuntimeError, match="proxy on port 1081 did not start"):
        setup_tests.setup_socks5()
    for process in (first, second):
        process.terminate.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=10)
    write_env.assert_not_called()


def test_setup_rejects_exited_proxy(proxy_setup):
    first, _second, popen, _connect, write_env = proxy_setup
    first.poll.return_value = 1
    with pytest.raises(RuntimeError, match="proxy on port 1080 exited"):
        setup_tests.setup_socks5()
    popen.assert_called_once()
    first.terminate.assert_not_called()
    first.wait.assert_called_once_with(timeout=10)
    write_env.assert_not_called()


@pytest.fixture
def proxy_teardown(monkeypatch):
    monkeypatch.setenv("TEST_NAME", "socks5")
    monkeypatch.setenv("SOCKS5_PROXY_PIDS", "11 22")
    monkeypatch.setenv("COVERAGE", "")
    script = str((Path(setup_tests.DRIVERS_TOOLS) / ".evergreen" / "socks5srv.py").absolute())
    result = subprocess.CompletedProcess([], 0, stdout=f"{sys.executable} {script} --port 1080")
    with (
        mock.patch("shutil.which", return_value="/usr/bin/ps"),
        mock.patch.object(subprocess, "run", return_value=result) as run,
        mock.patch.object(os, "kill") as kill,
    ):
        yield result, run, kill


def test_teardown_stops_recorded_proxies(proxy_teardown):
    _result, _run, kill = proxy_teardown
    runpy.run_path(str(setup_tests.HERE / "teardown_tests.py"))
    assert kill.call_args_list == [mock.call(11, signal.SIGTERM), mock.call(22, signal.SIGTERM)]


def test_teardown_refuses_reused_pid(proxy_teardown):
    result, _run, kill = proxy_teardown
    result.stdout = "python unrelated.py"
    with pytest.raises(RuntimeError, match="Refusing to stop unrelated process"):
        runpy.run_path(str(setup_tests.HERE / "teardown_tests.py"))
    kill.assert_not_called()


@pytest.mark.parametrize("pid", [-1, 0])
def test_teardown_rejects_invalid_pid(pid, proxy_teardown, monkeypatch):
    _result, _run, kill = proxy_teardown
    monkeypatch.setenv("SOCKS5_PROXY_PIDS", str(pid))
    with pytest.raises(ValueError, match="Invalid SOCKS5 proxy PID"):
        runpy.run_path(str(setup_tests.HERE / "teardown_tests.py"))
    kill.assert_not_called()


def test_teardown_accepts_exited_proxies(proxy_teardown):
    result, _run, kill = proxy_teardown
    result.returncode = 1
    runpy.run_path(str(setup_tests.HERE / "teardown_tests.py"))
    kill.assert_not_called()


def test_runner_selects_sync_and_async_prose_tests(monkeypatch):
    assert TEST_SUITE_MAP["socks5"] == ""
    monkeypatch.setattr(run_tests, "TEST_NAME", "socks5")
    monkeypatch.setattr(run_tests, "TEST_ARGS", ["-v"])
    monkeypatch.setattr(run_tests, "GREEN_FRAMEWORK", None)
    monkeypatch.setattr(run_tests, "TEST_PERF", None)
    monkeypatch.setattr(sys, "argv", ["run_tests.py"])
    monkeypatch.setenv("NO_EXT", "1")
    monkeypatch.setenv("PYMONGOCRYPT_LIB", "")
    monkeypatch.setenv("COVERAGE", "")
    monkeypatch.setenv("DEBUG_LOG", "")
    with (
        mock.patch.object(run_tests, "list_packages"),
        mock.patch.object(run_tests.pytest, "main", return_value=0) as pytest_main,
    ):
        run_tests.run()
    pytest_main.assert_called_once_with(
        ["-v", "test/test_socks5.py", "test/asynchronous/test_socks5.py", "-k", "TestSocks5Prose"]
    )


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("mongodb://localhost/", "localhost:27017"),
        ("mongodb://localhost:27018,localhost:27019/", "localhost:27018"),
        ("mongodb://user:pass@localhost:27018/", "localhost:27018"),
        ("mongodb://[::1]:27018,[::1]:27019/", "[::1]:27018"),
    ],
)
def test_first_seed_mapping(uri, expected):
    assert setup_tests._first_seed(uri) == expected

"""Tests for SOCKS5 test setup, execution, and teardown."""

from __future__ import annotations

import runpy
import signal
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import run_socks5_proxy
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
        mock.patch.object(setup_tests.socket, "socket") as socket_factory,
        mock.patch.object(setup_tests.socket, "create_connection") as connect,
        mock.patch.object(setup_tests.subprocess, "Popen", side_effect=[first, second]) as popen,
    ):
        yield first, second, popen, socket_factory, connect, write_env


def test_setup_starts_and_checks_both_proxies(proxy_setup):
    first, second, popen, socket_factory, connect, write_env = proxy_setup
    setup_tests.setup_socks5()
    args = [call.args[0] for call in popen.call_args_list]
    assert args == [
        [
            sys.executable,
            str(setup_tests.HERE / "run_socks5_proxy.py"),
            "--port",
            "1080",
            "--auth",
            "username:p4ssw0rd",
        ],
        [sys.executable, str(setup_tests.HERE / "run_socks5_proxy.py"), "--port", "1081"],
    ]
    assert socket_factory.return_value.__enter__.return_value.bind.call_args_list == [
        mock.call(("127.0.0.1", 1080)),
        mock.call(("127.0.0.1", 1081)),
    ]
    assert (
        socket_factory.return_value.__enter__.return_value.setsockopt.call_args_list
        == [mock.call(setup_tests.socket.SOL_SOCKET, setup_tests.socket.SO_REUSEADDR, 1)] * 2
    )
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
    first, second, _popen, _socket_factory, connect, write_env = proxy_setup
    connect.side_effect = [mock.MagicMock(), ConnectionRefusedError("not ready")]
    monkeypatch.setattr(setup_tests, "time", mock.Mock(monotonic=mock.Mock(side_effect=[0, 0, 11])))
    with pytest.raises(RuntimeError, match="proxy on port 1081 did not start"):
        setup_tests.setup_socks5()
    for process in (first, second):
        process.terminate.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=10)
    write_env.assert_not_called()


def test_setup_refuses_occupied_ports(proxy_setup):
    _first, _second, popen, socket_factory, _connect, write_env = proxy_setup
    socket_factory.return_value.__enter__.return_value.bind.side_effect = OSError("port in use")
    with pytest.raises(OSError, match="port in use"):
        setup_tests.setup_socks5()
    popen.assert_not_called()
    write_env.assert_not_called()


def test_teardown_stops_only_checkout_proxies():
    result = subprocess.CompletedProcess(
        [], 0, stdout=f"{sys.executable} {run_socks5_proxy.__file__} --port 1080"
    )
    with (
        mock.patch.object(run_socks5_proxy.subprocess, "run", return_value=result),
        mock.patch.object(run_socks5_proxy.os, "kill") as kill,
    ):
        run_socks5_proxy.stop_socks5_proxies([11])
    kill.assert_called_once_with(11, signal.SIGTERM)


def test_teardown_refuses_reused_pid():
    result = subprocess.CompletedProcess([], 0, stdout="python unrelated.py")
    with (
        mock.patch.object(run_socks5_proxy.subprocess, "run", return_value=result),
        mock.patch.object(run_socks5_proxy.os, "kill") as kill,
        pytest.raises(RuntimeError, match="Refusing to stop unrelated process"),
    ):
        run_socks5_proxy.stop_socks5_proxies([11])
    kill.assert_not_called()


@pytest.mark.parametrize("pid", [-1, 0])
def test_teardown_rejects_invalid_pid(pid):
    with (
        mock.patch.object(run_socks5_proxy.os, "kill") as kill,
        pytest.raises(ValueError, match="Invalid SOCKS5 proxy PID"),
    ):
        run_socks5_proxy.stop_socks5_proxies([pid])
    kill.assert_not_called()


def test_teardown_script_uses_recorded_proxy_pids(monkeypatch):
    monkeypatch.setenv("TEST_NAME", "socks5")
    monkeypatch.setenv("SOCKS5_PROXY_PIDS", "11 22")
    monkeypatch.setenv("COVERAGE", "")
    with mock.patch.object(run_socks5_proxy, "stop_socks5_proxies") as stop:
        runpy.run_path(str(setup_tests.HERE / "teardown_tests.py"))
    stop.assert_called_once_with([11, 22])


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
    assert run_socks5_proxy._first_seed(uri) == expected

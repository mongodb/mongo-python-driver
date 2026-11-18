"""Tests for .evergreen/scripts/setup_setup_sanitizer.py.

Run with: python -m pytest .evergreen/scripts/test_setup_sanitizer.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

import setup_sanitizer
import utils

pytestmark = [pytest.mark.default]


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A fake repo root with a fake env file and stale build artifacts."""
    root = tmp_path / "repo"
    (root / ".evergreen").mkdir(parents=True)
    (root / "build").mkdir()
    for pkg in ("bson", "pymongo"):
        (root / pkg).mkdir()
    (root / "bson" / "_cbson.so").write_bytes(b"")
    (root / "pymongo" / "_cmessage.so").write_bytes(b"")
    (root / ".evergreen" / "tsan-suppressions.txt").write_text("")
    interpreter = root / "interpreter" / "python3"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_bytes(b"")
    monkeypatch.setattr(setup_sanitizer, "ROOT", root)
    monkeypatch.setattr(setup_sanitizer, "TSAN_PYTHON", str(interpreter))
    monkeypatch.setattr(setup_sanitizer, "SANITIZER_PYTHON", str(interpreter))
    monkeypatch.setattr(utils, "ENV_FILE", tmp_path / "env.sh")
    return root


def env_lines():
    lines = {}
    for line in utils.ENV_FILE.read_text().splitlines():
        name, _, value = line.removeprefix("export ").partition("=")
        lines[name] = value.strip('"')
    return lines


@pytest.mark.usefixtures("repo")
def test_tsan_env_writes_expected_variables(monkeypatch):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")
    monkeypatch.setattr(setup_sanitizer, "tsan_interface_exported", lambda *_: True)

    test_args = setup_sanitizer.configure_env("tsan", "--existing args")

    lines = env_lines()
    assert lines["UV_PYTHON"] == setup_sanitizer.TSAN_PYTHON
    assert lines["CFLAGS"] == "-fsanitize=thread -fno-omit-frame-pointer -g -O1"
    assert lines["LDFLAGS"] == "-fsanitize=thread"
    assert lines["UV_NO_CACHE"] == "1"
    assert "tsan-suppressions.txt" in lines["TSAN_OPTIONS"]
    assert "halt_on_error=1" in lines["TSAN_OPTIONS"]
    assert lines["PYMONGO_C_EXT_MUST_BUILD"] == "1"
    assert test_args.startswith("-s test/test_bson.py test/test_message.py")
    assert test_args.endswith("--existing args")


def test_tsan_env_clears_stale_c_artifacts(repo, monkeypatch):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")
    monkeypatch.setattr(setup_sanitizer, "tsan_interface_exported", lambda *_: True)

    setup_sanitizer.configure_env("tsan", "")

    assert not (repo / "build").exists()
    assert not list(repo.glob("*/*.so"))


@pytest.mark.usefixtures("repo")
def test_asan_env_writes_expected_variables(monkeypatch):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")

    test_args = setup_sanitizer.configure_env("asan", "")

    lines = env_lines()
    assert lines["UV_PYTHON"] == setup_sanitizer.SANITIZER_PYTHON
    assert lines["CFLAGS"] == "-fsanitize=address,undefined -fno-omit-frame-pointer -g -O1"
    assert lines["LDFLAGS"] == "-fsanitize=address,undefined"
    assert lines["ASAN_OPTIONS"] == "detect_leaks=0"
    assert lines["UBSAN_OPTIONS"] == "halt_on_error=1:print_stacktrace=1"
    assert lines["UV_NO_CACHE"] == "1"
    assert "TSAN_OPTIONS" not in lines
    assert test_args.startswith("-s ")


@pytest.mark.usefixtures("repo")
def test_tsan_env_fails_without_the_exported_interface(monkeypatch):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")
    monkeypatch.setattr(setup_sanitizer, "tsan_interface_exported", lambda *_: False)

    with pytest.raises(RuntimeError, match="__tsan_func_entry"):
        setup_sanitizer.configure_env("tsan", "")


@pytest.mark.usefixtures("repo")
def test_env_fails_off_linux(monkeypatch):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "darwin")

    with pytest.raises(RuntimeError, match="Linux Python toolchain"):
        setup_sanitizer.configure_env("tsan", "")


def test_env_fails_without_the_interpreter(monkeypatch, tmp_path):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")
    monkeypatch.setattr(setup_sanitizer, "TSAN_PYTHON", str(tmp_path / "missing" / "python3"))

    with pytest.raises(RuntimeError, match="Sanitizer interpreter not found"):
        setup_sanitizer.configure_env("tsan", "")


def test_tsan_interface_exported_skips_without_nm(monkeypatch):
    monkeypatch.setattr(setup_sanitizer.shutil, "which", lambda *_: None)

    assert setup_sanitizer.tsan_interface_exported("python3") is True


def test_tsan_interface_exported_true(monkeypatch, tmp_path):
    binary = tmp_path / "python3"
    binary.write_bytes(b"")
    monkeypatch.setattr(
        setup_sanitizer.subprocess,
        "run",
        lambda *args, **kwargs: type("R", (), {"stdout": "T __tsan_func_entry\n"})(),
    )

    assert setup_sanitizer.tsan_interface_exported(str(binary)) is True


def test_tsan_interface_exported_false(monkeypatch, tmp_path):
    binary = tmp_path / "python3"
    binary.write_bytes(b"")
    monkeypatch.setattr(
        setup_sanitizer.subprocess,
        "run",
        lambda *args, **kwargs: type("R", (), {"stdout": ""})(),
    )

    assert setup_sanitizer.tsan_interface_exported(str(binary)) is False

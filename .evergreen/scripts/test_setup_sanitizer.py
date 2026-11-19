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
    interpreter.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "-c" ]; then\n'
        "  echo 'Clang 14.0.6 (Red Hat 14.0.6-1.module+el8.7.0+15816+ec020e8f)'\n"
        "else\n"
        "  echo 'clang -pthread'\n"
        "fi\n"
    )
    interpreter.chmod(0o755)
    monkeypatch.setattr(setup_sanitizer, "ROOT", root)
    monkeypatch.setattr(setup_sanitizer, "TSAN_PYTHON", str(interpreter))
    monkeypatch.setattr(setup_sanitizer, "SANITIZER_PYTHON", str(interpreter))
    monkeypatch.setattr(setup_sanitizer, "clang_candidates", lambda: ["/usr/bin/clang"])
    monkeypatch.setattr(setup_sanitizer, "compiler_clang_version", lambda *_: "14.0.6")
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
    assert lines["CC"] == "/usr/bin/clang"
    assert lines["CXX"] == "/usr/bin/clang++"
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
    assert lines["CC"] == "/usr/bin/clang"
    assert lines["CXX"] == "/usr/bin/clang++"
    assert (
        lines["CFLAGS"]
        == "-fsanitize=address,undefined -fno-sanitize=function -fno-omit-frame-pointer -g -O1"
    )
    assert lines["LDFLAGS"] == "-fsanitize=address,undefined"
    assert lines["ASAN_OPTIONS"] == "detect_leaks=0"
    assert lines["UBSAN_OPTIONS"] == "halt_on_error=1:print_stacktrace=1"
    assert lines["UV_NO_CACHE"] == "1"
    assert "TSAN_OPTIONS" not in lines
    assert test_args.startswith("-s ")


@pytest.mark.usefixtures("repo")
def test_tsan_env_fails_on_compiler_mismatch(monkeypatch):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")
    monkeypatch.setattr(setup_sanitizer, "tsan_interface_exported", lambda *_: True)
    monkeypatch.setattr(
        setup_sanitizer,
        "clang_candidates",
        lambda: ["/opt/mongodbtoolchain/v5/bin/clang", "/usr/bin/clang"],
    )
    versions = {
        "/opt/mongodbtoolchain/v5/bin/clang": "19.1.7",
        "/usr/bin/clang": "22.1.8",
    }
    monkeypatch.setattr(setup_sanitizer, "compiler_clang_version", lambda cc: versions[cc])

    with pytest.raises(RuntimeError, match=r"14\.0\.6.*19\.1\.7.*22\.1\.8"):
        setup_sanitizer.configure_env("tsan", "")


def test_find_matching_clang_picks_the_interpreters_version(monkeypatch):
    candidates = [
        "/opt/mongodbtoolchain/v5/bin/clang",
        "/opt/rh/llvm-toolset-14/root/usr/bin/clang",
    ]
    monkeypatch.setattr(setup_sanitizer, "clang_candidates", lambda: candidates)
    versions = dict(zip(candidates, ["19.1.7", "14.0.6"], strict=False))
    monkeypatch.setattr(setup_sanitizer, "compiler_clang_version", lambda cc: versions[cc])

    assert setup_sanitizer.find_matching_clang("14.0.6") == (
        "/opt/rh/llvm-toolset-14/root/usr/bin/clang",
        "14.0.6",
        True,
    )


def test_find_matching_clang_falls_back_to_the_newest_older_clang(monkeypatch):
    """No exact match: the newest clang OLDER than the interpreter's is the
    stopgap — a newer clang emits references the runtime does not export
    (the v5 clang 19.1.7 and the system clang 22.1.8 both fail on hosts
    whose interpreters were built with clang 14.0.6).
    """
    candidates = [
        "/opt/mongodbtoolchain/v4/bin/clang",
        "/opt/mongodbtoolchain/v5/bin/clang",
        "/usr/bin/clang",
    ]
    monkeypatch.setattr(setup_sanitizer, "clang_candidates", lambda: candidates)
    versions = dict(zip(candidates, ["12.0.1", "19.1.7", "22.1.8"], strict=False))
    monkeypatch.setattr(setup_sanitizer, "compiler_clang_version", lambda cc: versions[cc])

    assert setup_sanitizer.find_matching_clang("14.0.6") == (
        "/opt/mongodbtoolchain/v4/bin/clang",
        "12.0.1",
        False,
    )


def test_find_matching_clang_lists_the_probed_candidates_on_failure(monkeypatch):
    candidates = ["/opt/mongodbtoolchain/v5/bin/clang", "/usr/bin/clang"]
    monkeypatch.setattr(setup_sanitizer, "clang_candidates", lambda: candidates)
    versions = dict(zip(candidates, ["19.1.7", "22.1.8"], strict=False))
    monkeypatch.setattr(setup_sanitizer, "compiler_clang_version", lambda cc: versions[cc])

    with pytest.raises(RuntimeError, match=r"14\.0\.6.*19\.1\.7.*22\.1\.8"):
        setup_sanitizer.find_matching_clang("14.0.6")


@pytest.mark.usefixtures("repo")
def test_tsan_env_proceeds_without_a_discoverable_compiler_version(monkeypatch, capsys):
    monkeypatch.setattr(setup_sanitizer, "PLATFORM", "linux")
    monkeypatch.setattr(setup_sanitizer, "tsan_interface_exported", lambda *_: True)
    monkeypatch.setattr(setup_sanitizer, "interpreter_clang_version", lambda *_: None)
    monkeypatch.setattr(setup_sanitizer, "compiler_clang_version", lambda *_: None)

    setup_sanitizer.configure_env("tsan", "")

    assert "not discoverable" in capsys.readouterr().out


def test_interpreter_clang_version_parses_the_compiler_banner(tmp_path, monkeypatch):
    binary = tmp_path / "python3"
    binary.write_bytes(b"")
    monkeypatch.setattr(
        setup_sanitizer.subprocess,
        "run",
        lambda *args, **kwargs: type(
            "R", (), {"stdout": "Clang 14.0.6 (Red Hat 14.0.6-1.module+el8.7.0+15816+ec020e8f)\n"}
        )(),
    )

    assert setup_sanitizer.interpreter_clang_version(str(binary)) == "14.0.6"


def test_interpreter_clang_version_skips_a_non_clang_interpreter(tmp_path, monkeypatch):
    binary = tmp_path / "python3"
    binary.write_bytes(b"")
    monkeypatch.setattr(
        setup_sanitizer.subprocess,
        "run",
        lambda *args, **kwargs: type("R", (), {"stdout": "GCC 12.1.1\n"})(),
    )

    assert setup_sanitizer.interpreter_clang_version(str(binary)) is None


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

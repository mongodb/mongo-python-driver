"""Tests for .evergreen/scripts/setup_uv.py and .evergreen/scripts/setup-uv-python.sh.

Run with: python -m pytest .evergreen/scripts/test_setup_uv.py

The bash tests drive setup-uv-python.sh with a stub `uv` whose `python find` /
`python install` fail the way CI's uv does on hosts like rhel8.10 (an
un-inspectable /usr/bin/python3 and no cpython-3.15 download), plus a fake
toolchain root selected through PYMONGO_TOOLCHAIN_ROOT.
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
SETUP_UV = HERE / "setup_uv.py"
SETUP_UV_PYTHON_SH = HERE / "setup-uv-python.sh"

PIN = "==0.12.12"

UNPINNED_PYPROJECT = '[project]\nname = "test"\n'
PINNED_PYPROJECT = f'[project]\nname = "test"\n\n[tool.uv]\nrequired-version = "{PIN}"\n'

# A stub `uv`: records each invocation to $UV_STUB_LOG, reports
# $UV_STUB_VERSION for --version, fails `python find`/`python install` the way
# CI's uv does on rhel8.10 (except a ">=3.9" find, which reports
# $UV_STUB_FALLBACK when set), and emulates `uv tool install ... --from <req>`
# by writing itself into $UV_TOOL_BIN_DIR.
STUB_UV = """\
#!/bin/bash
[ -n "${UV_STUB_LOG:-}" ] && printf '%s\\n' "$*" >> "$UV_STUB_LOG"
case "${1:-}" in
  --version|version)
    printf 'uv %s (x86_64-unknown-linux-gnu)\\n' "${UV_STUB_VERSION:-0.12.24}"
    exit 0 ;;
esac
case "$1 $2" in
  "python find")
    if [ "${3:-}" = ">=3.9" ] && [ -n "${UV_STUB_FALLBACK:-}" ]; then
      printf '%s\\n' "$UV_STUB_FALLBACK"
      exit 0
    fi
    printf 'error: Failed to inspect Python interpreter from search path at `/usr/bin/python3`\\n' >&2
    exit 1 ;;
  "python install")
    printf 'error: No download found for request: cpython-3.15-linux-x86_64-gnu\\n' >&2
    exit 1 ;;
  "tool install")
    req=""
    prev=""
    for arg in "$@"; do
      if [ "$prev" = "--from" ]; then req="$arg"; fi
      prev="$arg"
    done
    [ -n "${UV_STUB_LOG:-}" ] && printf 'installed-from=%s\\n' "$req" >> "$UV_STUB_LOG"
    mkdir -p "${UV_TOOL_BIN_DIR:?}"
    cp "$0" "${UV_TOOL_BIN_DIR}/uv"
    exit 0 ;;
esac
printf 'stub uv: unhandled invocation: %s\\n' "$*" >&2
exit 64
"""


def make_executable(path: Path) -> Path:
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return path


def write_stub_uv(bin_dir: Path) -> Path:
    bin_dir.mkdir(parents=True, exist_ok=True)
    stub = bin_dir / "uv"
    stub.write_text(STUB_UV)
    return make_executable(stub)


def make_toolchain(root: Path, version: str) -> Path:
    """Create a fake toolchain interpreter, e.g. <root>/3.15/bin/python3.15."""
    interpreter = root / version / "bin" / f"python{version}"
    interpreter.parent.mkdir(parents=True, exist_ok=True)
    interpreter.write_text("#!/bin/sh\nexit 0\n")
    return make_executable(interpreter)


def stub_calls(log: Path) -> list[str]:
    if not log.exists():
        return []
    return [line for line in log.read_text().splitlines() if not line.startswith("installed-from=")]


# --- setup-uv-python.sh ------------------------------------------------------

# The whole module is unix-only: the bash tests need a unix shell, and the
# setup_uv.py tests use bash-script uv stubs and colon-separated PATH values.
pytestmark = pytest.mark.skipif(
    sys.platform in ("win32", "cygwin") or shutil.which("bash") is None,
    reason="needs a unix shell",
)


def run_setup_uv_python(tmp_path: Path, extra_env: dict) -> subprocess.CompletedProcess:
    """Source a copy of setup-uv-python.sh in a temporary project tree.

    The copy's HERE/ROOT resolve inside the tree, so the script sources the
    tree's env.sh (if any) and probes the tree's .venv, never the checkout's.
    Reports rc and the exported UV_PYTHON / search path / preference.
    """
    scripts = tmp_path / "proj" / ".evergreen" / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    script_copy = scripts / "setup-uv-python.sh"
    shutil.copy(SETUP_UV_PYTHON_SH, script_copy)
    env = {
        "HOME": str(tmp_path),
        "CI": "true",
    }
    env.update(extra_env)
    return subprocess.run(  # noqa: S603
        [
            "/bin/bash",
            "-c",
            (
                f'. "{script_copy}"; '
                'printf "\\nUV_PYTHON=%s" "$UV_PYTHON"; '
                'printf "\\nSEARCH_PATH=%s" "${UV_PYTHON_SEARCH_PATH-}"; '
                'printf "\\nPREFERENCE=%s" "${UV_PYTHON_PREFERENCE-}"; '
                'printf "\\nTOOL_BIN_DIR=%s" "${UV_TOOL_BIN_DIR-}"'
            ),
        ],
        check=False,
        env=env,
        capture_output=True,
        text=True,
    )


@pytest.fixture
def stub_env(tmp_path):
    """A stub uv bin dir and its invocation log; returns (log, env-overrides)."""
    log = tmp_path / "uv-stub.log"
    write_stub_uv(tmp_path / "stub-bin")
    return log, {"PATH": f"{tmp_path / 'stub-bin'}:/usr/bin:/bin", "UV_STUB_LOG": str(log)}


def test_toolchain_python_is_used_by_path(tmp_path, stub_env):
    """CI: an exact toolchain match wins by explicit path, without uv find/install."""
    log, env = stub_env
    interpreter = make_toolchain(tmp_path / "toolchain", "3.15")
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "UV_PYTHON": "3.15",
            "PYMONGO_TOOLCHAIN_ROOT": str(tmp_path / "toolchain"),
        },
    )
    assert proc.returncode == 0, proc.stderr
    assert f"UV_PYTHON={interpreter}" in proc.stdout
    assert f"SEARCH_PATH={interpreter.parent}" in proc.stdout
    assert "PREFERENCE=system" in proc.stdout
    # uv's discovery is bypassed entirely: no find, no install.
    assert stub_calls(log) == []


def test_free_threaded_toolchain_python_is_used_by_path(tmp_path, stub_env):
    """CI: the same bypass applies to free-threaded toolchain versions."""
    log, env = stub_env
    interpreter = make_toolchain(tmp_path / "toolchain", "3.15t")
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "UV_PYTHON": "3.15t",
            "PYMONGO_TOOLCHAIN_ROOT": str(tmp_path / "toolchain"),
        },
    )
    assert proc.returncode == 0, proc.stderr
    assert f"UV_PYTHON={interpreter}" in proc.stdout
    assert stub_calls(log) == []


def test_toolchain_falls_through_when_interpreter_missing(tmp_path, stub_env):
    """CI: a toolchain dir without the requested version keeps using uv find/install."""
    log, env = stub_env
    make_toolchain(tmp_path / "toolchain", "3.14")
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "UV_PYTHON": "3.15",
            "PYMONGO_TOOLCHAIN_ROOT": str(tmp_path / "toolchain"),
        },
    )
    assert proc.returncode == 1
    assert "could not find or install Python" in proc.stderr
    assert stub_calls(log) == ["python find 3.15", "python install 3.15"]


def test_task_preference_disables_bypass(tmp_path, stub_env):
    """A task-set UV_PYTHON_PREFERENCE (PYTHON-6135) keeps the pre-bypass behavior."""
    log, env = stub_env
    make_toolchain(tmp_path / "toolchain", "3.15")
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "UV_PYTHON": "3.15",
            "UV_PYTHON_PREFERENCE": "only-managed",
            "PYMONGO_TOOLCHAIN_ROOT": str(tmp_path / "toolchain"),
        },
    )
    assert proc.returncode == 1
    assert "could not find or install Python" in proc.stderr
    # No bypass: the script failed in uv's find/install (rc 0 would mean the
    # explicit-path branch ran, ignoring the task's preference).
    assert stub_calls(log) == ["python find 3.15", "python install 3.15"]


def test_ci_fails_loudly_without_toolchain(tmp_path, stub_env):
    """CI without the toolchain keeps failing after find and install both fail."""
    log, env = stub_env
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "UV_PYTHON": "3.15",
            # An empty toolchain root: no version can match it.
            "PYMONGO_TOOLCHAIN_ROOT": str(tmp_path / "no-toolchain"),
        },
    )
    assert proc.returncode == 1
    assert "could not find or install Python" in proc.stderr
    # find was attempted, then install.
    assert stub_calls(log) == ["python find 3.15", "python install 3.15"]


def test_local_fallback_without_toolchain(tmp_path, stub_env):
    """Local (no CI) hosts fall back to any interpreter >=3.9 when find/install fail."""
    _, env = stub_env
    fallback = make_toolchain(tmp_path / "elsewhere", "3.11")
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "UV_PYTHON": "3.15",
            "CI": "",
            "UV_STUB_FALLBACK": str(fallback),
            # An empty toolchain root: no version can match it.
            "PYMONGO_TOOLCHAIN_ROOT": str(tmp_path / "no-toolchain"),
        },
    )
    assert proc.returncode == 0, proc.stderr
    assert f"UV_PYTHON={fallback}" in proc.stdout


def test_explicit_interpreter_path_skips_probing(tmp_path, stub_env):
    """An explicit interpreter path in UV_PYTHON is used as-is."""
    log, env = stub_env
    interpreter = make_toolchain(tmp_path / "elsewhere", "3.12")
    proc = run_setup_uv_python(tmp_path, extra_env={**env, "UV_PYTHON": str(interpreter)})
    assert proc.returncode == 0, proc.stderr
    assert f"UV_PYTHON={interpreter}" in proc.stdout
    assert stub_calls(log) == []


def toolchain_bin_dir(script_copy: Path, sentinel: Path, version: str) -> str:
    """Call the script's _toolchain_bin_dir helper and return the printed path."""
    proc = subprocess.run(  # noqa: S603
        [
            "/bin/bash",
            "-c",
            f'UV_PYTHON="{sentinel}" . "{script_copy}"; _toolchain_bin_dir "{version}"',
        ],
        check=False,
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout.strip()


def test_toolchain_paths_preserved_without_override(tmp_path):
    """Without the test override, the platform toolchain paths are preserved as-is."""
    scripts = tmp_path / "proj" / ".evergreen" / "scripts"
    scripts.mkdir(parents=True)
    script_copy = scripts / "setup-uv-python.sh"
    shutil.copy(SETUP_UV_PYTHON_SH, script_copy)
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("#!/bin/sh\nexit 0\n")
    make_executable(sentinel)
    if sys.platform == "darwin":
        expected = {
            "3.15": "/Library/Frameworks/Python.Framework/Versions/3.15/bin",
            "3.15t": "/Library/Frameworks/PythonT.Framework/Versions/3.15/bin",
        }
    else:
        expected = {
            "3.15": "/opt/python/3.15/bin",
            "3.15t": "/opt/python/3.15t/bin",
        }
    for version, path in expected.items():
        assert toolchain_bin_dir(script_copy, sentinel, version) == path


def test_toolchain_override_keeps_version_layout(tmp_path):
    """With the test override, the <version>/bin layout under it is kept."""
    scripts = tmp_path / "proj" / ".evergreen" / "scripts"
    scripts.mkdir(parents=True)
    script_copy = scripts / "setup-uv-python.sh"
    shutil.copy(SETUP_UV_PYTHON_SH, script_copy)
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("#!/bin/sh\nexit 0\n")
    make_executable(sentinel)
    proc = subprocess.run(  # noqa: S603
        [
            "/bin/bash",
            "-c",
            (
                f'export UV_PYTHON="{sentinel}" '
                f'PYMONGO_TOOLCHAIN_ROOT="{tmp_path / "root"}"; '
                f'. "{script_copy}"; _toolchain_bin_dir "3.15"'
            ),
        ],
        check=False,
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip() == str(tmp_path / "root" / "3.15" / "bin")


def test_script_sources_env_sh_from_its_tree(tmp_path, stub_env):
    """The script sources env.sh from the copy's tree, not the checkout's."""
    _, env = stub_env
    fallback = make_toolchain(tmp_path / "elsewhere", "3.11")
    proc = run_setup_uv_python(
        tmp_path,
        extra_env={
            **env,
            "CI": "",
            "UV_STUB_FALLBACK": str(fallback),
        },
    )
    # The runner's tree has no env.sh, so the checkout's env.sh must not have
    # leaked: it exports UV_TOOL_BIN_DIR, which stays unset here.
    assert proc.returncode == 0, proc.stderr
    # The last printf has no trailing newline.
    assert proc.stdout.rstrip("\n").endswith("TOOL_BIN_DIR=")


# --- setup_uv.py ------------------------------------------------------------


@pytest.fixture
def project(tmp_path):
    """A fake project tree: a pyproject.toml plus a copy of setup_uv.py."""
    proj = tmp_path / "proj"
    scripts = proj / ".evergreen" / "scripts"
    scripts.mkdir(parents=True)
    shutil.copy(SETUP_UV, scripts / "setup_uv.py")
    (proj / "pyproject.toml").write_text(UNPINNED_PYPROJECT)
    return proj


def run_setup_uv(project: Path, args: list[str], env: dict) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603
        [sys.executable, str(project / ".evergreen" / "scripts" / "setup_uv.py"), *args],
        check=False,
        env={**os.environ, **env},
        capture_output=True,
        text=True,
    )


def test_check_unpinned_requires_project_uv(tmp_path, project):
    """Unpinned --check passes only for the uv setup installed, not any uv on PATH."""
    bin_dir = tmp_path / "project-bin"
    foreign = write_stub_uv(tmp_path / "foreign-bin")
    bin_dir.mkdir()
    write_stub_uv(bin_dir)
    env = {"PATH": f"{foreign.parent}:{bin_dir}:/usr/bin:/bin", "UV_TOOL_BIN_DIR": str(bin_dir)}
    assert run_setup_uv(project, ["--check"], env).returncode == 1


def test_check_unpinned_accepts_project_uv(tmp_path, project):
    """Unpinned --check passes when the uv on PATH is the project-installed one."""
    bin_dir = tmp_path / "project-bin"
    bin_dir.mkdir()
    write_stub_uv(bin_dir)
    env = {"PATH": f"{bin_dir}:/usr/bin:/bin", "UV_TOOL_BIN_DIR": str(bin_dir)}
    assert run_setup_uv(project, ["--check"], env).returncode == 0


def test_check_unpinned_without_uv_fails(tmp_path, project):
    """Unpinned --check fails when no uv is on PATH at all."""
    bin_dir = tmp_path / "project-bin"
    bin_dir.mkdir()
    env = {"PATH": "/usr/bin:/bin", "UV_TOOL_BIN_DIR": str(bin_dir)}
    assert run_setup_uv(project, ["--check"], env).returncode == 1


def test_check_pinned_requires_exact_version(tmp_path, project):
    """Pinned --check keeps comparing the uv on PATH with the pin."""
    (project / "pyproject.toml").write_text(PINNED_PYPROJECT)
    bin_dir = tmp_path / "project-bin"
    bin_dir.mkdir()
    write_stub_uv(bin_dir)
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "UV_TOOL_BIN_DIR": str(bin_dir),
        "UV_STUB_VERSION": "0.12.24",
    }
    assert run_setup_uv(project, ["--check"], env).returncode == 1
    env["UV_STUB_VERSION"] = "0.12.12"
    assert run_setup_uv(project, ["--check"], env).returncode == 0


def test_main_unpinned_installs_latest_uv_into_bin_dir(tmp_path, project):
    """Unpinned setup installs the latest uv into UV_TOOL_BIN_DIR, even with a foreign uv on PATH."""
    log = tmp_path / "log"
    foreign = write_stub_uv(tmp_path / "foreign-bin")
    bin_dir = tmp_path / "project-bin"
    env = {
        "PATH": f"{foreign.parent}:/usr/bin:/bin",
        "UV_TOOL_BIN_DIR": str(bin_dir),
        "UV_STUB_LOG": str(log),
    }
    proc = run_setup_uv(project, [], env)
    assert proc.returncode == 0, proc.stderr
    assert (bin_dir / "uv").exists()
    assert "installed-from=uv\n" in log.read_text()


def test_main_unpinned_skips_install_when_project_uv_on_path(tmp_path, project):
    """Unpinned setup does not reinstall when the project uv is already on PATH."""
    log = tmp_path / "log"
    bin_dir = tmp_path / "project-bin"
    write_stub_uv(bin_dir)
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "UV_TOOL_BIN_DIR": str(bin_dir),
        "UV_STUB_LOG": str(log),
    }
    proc = run_setup_uv(project, [], env)
    assert proc.returncode == 0, proc.stderr
    assert not log.exists() or "installed-from=" not in log.read_text()


def test_main_pinned_installs_pinned_uv(tmp_path, project):
    """Pinned setup keeps installing the pinned uv into UV_TOOL_BIN_DIR."""
    (project / "pyproject.toml").write_text(PINNED_PYPROJECT)
    log = tmp_path / "log"
    foreign = write_stub_uv(tmp_path / "foreign-bin")
    bin_dir = tmp_path / "project-bin"
    env = {
        "PATH": f"{foreign.parent}:/usr/bin:/bin",
        "UV_TOOL_BIN_DIR": str(bin_dir),
        "UV_STUB_LOG": str(log),
        "UV_STUB_VERSION": "0.12.24",
    }
    proc = run_setup_uv(project, [], env)
    assert proc.returncode == 0, proc.stderr
    assert (bin_dir / "uv").exists()
    assert f"installed-from=uv{PIN}\n" in log.read_text()


def test_main_pinned_self_overwrite_runs_from_copy(tmp_path, project):
    """When the driving uv is the binary being replaced, the install runs from a copy."""
    (project / "pyproject.toml").write_text(PINNED_PYPROJECT)
    log = tmp_path / "log"
    bin_dir = tmp_path / "project-bin"
    write_stub_uv(bin_dir)
    env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "UV_TOOL_BIN_DIR": str(bin_dir),
        "UV_STUB_LOG": str(log),
        "UV_STUB_VERSION": "0.12.12",
    }
    proc = run_setup_uv(project, [], env)
    assert proc.returncode == 0, proc.stderr
    assert (bin_dir / "uv").exists()
    assert f"installed-from=uv{PIN}\n" in log.read_text()

#!/usr/bin/env python3
"""Bootstrap uv/just for the test environment.

install-dependencies.sh probes with `setup_uv.py --check`, sources ensure-uv.sh
(which finds or installs uv), then runs this script for the rest. Only the
standard library is used.

The project always controls a uv in UV_TOOL_BIN_DIR: the pinned version when
[tool.uv] required-version is set in pyproject.toml, the latest uv otherwise.
The uv a host or the vendored drivers-evergreen-tools submodule puts on PATH
may be invisible to later steps (it is not in the project's bin dir) and may
lack behavior the project relies on, so it is never accepted as-is.

This runs under the host's system python3, which may be older than our minimum supported version.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
ENV_SH = HERE / "env.sh"
ASTRAL_INSTALL_URL = "https://astral.sh/uv/install.sh"


def required_uv_pin() -> str:
    """Return [tool.uv] required-version, e.g. '==1.2.3' ('' if absent)."""
    pattern = re.compile(r"required-version\s*=\s*['\"]?([^'\"\s]+)['\"]?")
    in_uv = False
    for line in (ROOT / "pyproject.toml").read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("["):
            in_uv = stripped == "[tool.uv]"
            continue
        if in_uv:
            match = pattern.search(stripped)
            if match:
                return match.group(1)
    return ""


def _add_path(dir_: str) -> None:
    os.environ["PATH"] = dir_ + os.pathsep + os.environ.get("PATH", "")


def _install_uv_astral() -> None:
    """Install uv from astral when ensure-uv.sh was not available.

    UV_TOOL_BIN_DIR is set (in native form on Windows) by install-dependencies.sh.
    """
    print("uv not found; installing the latest uv from astral...")
    env = {
        **os.environ,
        "UV_INSTALL_DIR": os.environ["UV_TOOL_BIN_DIR"],
        "INSTALLER_NO_MODIFY_PATH": "1",
    }
    curl = shutil.which("curl")
    sh = shutil.which("sh")
    proc = subprocess.run(  # noqa: S603
        [curl, "-LsSf", ASTRAL_INSTALL_URL], stdout=subprocess.PIPE, env=env, check=True
    )
    subprocess.run([sh], input=proc.stdout, env=env, check=True)  # noqa: S603
    _add_path(os.environ["UV_TOOL_BIN_DIR"])


def _install_uv(pin: str) -> None:
    """Install uv via uv tool install --force into UV_TOOL_BIN_DIR.

    Installs the pinned version when pin is set ("==0.12.12"), the latest uv
    otherwise (pin ""). UV_TOOL_BIN_DIR and UV_TOOL_DIR are inherited from the
    environment (set by install-dependencies.sh / ensure-uv.sh, in native form
    on Windows). uv tool install writes the binary into UV_TOOL_BIN_DIR and the
    tool venv into UV_TOOL_DIR; --force lets it overwrite an existing install.

    Windows will not overwrite a running executable, so when the uv driving
    the install is itself the binary about to be overwritten, run the install
    from a copy of it.
    """
    uv_path = shutil.which("uv")
    bin_dir = os.environ["UV_TOOL_BIN_DIR"]
    exe = "uv.exe" if os.name == "nt" else "uv"
    overwrite_self = uv_path is not None and _same_path(uv_path, str(Path(bin_dir, exe)))
    tool = uv_path
    tmp_uv = None
    if overwrite_self:
        tmp_uv = Path(tempfile.mkdtemp()) / Path(uv_path).name
        shutil.copy2(uv_path, tmp_uv)
        tool = str(tmp_uv)
    try:
        subprocess.run(  # noqa: S603
            [
                tool,
                "tool",
                "install",
                "--no-config",
                "-q",
                "--force",
                "--from",
                f"uv{pin}",
                "uv",
            ],
            check=True,
        )
    finally:
        if tmp_uv:
            shutil.rmtree(tmp_uv.parent, ignore_errors=True)


# The UV_* env vars this bootstrap (or its caller, ensure-uv.sh) manages and
# may persist into env.sh. Deliberately narrow: other UV_* vars (e.g.
# UV_PUBLISH_TOKEN, UV_INDEX_..._PASSWORD) may hold credentials and must
# never be written out.
PERSISTED_UV_VARS = ("UV_CACHE_DIR", "UV_PYTHON_INSTALL_DIR", "UV_TOOL_BIN_DIR", "UV_TOOL_DIR")


def _write_env() -> None:
    """Write the bootstrap's UV_* env vars into env.sh, replacing existing UV_* entries."""
    values = {k: v for k, v in os.environ.items() if k in PERSISTED_UV_VARS}
    if not values:
        return
    existing = ENV_SH.read_text() if ENV_SH.exists() else ""
    keep = []
    for line in existing.splitlines():
        stripped = line.strip()
        if stripped.startswith("export "):
            var, _, _ = stripped[len("export ") :].partition("=")
            if var.startswith("UV_"):
                continue
        keep.append(line)
    keep.append("")
    # shlex.quote emits shell-safe values (single-quoted, escaping embedded
    # quotes) so a value cannot break out of or inject into the assignment.
    keep.extend(f"export {name}={shlex.quote(value)}" for name, value in sorted(values.items()))
    # Write LF bytes directly: on Windows text mode translates \n to \r\n, which
    # breaks bash sourcing env.sh, and `newline=` isn't available on all Pythons.
    ENV_SH.write_bytes(("\n".join(keep) + "\n").encode())


def _same_path(a: str, b: str) -> bool:
    """Compare two path strings, tolerating forms realpath cannot unify.

    On cygwin, which() may report a /cygdrive form while UV_TOOL_BIN_DIR is
    native ("C:/..."), and realpath cannot translate between the two forms.
    """
    try:
        if os.path.realpath(a) == os.path.realpath(b):
            return True
    except OSError:
        pass
    return os.path.normcase(os.path.normpath(a)) == os.path.normcase(os.path.normpath(b))


def _project_uv_installed() -> bool:
    """Return True when the uv on PATH is the one this setup put in UV_TOOL_BIN_DIR."""
    uv_path = shutil.which("uv")
    bin_dir = os.environ.get("UV_TOOL_BIN_DIR")
    if not uv_path or not bin_dir:
        return False
    exe = "uv.exe" if os.name == "nt" else "uv"
    return _same_path(uv_path, str(Path(bin_dir, exe)))


def _required_uv_installed() -> bool:
    """Return True when the uv on PATH already satisfies the setup requirement.

    With a required-version pin, the uv on PATH must match it exactly; without
    a pin, it must be the project-controlled uv in UV_TOOL_BIN_DIR. A uv from
    elsewhere on PATH (a host install, the vendored submodule) does not count:
    later steps may not see it and its behavior is not the project's choice.
    """
    pin = required_uv_pin()
    if not pin:
        return _project_uv_installed()
    if not pin.startswith("=="):
        # Only exact pins are checked here; anything else falls back to setup.
        return False
    uv_path = shutil.which("uv")
    if uv_path is None:
        return False
    proc = subprocess.run(  # noqa: S603
        [uv_path, "--version"], stdout=subprocess.PIPE, check=True
    )
    # Output looks like "uv 0.12.12 (aarch64-unknown-linux-gnu)".
    return proc.stdout.decode().split()[1] == pin[len("==") :]


def main() -> int:
    # Side-effect-free probe for install-dependencies.sh: a full `uv sync`
    # there would download Python and install dependencies just to decide
    # whether setup is needed.
    if "--check" in sys.argv[1:]:
        return 0 if _required_uv_installed() else 1

    bin_dir = os.environ["UV_TOOL_BIN_DIR"]
    Path(bin_dir).mkdir(parents=True, exist_ok=True)
    _add_path(bin_dir)

    # Bootstrap a uv from astral if ensure-uv.sh didn't run and uv isn't on PATH.
    if shutil.which("uv") is None:
        _install_uv_astral()

    uv_pin = required_uv_pin()
    if uv_pin:
        _install_uv(uv_pin)
    elif not _project_uv_installed():
        # Unpinned: still install the latest uv into UV_TOOL_BIN_DIR, so the
        # project controls the uv that later steps find on PATH. A uv found
        # elsewhere (the vendored submodule's, the host's) may be missing from
        # those steps' PATH entirely, and may lack behavior the project relies
        # on, such as provisioning newer CPythons.
        _install_uv("")

    _write_env()
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Bootstrap the pinned uv/just for the test environment.

install-dependencies.sh bails out if the pinned uv is already on PATH, sources
ensure-uv.sh (which finds or installs uv), then runs this script for the rest.
Only the standard library is used.
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
    """Return [tool.uv] required-version, e.g. '==0.12.12' ('' if absent)."""
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
        [curl, "-LsSf", ASTRAL_INSTALL_URL], capture_output=True, env=env, check=True
    )
    subprocess.run([sh], input=proc.stdout, env=env, check=True)  # noqa: S603
    _add_path(os.environ["UV_TOOL_BIN_DIR"])


def _pin_uv(uv_pin: str) -> None:
    """Install the pinned uv via uv tool install --force.

    UV_TOOL_BIN_DIR and UV_TOOL_DIR are inherited from the environment (set by
    install-dependencies.sh / ensure-uv.sh, in native form on Windows). uv tool
    install writes the binary into UV_TOOL_BIN_DIR and the tool venv into
    UV_TOOL_DIR; --force lets it overwrite an existing install.

    Windows will not overwrite a running executable, so when the current uv is
    already the target bin dir, run the install from a copy of the binary.
    """
    uv_path = shutil.which("uv")
    tool = uv_path
    tmp_uv = None
    if os.name == "nt":
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
                f"uv{uv_pin}",
                "uv",
            ],
            check=True,
        )
    finally:
        if tmp_uv:
            shutil.rmtree(tmp_uv.parent, ignore_errors=True)


# The UV_* env vars this bootstrap manages and may persist into env.sh.
# Deliberately narrow: other UV_* vars (e.g. UV_PUBLISH_TOKEN,
# UV_INDEX_..._PASSWORD) may hold credentials and must never be written out.
PERSISTED_UV_VARS = ("UV_CACHE_DIR", "UV_TOOL_BIN_DIR", "UV_TOOL_DIR")


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


def main() -> int:
    bin_dir = os.environ["UV_TOOL_BIN_DIR"]
    Path(bin_dir).mkdir(parents=True, exist_ok=True)
    _add_path(bin_dir)

    # Bootstrap a uv from astral if ensure-uv.sh didn't run and uv isn't on PATH.
    if shutil.which("uv") is None:
        _install_uv_astral()

    uv_pin = required_uv_pin()
    if uv_pin:
        _pin_uv(uv_pin)

    _write_env()
    return 0


if __name__ == "__main__":
    sys.exit(main())

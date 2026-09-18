#!/bin/bash
# Install the necessary dependencies.
set -euo pipefail

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# PYMONGO_BIN_DIR is set by setup-system.sh/env.sh (or setup-dev-env.sh); default
# it for robustness. UV_TOOL_BIN_DIR is uv's name for the same dir (setup-uv.py
# reads both). UV_TOOL_DIR is left to ensure_uv.sh.
export PYMONGO_BIN_DIR="${PYMONGO_BIN_DIR:-$HOME/.local/bin}"
export UV_TOOL_BIN_DIR="${UV_TOOL_BIN_DIR:-$PYMONGO_BIN_DIR}"
# uv is a native Windows binary: give it a Windows path on cygwin.
if [ "Windows_NT" = "${OS:-}" ]; then
  _uv_tool_bin="$(cygpath -m "$PYMONGO_BIN_DIR")"
  export UV_TOOL_BIN_DIR="$_uv_tool_bin"
fi

# If uv is on PATH, check it via `uv sync`, which fails fast if it is not the
# pinned version (from pyproject.toml's [tool.uv] required-version). If that
# succeeds, the environment is already correct and there is nothing to set up;
# otherwise fall through to the setup below.
#
# On CI we also require UV_CACHE_DIR to be set: ensure_uv.sh scopes uv's cache
# to a task-local dir, so an unset UV_CACHE_DIR means the uv setup has not run
# yet in this task and we must do the setup phase.
_need_setup=1
if command -v uv >/dev/null 2>&1 && uv sync >/dev/null 2>&1; then
  if [ "${CI:-}" != "true" ] || [ -n "${UV_CACHE_DIR:-}" ]; then
    echo "uv is already set up; skipping uv setup."
    _need_setup=0
  fi
fi

# Set up uv if needed.
if [ "$_need_setup" = "1" ]; then
  # ensure-uv.sh (drivers-evergreen-tools) finds or installs uv and scopes its env.
  if [ -n "${DRIVERS_TOOLS:-}" ] && [ -f "$DRIVERS_TOOLS/.evergreen/ensure-uv.sh" ]; then
    . "$DRIVERS_TOOLS/.evergreen/ensure-uv.sh"
    ensure_uv || exit 1
  fi

  # Do the uv setup (bin dir, pinning, env.sh). Uses the toolchain python3
  # (added to PATH by configure-env.sh) so no project .venv is created here,
  # and no required-version check is triggered. On Windows the script path must
  # be a native Windows path for python3.
  _uv_setup_script="$HERE/setup-uv.py"
  if [ "Windows_NT" = "${OS:-}" ]; then
    _uv_setup_script="$(cygpath -m "$_uv_setup_script")"
  fi
  python3 "$_uv_setup_script"

  # Re-source env.sh so the values setup-uv.py wrote are available.
  if [ -f $HERE/env.sh ]; then
    . $HERE/env.sh
  fi
fi

# Make just available. It has no version constraint, so if it is already on PATH
# there is nothing to do; otherwise install it into the bin dir via uv.
if ! command -v just >/dev/null 2>&1; then
  uv tool install --no-config rust-just
fi

popd > /dev/null

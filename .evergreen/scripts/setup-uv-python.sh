#!/bin/bash
# Choose the Python that uv uses, and make sure uv can provide it.
#
# Input:
#   UV_PYTHON - a version ("3.11", "3.14t"), an implementation ("pypy3.11"),
#               or an interpreter path ("/usr/bin/python3.11").
#
# Exports:
#   UV_PYTHON_SEARCH_PATH - the Python toolchain bin dir for the request, so uv
#                           uses the toolchain Python instead of downloading one.
#   UV_PYTHON_PREFERENCE  - "system" so the toolchain wins over managed installs.
#                           A preference set by the task is left alone.
#   UV_PYTHON             - the Python interpreter uv uses; defaults to CPython
#                           3.11 when the task does not set one.
set -euo pipefail

HERE=$(dirname "${BASH_SOURCE:-$0}")
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname "$HERE")")

# A .venv whose interpreter was removed breaks every `uv python find` and
# `uv sync`. It is a build artifact, so remove it and let `uv sync` recreate it.
# A removed base interpreter leaves a dangling symlink on Linux/macOS and a
# launcher that will not start on Windows, so probe by running the interpreter.
for _py in "$ROOT/.venv/bin/python3" "$ROOT/.venv/bin/python" "$ROOT/.venv/Scripts/python.exe"; do
  if [ -e "$_py" ] || [ -L "$_py" ]; then
    if ! "$_py" -c 'import sys' > /dev/null 2>&1; then
      echo "Removing broken .venv (its interpreter will not start); uv sync will recreate it."
      rm -rf "$ROOT/.venv"
    fi
    break
  fi
done
unset _py

# Source the env files to pick up common variables.
if [ -f "$HERE/env.sh" ]; then
  . "$HERE/env.sh"
fi

# Get variables defined in test-env.sh.
if [ -f "$HERE/test-env.sh" ]; then
  . "$HERE/test-env.sh"
fi

# Default to a known-good Python so behavior is deterministic when a task does
# not pin one. Selecting the uv binary is separate and driven by the required
# uv version in pyproject.toml.
export UV_PYTHON="${UV_PYTHON:-3.11}"

# Print the Python toolchain bin dir for a version like "3.11" or "3.14t".
function _toolchain_dir() {
  local version="$1" dir
  # Only plain CPython versions live in the toolchain.
  [[ "$version" =~ ^[0-9]+\.[0-9]+t?$ ]] || return 1
  if [ "Windows_NT" = "${OS:-}" ]; then
    local winver="${version%t}"
    winver="${winver//./}"
    if [ -n "${IS_WIN32:-}" ]; then
      dir="C:/python/32/Python${winver}"
    else
      dir="C:/python/Python${winver}"
    fi
  elif [ "$(uname -s)" = "Darwin" ]; then
    if [[ "$version" == *t ]]; then
      dir="/Library/Frameworks/PythonT.Framework/Versions/${version%t}/bin"
    else
      dir="/Library/Frameworks/Python.Framework/Versions/${version}/bin"
    fi
  else
    dir="/opt/python/${version}/bin"
  fi
  [ -d "$dir" ] || return 1
  echo "$dir"
}

# Make sure uv can provide the requested Python, downloading it if needed.
# If that fails, fall back to any available Python on local hosts; on CI, fail.
function _ensure_python() {
  local request="$1" out _fallback
  if out=$(uv python find "$request" 2>&1); then
    return 0
  fi
  # Show why find failed: e.g. an interpreter uv cannot run, or none at all.
  echo "Python \"$request\" was not found on this host:" >&2
  sed 's/^/  /' <<<"$out" >&2
  echo "Asking uv to install Python \"$request\"..."
  if uv python install "$request"; then
    return 0
  fi
  echo "uv could not install Python \"$request\"; looking for another interpreter..."
  # Ignore UV_PYTHON and friends: they would steer the fallback right back to
  # the broken interpreter. Local setup only: the Evergreen agent exports
  # CI=true for every task command.
  if [ "${CI:-}" != "true" ] && \
    _fallback=$(env -u UV_PYTHON -u UV_PYTHON_SEARCH_PATH -u UV_PYTHON_PREFERENCE \
      uv python find ">=3.9" 2>/dev/null); then
    echo "Using fallback interpreter: $_fallback"
    export UV_PYTHON="$_fallback"
    return 0
  fi
  echo "ERROR: uv could not find or install Python \"$request\"." >&2
  return 1
}

_search_path=""
_skip_ensure=""

if [[ "$UV_PYTHON" == */* || "$UV_PYTHON" == *\\* ]]; then
  # An explicit interpreter path; uv uses it as-is, so probe/install is skipped.
  if [ ! -x "$UV_PYTHON" ]; then
    echo "ERROR: UV_PYTHON=$UV_PYTHON does not exist or is not executable." >&2
    exit 1
  fi
  _skip_ensure=1
elif _dir=$(_toolchain_dir "$UV_PYTHON"); then
  _search_path="$_dir"
fi

# Point uv at the toolchain Python when there is one and the task did not set
# its own preference: a task-level UV_PYTHON_PREFERENCE wins (PYTHON-6135).
# On CI the toolchain dir is already first on PATH (configure-env.sh), so this
# mainly benefits local hosts and later steps.
if [ -n "$_search_path" ] && [ -z "${UV_PYTHON_PREFERENCE:-}" ]; then
  export UV_PYTHON_SEARCH_PATH="$_search_path"
  export UV_PYTHON_PREFERENCE="system"
fi

if [ -z "$_skip_ensure" ]; then
  _ensure_python "${UV_PYTHON:-}"
fi

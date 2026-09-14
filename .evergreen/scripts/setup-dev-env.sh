#!/bin/bash
# Set up development environment.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Get variables defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# Handle the value for UV_PYTHON.
. $HERE/setup-uv-python.sh

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

# Re-source env.sh: install-dependencies.sh may have appended to it, e.g. when it
# had to install Python on an image that lacks a toolchain.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# UV_PYTHON is a bare version identifier on Linux/macOS; a matching interpreter
# is put on the path so uv resolves it.  On Windows uv cannot resolve a bare
# version without tripping over a broken Chocolatey python3 shim, so UV_PYTHON
# is set to the fetched interpreter path.  A download only happens when no
# toolchain Python matches (PYTHON_FOUND unset): try uv's managed build first,
# and fall back to python-build-standalone's latest release.
if [ -n "${UV_PYTHON:-}" ] && [ "${PYTHON_FOUND:-}" != "1" ]; then
  if ! uv python install "$UV_PYTHON" >/dev/null 2>&1; then
    _prefix="$(bash "$HERE/fetch-python.sh")" || {
      echo "Failed to obtain a Python $UV_PYTHON interpreter" >&2
      exit 1
    }

    if [ "Windows_NT" = "${OS:-}" ]; then
      if [ -x "$_prefix/bin/python3" ]; then
        _interpreter="$_prefix/bin/python3"
        _path_dir="$_prefix/bin"
      elif [ -x "$_prefix/bin/python3t" ]; then
        _interpreter="$_prefix/bin/python3t"
        _path_dir="$_prefix/bin"
      elif [ -x "$_prefix/bin/python" ]; then
        _interpreter="$_prefix/bin/python"
        _path_dir="$_prefix/bin"
      elif [ -f "$_prefix/python.exe" ]; then
        _interpreter="$_prefix/python.exe"
        _path_dir="$_prefix"
      else
        echo "No Python interpreter found under $_prefix" >&2
        exit 1
      fi
      export UV_PYTHON="$_interpreter"
    else
      if [ -f "$_prefix/python.exe" ]; then
        _path_dir="$_prefix"
      else
        _path_dir="$_prefix/bin"
      fi
    fi
    export PATH="$_path_dir:$PATH"
    # Mark the interpreter as resolved so a later setup-dev-env.sh invocation
    # (just.sh is sourced once per just command) does not re-enter the fallback
    # with UV_PYTHON now set to a path.
    export PYTHON_FOUND=1
  fi
fi

# Add the default install path to the path if needed.
if [ -z "${PYMONGO_BIN_DIR:-}" ]; then
  export PATH="$PATH:$HOME/.local/bin"
fi

# Only run the next part if not running on CI.
if [ -z "${CI:-}" ]; then
  # Set up venv, making sure c extensions build unless disabled.
  if [ -z "${NO_EXT:-}" ]; then
    export PYMONGO_C_EXT_MUST_BUILD=1
  fi

  (
    cd $ROOT && uv sync
  )

  # Set up build utilities on Windows spawn hosts.
  if [ -f $HOME/.visualStudioEnv.sh ]; then
    set +u
    SSH_TTY=1 source $HOME/.visualStudioEnv.sh
    set -u
  fi

  # Only set up pre-commit if we are in a git checkout.
  if [ -f $HERE/.git ]; then
    if ! command -v pre-commit &>/dev/null; then
      uv tool install pre-commit
    fi

    if [ ! -f .git/hooks/pre-commit ]; then
      uvx pre-commit install
    fi
  fi
fi

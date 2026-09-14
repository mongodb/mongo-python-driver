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

# The bin dir for the pinned uv/just. setup-system.sh sets it on evergreen hosts;
# default it here so local dev (without setup-system.sh) also has a usable value.
export PYMONGO_BIN_DIR="${PYMONGO_BIN_DIR:-$HOME/.local/bin}"

# Make sure a login shell can find the bin dir by adding it to the rc file, so
# local dev (which may never run setup-system.sh) still has it on PATH. env.sh's
# PATH does not persist past this session. Prefer .zshrc when the shell is zsh.
if [ "${CI:-}" != "true" ] && [ "${GITHUB_ACTIONS:-}" != "true" ]; then
  if [ -f "$HOME/.zshrc" ]; then
    _rc="$HOME/.zshrc"
  else
    _rc="$HOME/.bashrc"
  fi
  if [ -f "$_rc" ]; then
    grep -qF 'export PATH="'"$PYMONGO_BIN_DIR"':$PATH"' "$_rc" 2>/dev/null || \
      printf 'export PATH="%s:$PATH"\n' "$PYMONGO_BIN_DIR" >> "$_rc"
  fi
fi

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

# Re-source env.sh in case a dependency install updated it, e.g. on a host
# without a toolchain where uv was installed into a shared bin dir.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Handle the value for UV_PYTHON.
. $HERE/setup-uv-python.sh

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

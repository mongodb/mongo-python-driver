#!/bin/bash
# Set up development environment.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")

# Set up the uv environment if indicated.
if [ "${1:-}" == "ensure-uv" ]; then
  bash $HERE/setup-uv-python.sh
fi

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Get variables defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

# Only run the next part if not running on CI and there is a git checkout.
if [ -z "${CI:-}" ] && [ -f $HERE/.git ]; then
  # Add the default install path to the path if needed.
  if [ -z "${PYMONGO_BIN_DIR:-}" ]; then
    export PATH="$PATH:$HOME/.local/bin"
  fi

  # Set up venv, making sure c extensions build unless disabled.
  if [ -z "${NO_EXT:-}" ]; then
    export PYMONGO_C_EXT_MUST_BUILD=1
  fi

  (
    cd $ROOT && uv sync
  )

  if ! command -v pre-commit &>/dev/null; then
    uv tool install pre-commit
  fi

  if [ ! -f .git/hooks/pre-commit ]; then
    uvx pre-commit install
  fi
fi

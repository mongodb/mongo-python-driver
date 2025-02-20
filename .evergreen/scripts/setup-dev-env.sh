#!/bin/bash

set -eux

HERE=$(dirname ${BASH_SOURCE:-$0})
ROOT=$(dirname "$(dirname $HERE)")
pushd $ROOT > /dev/null

pwd

echo "ROOT=$ROOT"

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi
# PYTHON_BINARY may be defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

pwd
# Set the location of the python bin dir.
if [ "Windows_NT" = "${OS:-}" ]; then
  BIN_DIR=.venv/Scripts
else
  BIN_DIR=.venv/bin
fi

# Ensure there is a python venv.
if [ ! -d $BIN_DIR ]; then
  ls
  ls .evergreen
  . .evergreen/utils.sh

  if [ -z "${PYTHON_BINARY:-}" ]; then
      PYTHON_BINARY=$(find_python3)
  fi
  export UV_PYTHON=${PYTHON_BINARY}
  echo "export UV_PYTHON=$UV_PYTHON" >> $HERE/env.sh
  echo "Using python $UV_PYTHON"
fi

# Add the default install path to the path if needed.
if [ -z "${PYMONGO_BIN_DIR:-}" ]; then
  export PATH="$PATH:$HOME/.local/bin"
fi

uv sync --frozen
uv run --frozen --with pip pip install -e .
echo "Setting up python environment... done."

# Ensure there is a pre-commit hook if there is a git checkout.
if [ -d .git ] && [ ! -f .git/hooks/pre-commit ]; then
    uv run --frozen pre-commit install
fi

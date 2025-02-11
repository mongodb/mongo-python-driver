#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env file to pick up common variables.
if [ -f $HERE/env.sh ]; then
  source $HERE/env.sh
fi

# Ensure dependencies are installed.
. $HERE/install-dependencies.sh


# Set the location of the python bin dir.
if [ "Windows_NT" = "${OS:-}" ]; then
  BIN_DIR=.venv/Scripts
else
  BIN_DIR=.venv/bin
fi

# Ensure there is a python venv.
if [ ! -d $BIN_DIR ]; then
  . .evergreen/utils.sh

  if [ -z "${PYTHON_BINARY:-}" ]; then
      PYTHON_BINARY=$(find_python3)
  fi
  export UV_PYTHON=${PYTHON_BINARY}
  echo "export UV_PYTHON=$UV_PYTHON" >> $HERE/env.sh
  echo "Using python $UV_PYTHON"
fi
uv sync --frozen
uv run --frozen --with pip pip install -e .
echo "Setting up python environment... done."

# Ensure there is a pre-commit hook if there is a git checkout.
if [ -d .git ] && [ ! -f .git/hooks/pre-commit ]; then
    uv run --frozen pre-commit install
fi

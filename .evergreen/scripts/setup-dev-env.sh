#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env file to pick up common variables.
if [ -f $HERE/env.sh ]; then
  source $HERE/env.sh
fi

# Ensure dependencies are installed.
. .evergreen/install-dependencies.sh
echo "HELLO: $PATH"

# Ensure that we have the correct base python binary.
if [ -z "${UV_PYTHON:-}" ]; then
  . .evergreen/utils.sh

  if [ -z "${PYTHON_BINARY:-}" ]; then
      PYTHON_BINARY=$(find_python3)
  fi
  export UV_PYTHON=${PYTHON_BINARY}
  echo "export UV_PYTHON=$UV_PYTHON" >> $HERE/env.sh
fi

# Set up the python environment.
uv sync
uv run --with pip pip install -e .

# Ensure there is a pre-commit hook if there is a git checkout.
if [ -d .git ] && [ ! -f .git/hooks/pre-commit ]; then
    uv run pre-commit install
fi

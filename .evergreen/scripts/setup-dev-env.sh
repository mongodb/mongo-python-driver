#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env file to pick up common variables.
if [ -f $HERE/env.sh ]; then
  source $HERE/env.sh
fi

# Install just and uv directly if necessary.
if ! command -v just 2>/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s
fi
if ! command -v uv 2>/dev/null; then
  curl -LsSf https://astral.sh/uv/install.sh | sh
  source $HOME/.local/bin/env
fi

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

#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env file to pick up common variables.
if [ -f $HERE/scripts/env.sh ]; then
  source $HERE/scripts/env.sh
fi

# Set the location of the python bin dir.
if [ "Windows_NT" = "${OS:-}" ]; then
  BIN_DIR=.venv/Scripts
else
  BIN_DIR=.venv/bin
fi

. $HERE/install-dependencies.sh

# Ensure there is a python venv.
if [ ! -d $BIN_DIR ]; then
  . .evergreen/utils.sh

  if [ -z "${PYTHON_BINARY:-}" ]; then
      PYTHON_BINARY=$(find_python3)
  fi

  echo "Creating virtual environment..."
  createvirtualenv "$PYTHON_BINARY" .venv
  echo "Creating virtual environment... done."
fi

# Activate the virtual env.
. $BIN_DIR/activate

# Ensure there is a local hatch.
if [ ! -f $BIN_DIR/hatch ]; then
  echo "Installing hatch..."
  python -m pip install hatch || {
    # CARGO_HOME is defined in configure-env.sh
    export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo/}
    export RUSTUP_HOME="${CARGO_HOME}/.rustup"
    ${DRIVERS_TOOLS}/.evergreen/install-rust.sh
    source "${CARGO_HOME}/env"
    python -m pip install hatch
  }
  echo "Installing hatch... done."
fi

# Ensure hatch does not write to user or global locations.
HATCH_CONFIG=${HATCH_CONFIG:-hatch_config.toml}
if [ ! -f ${HATCH_CONFIG} ]; then
  touch hatch_config.toml
  hatch config restore
  hatch config set dirs.data "$(pwd)/.hatch/data"
  hatch config set dirs.cache "$(pwd)/.hatch/cache"
fi

# Ensure there is a local pre-commit if there is a git checkout.
if [ -d .git ]; then
  if [ ! -f $BIN_DIR/pre-commit ]; then
    python -m pip install pre-commit
  fi

  # Ensure the pre-commit hook is installed.
  if [ ! -f .git/hooks/pre-commit ]; then
    pre-commit install
  fi
fi

# Install pymongo and its test deps.
python -m pip install ".[test]"

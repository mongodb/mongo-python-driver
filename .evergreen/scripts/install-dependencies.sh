#!/bin/bash

set -eu

CARGO_HOME=${CARGO_HOME:-${DRIVERS_TOOLS}/.cargo}
# Handle paths on Windows.
if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
   CARGO_HOME=$(cygpath -m $CARGO_HOME)
fi

# Install a virtual env with "hatch"
# Ensure there is a python venv.
. ${DRIVERS_TOOLS}/.evergreen/venv-utils.sh
VENV_DIR=.venv
if [ ! -d $VENV_DIR ]; then
  echo "Creating virtual environment..."
  . ${DRIVERS_TOOLS}/.evergreen/find-python3.sh
  PYTHON_BINARY=$(ensure_python3)
  venvcreate $PYTHON_BINARY $VENV_DIR
  echo "Creating virtual environment... done."
fi

venvactivate $VENV_DIR
python --version

echo "Installing hatch..."
python -m pip install -U pip
python -m pip install hatch || {
  # Install rust and try again.
  export RUSTUP_HOME="${CARGO_HOME}/.rustup"
  ${DRIVERS_TOOLS}/.evergreen/install-rust.sh
  source "${CARGO_HOME}/env"
  python -m pip install hatch
}
# Ensure hatch does not write to user or global locations.
touch hatch_config.toml
HATCH_CONFIG=$(pwd)/hatch_config.toml
if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
    HATCH_CONFIG=$(cygpath -m "$HATCH_CONFIG")
fi
export HATCH_CONFIG
hatch config restore
hatch config set dirs.data "$(pwd)/.hatch/data"
hatch config set dirs.cache "$(pwd)/.hatch/cache"

echo "Installing hatch... done."
hatch --version

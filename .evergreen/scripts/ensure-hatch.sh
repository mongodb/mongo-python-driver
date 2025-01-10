#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Ensure hatch is available locally.
VENV_DIR=.venv
if [ -f $VENV_DIR/Scripts/hatch ]; then
  return 0
fi
if [ -f $VENV_DIR/bin/hatch ]; then
  return 0
fi

# Install a virtual env with "hatch"
# Ensure there is a python venv.
. .evergreen/utils.sh

if [ -z "${PYTHON_BINARY:-}" ]; then
    PYTHON_BINARY=$(find_python3)
fi

if [ ! -d $VENV_DIR ]; then
  echo "Creating virtual environment..."
  createvirtualenv "$PYTHON_BINARY" .venv
  echo "Creating virtual environment... done."
fi
if [ -f $VENV_DIR/Scripts/activate ]; then
  . $VENV_DIR/Scripts/activate
else
  . $VENV_DIR/bin/activate
fi

echo "Installing hatch..."
python -m pip install -U pip
python -m pip install hatch || {
  # Install rust and try again.
  CARGO_HOME=${CARGO_HOME:-${DRIVERS_TOOLS}/.cargo}
  # Handle paths on Windows.
  if [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
    CARGO_HOME=$(cygpath -m $CARGO_HOME)
  fi
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

#!/bin/bash
# Install the dependencies needed for an evergreen run.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Set up the default bin directory.
if [ -z "${PYMONGO_BIN_DIR:-}" ]; then
  PYMONGO_BIN_DIR="$HOME/.local/bin"
  export PATH="$PYMONGO_BIN_DIR:$PATH"
fi

# Helper function to pip install a dependency using a temporary python env.
function _pip_install() {
  _HERE=$(dirname ${BASH_SOURCE:-$0})
  . $_HERE/../utils.sh
  _VENV_PATH=$(mktemp -d)
  if [ "Windows_NT" = "${OS:-}" ]; then
    _VENV_PATH=$(cygpath -m $_VENV_PATH)
  fi
  echo "Installing $2 using pip..."
  createvirtualenv "$(find_python3)" $_VENV_PATH
  python -m pip install $1
  _suffix=""
  if [ "Windows_NT" = "${OS:-}" ]; then
    _suffix=".exe"
  fi
  ln -s "$(which $2)" $PYMONGO_BIN_DIR/${2}${_suffix}
  # uv also comes with a uvx binary.
  if [ $2 == "uv" ]; then
    ln -s "$(which uvx)" $PYMONGO_BIN_DIR/uvx${_suffix}
  fi
  echo "Installed to ${PYMONGO_BIN_DIR}"
  echo "Installing $2 using pip... done."
}

# Ensure just is installed.
if ! command -v just &>/dev/null; then
  # On most systems we can install directly.
  _TARGET=""
  if [ "Windows_NT" = "${OS:-}" ]; then
    _TARGET="--target x86_64-pc-windows-msvc"
  fi
  _BIN_DIR=$PYMONGO_BIN_DIR
  echo "Installing just..."
  mkdir -p "$_BIN_DIR" 2>/dev/null || true
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- $_TARGET --to "$_BIN_DIR" || {
    # Remove just file if it exists (can be created if there was an install error).
    rm -f ${_BIN_DIR}/just
    _pip_install rust-just just
  }
  echo "Installing just... done."
fi

# Ensure uv is installed.
if ! command -v uv &>/dev/null; then
  _BIN_DIR=$PYMONGO_BIN_DIR
  echo "Installing uv..."
  # On most systems we can install directly.
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$_BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh || {
     _pip_install uv uv
  }
  if [ "Windows_NT" = "${OS:-}" ]; then
    chmod +x "$(cygpath -u $_BIN_DIR)/uv.exe"
  fi
  echo "Installing uv... done."
fi

popd > /dev/null

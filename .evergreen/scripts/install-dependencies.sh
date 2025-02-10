#!/bin/bash

set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

echo "HELLO?"
cat $HERE/env.sh
_BIN_DIR=${PYMONGO_BIN_DIR:-}

# Helper function to pip install a dependency using a temporary python env.
function _pip_install() {
  _HERE=$(dirname ${BASH_SOURCE:-$0})
  . $_HERE/../utils.sh
  _VENV_PATH=$(mktemp -d)
  echo "Installing $2 using pip..."
  createvirtualenv "$(find_python3)" $_VENV_PATH
  python -m pip install $1
  ln -s "$(which $2)" $_BIN_DIR/$2
  echo "Installing $2 using pip... done."
}


# Ensure just is installed.
if ! command -v just 2>/dev/null; then
  if [ -z "${_BIN_DIR}" ]; then
    echo "Please install just!"
    exit 1
  fi
  # On most systems we can install directly.
  _TARGET=""
  if [ "Windows_NT" = "${OS:-}" ]; then
    _TARGET="--target x86_64-pc-windows-msvc"
  fi
  echo "Installing just..."
  mkdir -p "$_BIN_DIR" 2>/dev/null || true
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- $_TARGET --to "$_BIN_DIR" || {
    _pip_install rust-just just
  }
  echo "Installing just... done."
fi

# Install uv.
if ! command -v uv 2>/dev/null; then
  if [ -z "${_BIN_DIR}" ]; then
    echo "Please install uv!"
    exit 1
  fi
  echo "Installing uv..."
  # On most systems we can install directly.
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$_BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh || {
     _pip_install uv uv
  }
  echo "Installing uv... done."
fi

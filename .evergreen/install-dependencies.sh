#!/bin/bash
set -eu

# Set where binaries are expected to be.
# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME.
if [ "${CI:-}" == "true" ]; then
  _BIN_DIR=$DRIVERS_TOOLS_BINARIES
# On Windows spawn hosts, $HOME/cli_bin is on the PATH.
elif [ "Windows_NT" = "${OS:-}" ]; then
  _BIN_DIR=$HOME/cli_bin
# On local machines and Linux spawn hosts, we expect $HOME/.local/bin to be on the PATH.
else
  _BIN_DIR=$HOME/.local/bin
fi
mkdir -p $_BIN_DIR 2>/dev/null || true

function _pip_install() {
  _HERE=$(dirname ${BASH_SOURCE:-$0})
  . $_HERE/utils.sh
  _VENV_PATH=$(mktemp -d)
  createvirtualenv $(find_python3) $_VENV_PATH
  python -m pip install $1
  ln -s $(which $2) $_BIN_DIR/$2
}

_CURL_ARGS="--tlsv1.2 -LsSf"

# Install just.
if ! command -v just 2>/dev/null; then
  if [ "Windows_NT" = "${OS:-}" ]; then
    _TARGET="--target x86_64-pc-windows-msvc"
  else
    _TARGET=""
  fi
  # On most systems we can install directly.
  curl $_CURL_ARGS https://just.systems/install.sh | bash -s -- $_TARGET --to "$_BIN_DIR" || {
    _pip_install rust-just just
  }
  if ! command -v just 2>/dev/null; then
    export PATH="$PATH:$_BIN_DIR"
  fi
fi

# Install uv.
if ! command -v uv 2>/dev/null; then
  # On most systems we can install directly.
  curl $_CURL_ARGS https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$_BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh || {
     _pip_install uv uv
  }
  if ! command -v uv 2>/dev/null; then
    export PATH="$PATH:$_BIN_DIR"
  fi
fi

#!/bin/bash
# Install the necessary dependencies.
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
fi

# Ensure uv >= 0.10 is installed (older versions don't support pyproject.toml
# settings we rely on, e.g. relative `exclude-newer` values).
MIN_UV_VERSION="0.10.0"

uv_version_ok() {
  command -v uv &>/dev/null || return 1
  _current="$(uv --version | cut -d' ' -f2)"
  [ "$(printf '%s\n%s\n' "$MIN_UV_VERSION" "$_current" | sort -V | head -n1)" = "$MIN_UV_VERSION" ]
}

if ! uv_version_ok; then
  _BIN_DIR=$PYMONGO_BIN_DIR
  mkdir -p ${_BIN_DIR}
  echo "Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$_BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh
  if [ "Windows_NT" = "${OS:-}" ]; then
    chmod +x "$(cygpath -u $_BIN_DIR)/uv.exe"
  fi
  export PATH="$PYMONGO_BIN_DIR:$PATH"
  echo "Installing uv... done."
  if ! uv_version_ok; then
    echo "Installed uv $(uv --version) but >= $MIN_UV_VERSION is required." >&2
    exit 1
  fi
fi

# Ensure just is installed.
if ! command -v just &>/dev/null; then
  uv tool install rust-just
fi

popd > /dev/null

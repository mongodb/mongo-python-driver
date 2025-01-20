#!/bin/bash
set -eu

# Set where binaries are expected to be.
# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME.
if [ "${CI:-}" == "true" ]; then
  BIN_DIR=$DRIVERS_TOOLS_BINARIES
else
  BIN_DIR=$HOME/.local/bin
fi


_CURL_ARGS="--proto '=https' --tlsv1.2 -LsSf"

function _cargo_install() {
  # CARGO_HOME is defined in configure-env.sh
  export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo/}
  export RUSTUP_HOME="${CARGO_HOME}/.rustup"
  . ${DRIVERS_TOOLS}/.evergreen/install-rust.sh
  cargo install "$@"
}

# Install just.
if ! command -v just 2>/dev/null; then
  if [ "Windows_NT" = "${OS:-}" ]; then
    TARGET="--target x86_64-pc-windows-msvc"
  else
    TARGET=""
  fi
  curl $_CURL_ARGS https://just.systems/install.sh | bash -s -- $TARGET --to "$BIN_DIR" || {
    _cargo_install just
    if [ "Windows_NT" = "${OS:-}" ]; then
      mv $CARGO_HOME/bin/just.exe $BIN_DIR/just
    else
      mv $CARGO_HOME/bin/just $BIN_DIR
    fi
  }
  if ! command -v just 2>/dev/null; then
    export PATH="$PATH:$BIN_DIR"
  fi
fi

# Install uv.
if ! command -v uv 2>/dev/null; then
  # On most non-Windows systems we can install directly.
  if [ "Windows_NT" != "${OS:-}" ]; then
    curl $_CURL_ARGS https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh || true
  fi
  # On Windows or unsupported systems, fall back to installing from cargo.
  if [ ! -f $BIN_DIR/uv ]; then
    _cargo_install --git https://github.com/astral-sh/uv uv
    if [ "Windows_NT" = "${OS:-}" ]; then
      mv $CARGO_HOME/bin/uv.exe $BIN_DIR/uv
      mv $CARGO_HOME/bin/uvx.exe $BIN_DIR/uvx
    else
      mv $CARGO_HOME/bin/uv $BIN_DIR
      mv $CARGO_HOME/bin/uvx $BIN_DIR
    fi
  fi
  if ! command -v uv 2>/dev/null; then
    export PATH="$PATH:$BIN_DIR"
  fi
fi

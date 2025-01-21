#!/bin/bash

set -eu

# Install just.
# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME.
if [ "${CI:-}" == "true" ]; then
  BIN_DIR=${DRIVERS_TOOLS_BINARIES:-}
else
  BIN_DIR=$HOME/.local/bin
fi
if [ ! -f $BIN_DIR/just ]; then
  if [ "Windows_NT" = "${OS:-}" ]; then
    TARGET="--target x86_64-pc-windows-msvc"
  else
    TARGET=""
  fi
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- $TARGET --to "$BIN_DIR" || {
    # CARGO_HOME is defined in configure-env.sh
    export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo/}
    export RUSTUP_HOME="${CARGO_HOME}/.rustup"
    . ${DRIVERS_TOOLS}/.evergreen/install-rust.sh
    cargo install just
    if [ "Windows_NT" = "${OS:-}" ]; then
      mv $CARGO_HOME/just.exe $BIN_DIR/just
    else
      mv $CARGO_HOME/just $BIN_DIR
    fi
  }
fi

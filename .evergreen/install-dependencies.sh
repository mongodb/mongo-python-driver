#!/bin/bash
set -eu

# Set where binaries are expected to be.
# On Evergreen jobs, "CI" will be set, and we don't want to write to $HOME.
if [ "${CI:-}" == "true" ]; then
  BIN_DIR=$DRIVERS_TOOLS_BINARIES
else
  BIN_DIR=$HOME/.local/bin
  # Ensure bin dir is on the path.
  export PATH="$BIN_DIR:$PATH"
fi

# Install just.
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
      mv $CARGO_HOME/bin/just.exe $BIN_DIR/just
    else
      mv $CARGO_HOME/binjust $BIN_DIR
    fi
  }
fi

# Install uv.
if [ ! -f $BIN_DIR/uv ]; then
  # On most non-Windows systems we can install directly.
  if [ "Windows_NT" != "${OS:-}" ]; then
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh || true
  fi
  # On Windows or unsupported systems, fall back to installing from cargo.
  if [ ! -f $BIN_DIR/uv ]; then
     # CARGO_HOME is defined in configure-env.sh
    export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo/}
    export RUSTUP_HOME="${CARGO_HOME}/.rustup"
    . ${DRIVERS_TOOLS}/.evergreen/install-rust.sh
    cargo install --git https://github.com/astral-sh/uv uv
    if [ "Windows_NT" = "${OS:-}" ]; then
      mv $CARGO_HOME/bin/uv.exe $BIN_DIR/uv
      mv $CARGO_HOME/bin/uvx.exe $BIN_DIR/uvx
    else
      mv $CARGO_HOME/bin/uv $BIN_DIR
      mv $CARGO_HOME/bin/uvx $BIN_DIR
    fi
  fi
fi

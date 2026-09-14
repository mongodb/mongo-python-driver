#!/bin/bash
# Install the necessary dependencies.
set -euo pipefail

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
pushd "$(dirname "$(dirname $HERE)")" > /dev/null

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Set up the default bin directory.
if [ -z "${PYMONGO_BIN_DIR:-}" ]; then
  PYMONGO_BIN_DIR="$HOME/.local/bin"
fi
# uv.exe on Windows needs Windows-style paths, while bash uses the cygwin form.
# Keep PYMONGO_BIN_DIR in the form PATH uses and give uv the Windows form.
if [ "Windows_NT" = "${OS:-}" ]; then
  _uv_tool_bin="$(cygpath -m "$PYMONGO_BIN_DIR")"
  _uv_tool_dir="$(cygpath -m "${UV_TOOL_DIR:-$(dirname "$PYMONGO_BIN_DIR")/uv-tools}")"
  export UV_TOOL_BIN_DIR="$_uv_tool_bin"
  export UV_TOOL_DIR="$_uv_tool_dir"
else
  export UV_TOOL_BIN_DIR="$PYMONGO_BIN_DIR"
fi
mkdir -p "$PYMONGO_BIN_DIR"

# Locate the Python toolchain's binary dir, so we can prefer its uv and just.
_toolchain_bin=""
if [ "Windows_NT" = "${OS:-}" ]; then
  _toolchain_bin="/cygdrive/c/Python/Current/Scripts"
elif [ "$(uname -s)" = "Darwin" ]; then
  _toolchain_bin="/Library/Frameworks/Python.Framework/Versions/Current/bin"
else
  _toolchain_bin="/opt/python/Current/bin"
fi

# Prefer the toolchain's uv as a bootstrap when uv is not already on PATH, so we
# do not fall back to installing from astral.
if ! command -v uv &>/dev/null; then
  if [ -x "$_toolchain_bin/uv" ] || [ -x "$_toolchain_bin/uv.exe" ]; then
    echo "Found uv in the toolchain at $_toolchain_bin"
    export PATH="$_toolchain_bin:$PATH"
  fi
fi

# Ensure uv is available (bootstrap if absent).
if ! command -v uv &>/dev/null; then
  _BIN_DIR=$PYMONGO_BIN_DIR
  mkdir -p ${_BIN_DIR}
  echo "uv not found on PATH; installing the latest uv from astral..."
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$_BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh
  if [ "Windows_NT" = "${OS:-}" ]; then
    chmod +x "$(cygpath -u $_BIN_DIR)/uv.exe"
  fi
  export PATH="$PYMONGO_BIN_DIR:$PATH"
fi

# Pin the uv binary to the version in pyproject.toml's [tool.uv] required-version.
# Run the current uv directly: it writes into PYMONGO_BIN_DIR (a different
# location), so nothing running is overwritten. If the running uv is already our
# pinned bin-dir uv, skip the install to avoid overwriting it (Windows refuses to
# overwrite a running executable); otherwise install so the pin lands in the bin
# dir even when the discovered uv already matches.
_uv_bin="$(command -v uv 2>/dev/null || true)"
if [ -n "$_uv_bin" ]; then
  _uv_pin="$(awk -F'"' '/^[[:space:]]*required-version[[:space:]]*=/{print $2}' pyproject.toml)"
  case "$_uv_bin" in
    "$PYMONGO_BIN_DIR"/*)
      _uv_vers="$(uv --version 2>/dev/null | head -1 | awk '{print $2}' | sed 's/^v//')"
      if [ "uv${_uv_pin}" != "uv==${_uv_vers}" ]; then
        # The running uv lives in our bin dir and is not the pin, so run the
        # install from a copy: Windows will not overwrite a running executable.
        _uv_tmp="$(mktemp -d)/$(basename "$_uv_bin")"
        cp "$_uv_bin" "$_uv_tmp" && chmod +x "$_uv_tmp"
        "$_uv_tmp" tool install --no-config -q --force --from "uv${_uv_pin}" uv
        rm -rf "$(dirname "$_uv_tmp")"
        echo "Using uv at $PYMONGO_BIN_DIR/uv ($("$PYMONGO_BIN_DIR/uv" --version 2>/dev/null | head -1 | awk '{print $2}'))"
      fi
      ;;
    *)
      uv tool install --no-config -q --force --from "uv${_uv_pin}" uv
      echo "Using uv at $PYMONGO_BIN_DIR/uv ($("$PYMONGO_BIN_DIR/uv" --version 2>/dev/null | head -1 | awk '{print $2}'))"
      ;;
  esac
fi

# Use just from the toolchain if available, otherwise install it. It must live in
# our bin dir to be on PATH for callers; copying it keeps the toolchain's just.
if [ ! -x "$PYMONGO_BIN_DIR/just" ] && [ ! -x "$PYMONGO_BIN_DIR/just.exe" ]; then
  if [ -x "$_toolchain_bin/just" ]; then
    echo "Using just from the toolchain"
    cp "$_toolchain_bin/just" "$PYMONGO_BIN_DIR/just"
    chmod +x "$PYMONGO_BIN_DIR/just"
  elif [ -x "$_toolchain_bin/just.exe" ]; then
    echo "Using just from the toolchain"
    cp "$_toolchain_bin/just.exe" "$PYMONGO_BIN_DIR/just.exe"
    chmod +x "$PYMONGO_BIN_DIR/just.exe"
  else
    uv tool install --no-config rust-just
  fi
fi

popd > /dev/null

#!/bin/bash
# Install the necessary dependencies.
set -eu

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

# Ensure uv is installed.
if ! command -v uv &>/dev/null; then
  _BIN_DIR=$PYMONGO_BIN_DIR
  mkdir -p ${_BIN_DIR}
  echo "Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$_BIN_DIR" INSTALLER_NO_MODIFY_PATH=1 sh
  if [ "Windows_NT" = "${OS:-}" ]; then
    chmod +x "$(cygpath -u $_BIN_DIR)/uv.exe"
  fi
  export PATH="$PYMONGO_BIN_DIR:$PATH"
  echo "Installing uv... done."
fi

# Ensure just is installed.
if ! command -v just &>/dev/null; then
  uv tool install rust-just
fi

# Some images (e.g. the DEVPROD-19149 Windows image) ship without a Python toolchain.
# A python3 may still be on PATH there (the mingw one that comes with chocolatey) that
# runs but cannot bootstrap pip, so probe for one that can actually create a virtual
# environment. We probe python3 specifically because that is the name
# drivers-evergreen-tools' find_python3 falls back to, and it only checks that the
# interpreter runs - not that it can build the venv it then goes on to need.
_have_python3=""
if command -v python3 &>/dev/null; then
  _probe=$(mktemp -d)
  _probe_arg="$_probe"
  if [ "Windows_NT" = "${OS:-}" ]; then
    _probe_arg=$(cygpath -aw "$_probe")
  fi
  if python3 -m venv "$_probe_arg" &>/dev/null; then
    _have_python3="1"
  fi
  rm -rf "$_probe"
fi

# Install a Python with uv and expose it so that both pymongo and
# drivers-evergreen-tools can find it.
if [ -z "$_have_python3" ]; then
  echo "No venv-capable python3 found, installing with uv..."
  # UV_PYTHON may be a toolchain path; fall back to a bare version for the download.
  _py_ver="${UV_PYTHON:-}"
  case "$_py_ver" in
    ""|*/*|*\\*) _py_ver="3.10" ;;
  esac
  uv python install "$_py_ver"
  _py_bin="$(uv python find --no-project --managed-python "$_py_ver")"
  if [ "Windows_NT" = "${OS:-}" ]; then
    _py_bin=$(cygpath -u "$_py_bin")
  fi
  _py_dir=$(dirname "$_py_bin")
  # uv's Windows builds ship python.exe only. Without a python3.exe alongside it the
  # mingw python3 further down PATH still wins every bare `python3` lookup.
  if [ "Windows_NT" = "${OS:-}" ] && [ ! -e "$_py_dir/python3.exe" ]; then
    cp "$_py_bin" "$_py_dir/python3.exe"
  fi
  export PATH="$_py_dir:$PATH"
  export DRIVERS_TOOLS_PYTHON="$_py_bin"
  # Persist for the steps that source env.sh rather than inheriting this shell.
  cat <<EOT >> "$HERE/env.sh"
export PATH="$_py_dir:\$PATH"
export DRIVERS_TOOLS_PYTHON="$_py_bin"
EOT
  echo "Installed Python at $_py_bin"
  echo "python3 now resolves to: $(command -v python3 || echo NONE)"
fi

popd > /dev/null

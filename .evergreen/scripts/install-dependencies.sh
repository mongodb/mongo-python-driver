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
# A python may still be on PATH there (the mingw python3 that comes with chocolatey)
# that cannot bootstrap pip, so probe for one that can actually create a virtual
# environment - that is what drivers-evergreen-tools needs it for.
_have_python=""
for _cand in python3 python; do
  if ! command -v "$_cand" &>/dev/null; then
    continue
  fi
  _probe=$(mktemp -d)
  _probe_arg="$_probe"
  if [ "Windows_NT" = "${OS:-}" ]; then
    _probe_arg=$(cygpath -aw "$_probe")
  fi
  if "$_cand" -m venv "$_probe_arg" &>/dev/null; then
    _have_python="$_cand"
  fi
  rm -rf "$_probe"
  if [ -n "$_have_python" ]; then
    break
  fi
done

# Install a Python with uv and expose it so that both pymongo and
# drivers-evergreen-tools can find it: DRIVERS_TOOLS_PYTHON is honored by
# DET's ensure_python3(), which is what builds its virtual environments.
if [ -z "$_have_python" ]; then
  echo "No usable Python found, installing with uv..."
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
  export PATH="$_py_dir:$PATH"
  export DRIVERS_TOOLS_PYTHON="$_py_bin"
  # Persist for the steps that source env.sh rather than inheriting this shell.
  cat <<EOT >> "$HERE/env.sh"
export PATH="$_py_dir:\$PATH"
export DRIVERS_TOOLS_PYTHON="$_py_bin"
EOT
  echo "No usable Python found, installing with uv... done ($_py_bin)."
fi

popd > /dev/null

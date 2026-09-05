#!/bin/bash
# Set up the UV_PYTHON variable and put the toolchain pythons on the path.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"

# Use min supported version by default.
_python="3.10"

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi

# Get variables defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# Always use UV_PYTHON to select the Python version.
if [ -z "${UV_PYTHON:-}" ]; then
  export UV_PYTHON="$_python"
fi

# Prefer a toolchain (system) python over a uv-managed download: resolve a
# bare version like 3.10 or 3.14t to the toolchain interpreter when one is
# installed, and put its directory on the path.  Anything else (a path, or a
# version uv must install itself, such as a pre-release) is left alone.
if [[ "$UV_PYTHON" =~ ^3\.[0-9]+t?$ ]]; then
  case "$(uname -s)" in
    Darwin)
      if [[ "$UV_PYTHON" == *"t"* ]]; then
        binary_name="python3t"
        framework_dir="PythonT"
      else
        binary_name="python3"
        framework_dir="Python"
      fi
      _version="${UV_PYTHON%t}"
      _bin_dir="/Library/Frameworks/${framework_dir}.Framework/Versions/$_version/bin"
      if [ -x "$_bin_dir/$binary_name" ]; then
        export UV_PYTHON="$_bin_dir/$binary_name"
        export PATH="$_bin_dir:$PATH"
      fi
      ;;
    *)
      if [ "Windows_NT" = "${OS:-}" ]; then
        _dir=$(echo "$UV_PYTHON" | cut -d. -f1,2 | sed 's/\.//g; s/t//g')
        if [[ "$UV_PYTHON" == *"t"* ]]; then
          _exe="python${UV_PYTHON}.exe"
        else
          _exe="python.exe"
        fi
        if [ -n "${IS_WIN32:-}" ]; then
          _bin_dir="C:/python/32/Python${_dir}"
        else
          _bin_dir="C:/python/Python${_dir}"
        fi
        if [ -f "$_bin_dir/$_exe" ]; then
          export UV_PYTHON="$_bin_dir/$_exe"
          export PATH="$_bin_dir:$PATH"
        fi
      else
        _bin_dir="/opt/python/$UV_PYTHON/bin"
        if [ -x "$_bin_dir/python3" ]; then
          export UV_PYTHON="$_bin_dir/python3"
          export PATH="$_bin_dir:$PATH"
        fi
      fi
      ;;
  esac
fi

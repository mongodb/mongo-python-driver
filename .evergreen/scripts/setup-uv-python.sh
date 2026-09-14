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

# Prefer system/toolchain interpreters over uv-managed downloads.  Skip on
# Windows, where the first python3 on the path is a broken Chocolatey shim that
# uv cannot inspect.
if [ "Windows_NT" != "${OS:-}" ]; then
  export UV_PYTHON_PREFERENCE=system
fi

# UV_PYTHON is a version identifier (e.g. 3.14) on Linux/macOS, where uv
# discovers a matching toolchain Python from the path.  On Windows uv cannot
# reliably resolve a bare version (its path search trips over a broken
# Chocolatey python3 shim), so UV_PYTHON is set to the interpreter path there.
if [ -z "${UV_PYTHON:-}" ]; then
  if [ "${REQUIRE_FIPS:-}" = "1" ]; then
    # FIPS hosts provision a specific Python; put its directory first on the
    # path and leave UV_PYTHON unset so uv resolves the interpreter from PATH.
    export PATH="/usr/bin:$PATH"
  else
    export UV_PYTHON="$_python"
  fi
fi

# Whether a toolchain Python matching UV_PYTHON was found on the host.
PYTHON_FOUND=0
# UV_PYTHON may already be an absolute path, e.g. the Windows toolchain path
# resolved on a prior invocation.  A path is already usable, so it is treated as
# found and never triggers a download.
if [ -n "${UV_PYTHON:-}" ] && [[ "$UV_PYTHON" == /* || "$UV_PYTHON" == ?:/* ]]; then
  PYTHON_FOUND=1
elif [ -n "${UV_PYTHON:-}" ] && [[ "$UV_PYTHON" =~ ^3\.[0-9]+t?$ ]]; then
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
        export PATH="$_bin_dir:$PATH"
        PYTHON_FOUND=1
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
          # Windows: point UV_PYTHON at the interpreter (path-based) so uv does
          # not probe the path and trip over a broken Chocolatey python3 shim.
          export UV_PYTHON="$_bin_dir/$_exe"
          export PATH="$_bin_dir:$PATH"
          PYTHON_FOUND=1
        fi
      else
        _bin_dir="/opt/python/$UV_PYTHON/bin"
        if [ -x "$_bin_dir/python3" ]; then
          export PATH="$_bin_dir:$PATH"
          PYTHON_FOUND=1
        fi
      fi
      ;;
  esac
fi
export PYTHON_FOUND

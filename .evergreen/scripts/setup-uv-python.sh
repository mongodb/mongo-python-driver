#!/bin/bash
# Set up the UV_PYTHON variable.
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

# Translate PYTHON_BINARY/PYTHON_VERSION to UV_PYTHON.
if [ -z "${UV_PYTHON:-}" ]; then
  if [ -n "${PYTHON_BINARY:-}" ]; then
    _python=$PYTHON_BINARY

  elif [ -n "${PYTHON_VERSION:-}" ]; then
    _python=$PYTHON_VERSION
    if [ "$(uname -s)" = "Darwin" ]; then
        if [[ "$_python" == *"t"* ]]; then
            binary_name="python3t"
            framework_dir="PythonT"
        else
            binary_name="python3"
            framework_dir="Python"
        fi
        _python=$(echo "$_python" | sed 's/t//g')
        _python="/Library/Frameworks/$framework_dir.Framework/Versions/$_python/bin/$binary_name"
    elif [ "Windows_NT" = "${OS:-}" ]; then
        _python=$(echo $_python | cut -d. -f1,2 | sed 's/\.//g; s/t//g')
        if [ -n "${IS_WIN32:-}" ]; then
            _python="C:/python/32/Python$_python/python.exe"
        else
            _python="C:/python/Python$_python/python.exe"
        fi
    elif [ -d "/opt/python/$_python/bin" ]; then
        _python="/opt/python/$_python/bin/python3"
    fi
  fi
  export UV_PYTHON="$_python"
fi

#!/bin/bash
# Set up the UV_PYTHON variable.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"

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
    echo "export UV_PYTHON=$PYTHON_BINARY" >> $HERE/env.sh
  elif [ -n "${PYTHON_VERSION:-}" ]; then
    version=$PYTHON_VERSION
    if [ "$(uname -s)" = "Darwin" ]; then
        if [[ "$version" == *"t"* ]]; then
            binary_name="python3t"
            framework_dir="PythonT"
        else
            binary_name="python3"
            framework_dir="Python"
        fi
        version=$(echo "$version" | sed 's/t//g')
        UV_PYTHON="/Library/Frameworks/$framework_dir.Framework/Versions/$version/bin/$binary_name"
    elif [ "Windows_NT" = "${OS:-}" ]; then
        version=$(echo $version | cut -d. -f1,2 | sed 's/\.//g; s/t//g')
        if [ -n "${IS_WIN32:-}" ]; then
            UV_PYTHON="C:/python/32/Python$version/python.exe"
        else
            UV_PYTHON="C:/python/Python$version/python.exe"
        fi
    elif [ -d "/opt/python/$version/bin" ]; then
        UV_PYTHON="/opt/python/$version/bin/python3"
    else
        UV_PYTHON="${PYTHON_VERSION}"
    fi
    echo "export UV_PYTHON=$UV_PYTHON" >> $HERE/env.sh
  fi
fi

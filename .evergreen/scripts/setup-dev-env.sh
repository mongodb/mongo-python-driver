#!/bin/bash
# Set up a development environment on an evergreen host.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")
pushd $ROOT > /dev/null

# Bail early if running on GitHub Actions.
if [ -n "${GITHUB_ACTION:-}" ]; then
  exit 0
fi

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi
# PYTHON_BINARY or PYTHON_VERSION may be defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

# Get the appropriate UV_PYTHON.
. $ROOT/.evergreen/utils.sh

if [ -z "${PYTHON_BINARY:-}" ]; then
    if [ -n "${PYTHON_VERSION:-}" ]; then
      PYTHON_BINARY=$(get_python_binary $PYTHON_VERSION)
    else
      PYTHON_BINARY=$(find_python3)
    fi
fi
export UV_PYTHON=${PYTHON_BINARY}
echo "Using python $UV_PYTHON"

# Add the default install path to the path if needed.
if [ -z "${PYMONGO_BIN_DIR:-}" ]; then
  export PATH="$PATH:$HOME/.local/bin"
fi

# Set up venv, making sure c extensions build unless disabled.
if [ -z "${NO_EXT:-}" ]; then
  export PYMONGO_C_EXT_MUST_BUILD=1
fi
# Set up visual studio env on Windows spawn hosts.
if [ -f $HOME/.visualStudioEnv.sh ]; then
  set +u
  SSH_TTY=1 source $HOME/.visualStudioEnv.sh
  set -u
fi
uv sync

echo "Setting up python environment... done."

popd > /dev/null

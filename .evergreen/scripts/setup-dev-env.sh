#!/bin/bash
# Set up a development environment on an evergreen host.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
ROOT=$(dirname "$(dirname $HERE)")
pushd $ROOT > /dev/null

# Source the env files to pick up common variables.
if [ -f $HERE/env.sh ]; then
  . $HERE/env.sh
fi
# PYTHON_BINARY may be defined in test-env.sh.
if [ -f $HERE/test-env.sh ]; then
  . $HERE/test-env.sh
fi

# Ensure dependencies are installed.
bash $HERE/install-dependencies.sh

# Set the location of the python bin dir.
if [ "Windows_NT" = "${OS:-}" ]; then
  BIN_DIR=.venv/Scripts
else
  BIN_DIR=.venv/bin
fi

# Ensure there is a python venv.
if [ ! -d $BIN_DIR ]; then
  . $ROOT/.evergreen/utils.sh

  if [ -z "${PYTHON_BINARY:-}" ]; then
      PYTHON_BINARY=$(find_python3)
  fi
  export UV_PYTHON=${PYTHON_BINARY}
  echo "export UV_PYTHON=$UV_PYTHON" >> $HERE/env.sh
  echo "Using python $UV_PYTHON"
fi

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
uv sync --frozen

echo "Setting up python environment... done."

# Ensure there is a pre-commit hook if there is a git checkout.
if [ -d .git ] && [ ! -f .git/hooks/pre-commit ]; then
    uv run --frozen pre-commit install
fi

popd > /dev/null

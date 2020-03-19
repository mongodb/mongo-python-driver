#!/bin/sh
set -o xtrace

export PYMONGO_VIRTUALENV_NAME="pymongotestvenv"
export PYMONGO_PYTHON_RUNTIME="$(pwd)/driver-src/$PYMONGO_VIRTUALENV_NAME/bin/python"
cat <<EOT > driver-expansion.yml
PREPARE_SHELL_DRIVER: |
  set -o errexit
  export PYTHON_BINARY="${PYTHON_BINARY}"
  export PYMONGO_VIRTUALENV_NAME="$PYMONGO_VIRTUALENV_NAME"
  export PYMONGO_PYTHON_RUNTIME="$PYMONGO_PYTHON_RUNTIME"
EOT

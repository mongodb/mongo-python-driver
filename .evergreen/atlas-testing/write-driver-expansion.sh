#!/bin/sh
set -o xtrace

export PYMONGO_VIRTUALENV_NAME="pymongotestvenv"
cat <<EOT > driver-expansion.yml
PREPARE_SHELL_DRIVER: |
  set -o errexit
  export PYTHON_BINARY="$PYTHON_BINARY"
  export PYMONGO_VIRTUALENV_NAME="$PYMONGO_VIRTUALENV_NAME"
  export PYMONGO_BIN_DIR="$(pwd)/$PYMONGO_VIRTUALENV_NAME/bin"
EOT

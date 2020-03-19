#!/bin/sh
set -o xtrace

"$PYTHON_BINARY" -m virtualenv "$PYMONGO_VIRTUALENV_NAME"
"$PYMONGO_BIN_DIR/pip" install -e .[srv]


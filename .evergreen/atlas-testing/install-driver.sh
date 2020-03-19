#!/bin/sh
set -o xtrace

"$PYTHON_BINARY" -m virtualenv "$PYMONGO_VIRTUALENV_NAME"
"$PYMONGO_VIRTUALENV_NAME/bin/pip" install -e .[srv]


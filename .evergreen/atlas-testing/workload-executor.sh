#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

trap "exit 0" INT

"$PYMONGO_BIN_DIR/python" "driver-src/.evergreen/atlas-testing/workload-executor.py" "$1" "$2"

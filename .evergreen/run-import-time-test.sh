#!/bin/bash -ex

set -o errexit  # Exit the script with error if any of the commands fail
set -x

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

# Skip the report of it isn't a PR run.
if [ "$BASE_SHA" == "$HEAD_SHA" ]; then
    echo "Skipping API Report"
    exit 0
fi

function get_import_time() {
    local log_file
    createvirtualenv "$PYTHON_BINARY" import-venv
    python -m pip install -q ".[aws,encryption,gssapi,ocsp,snappy,zstd]"
    # Import once to cache modules
    python -c "import pymongo"
    log_file="pymongo-$1.log"
    python -X importtime -c "import pymongo" 2> $log_file
    last_line=$(echo "$(tail -n 1 $log_file)" | cut -d " " -f 5)
    rm -rf import-venv
}

get_import_time $HEAD_SHA
import_time_curr=$last_line
git checkout $BASE_SHA
get_import_time $BASE_SHA
import_time_prev=$last_line

# Check if we got 20% or more slower
let diff=$import_time_curr-$import_time_prev
let ratio=$diff / $import_time_prev * 100
if [ $ratio -gt 20 ]; then
    echo "Import got $ratio percent slower"
    exit 1
fi

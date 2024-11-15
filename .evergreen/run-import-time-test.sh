#!/bin/bash -ex

set -o errexit  # Exit the script with error if any of the commands fail
set -x

. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON_BINARY=$(find_python3)
fi

# Use the previous commit if this was not a PR run.
if [ "$BASE_SHA" == "$HEAD_SHA" ]; then
    BASE_SHA=$(git rev-parse HEAD~1)
fi

function get_import_time() {
    local log_file
    createvirtualenv "$PYTHON_BINARY" import-venv
    python -m pip install -q ".[aws,encryption,gssapi,ocsp,snappy,zstd]"
    # Import once to cache modules
    python -c "import pymongo"
    log_file="pymongo-$1.log"
    python -X importtime -c "import pymongo" 2> $log_file
}

get_import_time $HEAD_SHA
git stash
git checkout $BASE_SHA
get_import_time $BASE_SHA
git checkout $HEAD_SHA
git stash apply
python tools/compare_import_time.py $HEAD_SHA $BASE_SHA

#!/bin/bash
# Check for regressions in the import time of pymongo.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})

source $HERE/env.sh

pushd $HERE/../.. >/dev/null

BASE_SHA="$1"
HEAD_SHA="$2"

# Set up the virtual env.
. $HERE/setup-dev-env.sh
uv venv --seed
source .venv/bin/activate

# Use the previous commit if this was not a PR run.
if [ "$BASE_SHA" == "$HEAD_SHA" ]; then
    BASE_SHA=$(git rev-parse HEAD~1)
fi

function get_import_time() {
    local log_file
    python -m pip install -q ".[aws,encryption,gssapi,ocsp,snappy,zstd]"
    # Import once to cache modules
    python -c "import pymongo"
    log_file="pymongo-$1.log"
    python -X importtime -c "import pymongo" 2> $log_file
}

get_import_time $HEAD_SHA
git stash || true
git checkout $BASE_SHA
get_import_time $BASE_SHA
git checkout $HEAD_SHA
git stash apply || true
python tools/compare_import_time.py $HEAD_SHA $BASE_SHA

popd >/dev/null

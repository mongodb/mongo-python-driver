#!/bin/bash

set -o xtrace
set -o errexit

git clone https://github.com/mongodb-labs/driver-performance-test-data.git
cd driver-performance-test-data
tar xf extended_bson.tgz
tar xf parallel.tgz
tar xf single_and_multi_document.tgz
cd ..

export TEST_PATH="${PROJECT_DIRECTORY}/driver-performance-test-data"
export OUTPUT_FILE="${PROJECT_DIRECTORY}/results.json"

echo "python3 version:"
python3 --version || true
echo "mongodbtoolchain versions:"
ls -la /opt/mongodbtoolchain/

export PYTHON_BINARY=/opt/mongodbtoolchain/v3/bin/python3
export PERF_TEST=1

bash ./.evergreen/tox.sh -m test-eg

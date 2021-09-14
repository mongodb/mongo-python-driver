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

MTCBIN=/opt/mongodbtoolchain/v2/bin
VIRTUALENV="$MTCBIN/virtualenv -p $MTCBIN/python3"

$VIRTUALENV pyperftest
. pyperftest/bin/activate
python -m pip install simplejson

python setup.py build_ext -i
start_time=$(date +%s)
python test/performance/perf_test.py --locals
end_time=$(date +%s)
elapsed_secs=$((end_time-start_time))

cat results.json

echo "{\"failures\": 0, \"results\": [{\"status\": \"pass\", \"exit_code\": 0, \"test_file\": \"BenchMarkTests\", \"start\": $start_time, \"end\": $end_time, \"elapsed\": $elapsed_secs}]}" > report.json

cat report.json

#!/bin/bash
# Run all of the integration test files using `uv run`.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$HERE" > /dev/null
trap 'popd' ERR

for file in test_*.py ; do
    echo "-----------------"
    echo "Running $file..."
    uv run $file
    echo "Running $file...done."
    echo "-----------------"
done

popd

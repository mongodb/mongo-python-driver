#!/bin/bash
# Run all of the integration test files using `uv run`.
set -eu

for file in integration_tests/test_*.py ; do
    echo "-----------------"
    echo "Running $file..."
    uv run $file
    echo "Running $file...done."
    echo "-----------------"
done

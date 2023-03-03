#!/bin/bash

# Exit on error and enable trace.
set -o errexit
set -o xtrace

if [ -z "$PYTHON_BINARY" ]; then
    echo "No python binary specified"
    PYTHON_BINARY=$(command -v python3) || true
    if [ -z "$PYTHON_BINARY" ]; then
        echo "Cannot test without python3 installed!"
        exit 1
    fi
fi

$PYTHON_BINARY test/atlas/test_connection.py

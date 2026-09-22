#!/bin/bash
# Set up the test environment, including secrets and services.
set -eu

# Supported/used environment variables:
#  AUTH                 Set to enable authentication. Defaults to "noauth"
#  SSL                  Set to enable SSL. Defaults to "nossl"
#  GREEN_FRAMEWORK      The green framework to test with, if any.
#  COVERAGE             If non-empty, run the test suite with coverage.
#  COMPRESSORS          If non-empty, install appropriate compressor.
#  LIBMONGOCRYPT_URL    The URL to download libmongocrypt.
#  TEST_CRYPT_SHARED    If non-empty, install crypt_shared lib.
#  MONGODB_API_VERSION  The mongodb api version to use in tests.
#  MONGODB_URI          If non-empty, use as the MONGODB_URI in tests.
#  USE_ACTIVE_VENV      If non-empty, use the active virtual environment.

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})

# Try to source the env file.
if [ -f $SCRIPT_DIR/env.sh ]; then
  source $SCRIPT_DIR/env.sh
fi

echo "Setting up tests with args \"$*\"..."
uv run ${USE_ACTIVE_VENV:+--active} "$SCRIPT_DIR/setup_tests.py" "$@"
echo "Setting up tests with args \"$*\"... done."

#!/bin/bash
set -eu

# Supported/used environment variables:
#  AUTH                 Set to enable authentication. Defaults to "noauth"
#  SSL                  Set to enable SSL. Defaults to "nossl"
#  GREEN_FRAMEWORK      The green framework to test with, if any.
#  COVERAGE             If non-empty, run the test suite with coverage.
#  COMPRESSORS          If non-empty, install appropriate compressor.
#  LIBMONGOCRYPT_URL    The URL to download libmongocrypt.
#  TEST_DATA_LAKE       If non-empty, run data lake tests.
#  TEST_ENCRYPTION      If non-empty, run encryption tests.
#  TEST_CRYPT_SHARED    If non-empty, install crypt_shared lib.
#  TEST_SERVERLESS      If non-empy, test on serverless.
#  TEST_LOADBALANCER    If non-empy, test load balancing.
#  TEST_FLE_AZURE_AUTO  If non-empy, test auto FLE on Azure
#  TEST_FLE_GCP_AUTO    If non-empy, test auto FLE on GCP
#  TEST_PYOPENSSL       If non-empy, test with PyOpenSSL
#  TEST_ENTERPRISE_AUTH If non-empty, test with Enterprise Auth
#  TEST_AUTH_AWS        If non-empty, test AWS Auth Mechanism
#  TEST_AUTH_OIDC       If non-empty, test OIDC Auth Mechanism
#  TEST_PERF            If non-empty, run performance tests
#  TEST_OCSP            If non-empty, run OCSP tests
#  TEST_ATLAS           If non-empty, test Atlas connections
#  TEST_INDEX_MANAGEMENT        If non-empty, run index management tests
#  TEST_ENCRYPTION_PYOPENSSL    If non-empy, test encryption with PyOpenSSL
#  PERF_TEST            If non-empty, run the performance tests.
#  MONGODB_URI          If non-empty, use as the MONGODB_URI in tests.
#  PYTHON_BINARY        The python binary to use in tests.

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
ROOT_DIR="$(dirname $(dirname $SCRIPT_DIR))"

. $ROOT_DIR/.evergreen/utils.sh
PYTHON=${PYTHON_BINAR:-$(find_python3)}
$PYTHON $SCRIPT_DIR/setup-tests.py

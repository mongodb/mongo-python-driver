#!/bin/bash

# Disable xtrace
set +x
if [ -n "${MONGODB_STARTED}" ]; then
  export PYMONGO_MUST_CONNECT=true
fi
if [ -n "${DISABLE_TEST_COMMANDS}" ]; then
  export PYMONGO_DISABLE_TEST_COMMANDS=1
fi
if [ -n "${test_encryption}" ]; then
  # Disable xtrace (just in case it was accidentally set).
  set +x
  bash "${DRIVERS_TOOLS}"/.evergreen/csfle/await-servers.sh
  export TEST_ENCRYPTION=1
  if [ -n "${test_encryption_pyopenssl}" ]; then
    export TEST_ENCRYPTION_PYOPENSSL=1
  fi
fi
if [ -n "${test_crypt_shared}" ]; then
  export TEST_CRYPT_SHARED=1
  export CRYPT_SHARED_LIB_PATH=${CRYPT_SHARED_LIB_PATH}
fi
if [ -n "${test_pyopenssl}" ]; then
  export TEST_PYOPENSSL=1
fi
if [ -n "${SETDEFAULTENCODING}" ]; then
  export SETDEFAULTENCODING="${SETDEFAULTENCODING}"
fi
if [ -n "${test_loadbalancer}" ]; then
  export TEST_LOADBALANCER=1
  export SINGLE_MONGOS_LB_URI="${SINGLE_MONGOS_LB_URI}"
  export MULTI_MONGOS_LB_URI="${MULTI_MONGOS_LB_URI}"
fi
if [ -n "${test_serverless}" ]; then
  export TEST_SERVERLESS=1
fi
if [ -n "${TEST_INDEX_MANAGEMENT:-}" ]; then
  export TEST_INDEX_MANAGEMENT=1
fi
if [ -n "${SKIP_CSOT_TESTS}" ]; then
  export SKIP_CSOT_TESTS=1
fi
GREEN_FRAMEWORK=${GREEN_FRAMEWORK} \
  PYTHON_BINARY=${PYTHON_BINARY} \
  NO_EXT=${NO_EXT} \
  COVERAGE=${COVERAGE} \
  COMPRESSORS=${COMPRESSORS} \
  AUTH=${AUTH} \
  SSL=${SSL} \
  TEST_DATA_LAKE=${TEST_DATA_LAKE:-} \
  TEST_SUITES=${TEST_SUITES:-} \
  MONGODB_API_VERSION=${MONGODB_API_VERSION} \
  bash "${PROJECT_DIRECTORY}"/.evergreen/test.sh test-eg

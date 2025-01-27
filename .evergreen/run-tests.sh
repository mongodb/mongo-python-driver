#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -o xtrace

# Note: It is assumed that you have already set up a virtual environment before running this file.

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

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
TEST_SUITES=${TEST_SUITES:-}
TEST_ARGS=("${*:1}")

export PIP_QUIET=1  # Quiet by default
export PIP_PREFER_BINARY=1 # Prefer binary dists by default

set +x
PYTHON_IMPL=$(uv run --frozen python -c "import platform; print(platform.python_implementation())")

# Try to source local Drivers Secrets
if [ -f ./secrets-export.sh ]; then
  echo "Sourcing secrets"
  source ./secrets-export.sh
else
  echo "Not sourcing secrets"
fi

# Start compiling the args we'll pass to uv.
# Run in an isolated environment so as not to pollute the base venv.
UV_ARGS=("--isolated --frozen --extra test")

# Ensure C extensions if applicable.
if [ -z "${NO_EXT:-}" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    uv run --frozen tools/fail_if_no_c.py
fi

if [ "$AUTH" != "noauth" ]; then
    if [ -n "$TEST_DATA_LAKE" ]; then
        export DB_USER="mhuser"
        export DB_PASSWORD="pencil"
    elif [ -n "$TEST_SERVERLESS" ]; then
        source "${DRIVERS_TOOLS}"/.evergreen/serverless/secrets-export.sh
        export DB_USER=$SERVERLESS_ATLAS_USER
        export DB_PASSWORD=$SERVERLESS_ATLAS_PASSWORD
        export MONGODB_URI="$SERVERLESS_URI"
        echo "MONGODB_URI=$MONGODB_URI"
        export SINGLE_MONGOS_LB_URI=$MONGODB_URI
        export MULTI_MONGOS_LB_URI=$MONGODB_URI
    elif [ -n "$TEST_AUTH_OIDC" ]; then
        export DB_USER=$OIDC_ADMIN_USER
        export DB_PASSWORD=$OIDC_ADMIN_PWD
        export DB_IP="$MONGODB_URI"
    else
        export DB_USER="bob"
        export DB_PASSWORD="pwd123"
    fi
    echo "Added auth, DB_USER: $DB_USER"
fi

if [ -n "$TEST_ENTERPRISE_AUTH" ]; then
    UV_ARGS+=("--extra gssapi")
    if [ "Windows_NT" = "$OS" ]; then
        echo "Setting GSSAPI_PASS"
        export GSSAPI_PASS=${SASL_PASS}
        export GSSAPI_CANONICALIZE="true"
    else
        # BUILD-3830
        touch krb5.conf.empty
        export KRB5_CONFIG=${PROJECT_DIRECTORY}/.evergreen/krb5.conf.empty

        echo "Writing keytab"
        echo ${KEYTAB_BASE64} | base64 -d > ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab
        echo "Running kinit"
        kinit -k -t ${PROJECT_DIRECTORY}/.evergreen/drivers.keytab -p ${PRINCIPAL}
    fi
    echo "Setting GSSAPI variables"
    export GSSAPI_HOST=${SASL_HOST}
    export GSSAPI_PORT=${SASL_PORT}
    export GSSAPI_PRINCIPAL=${PRINCIPAL}

    export TEST_SUITES="auth"
fi

if [ -n "$TEST_LOADBALANCER" ]; then
    export LOAD_BALANCER=1
    export SINGLE_MONGOS_LB_URI="${SINGLE_MONGOS_LB_URI:-mongodb://127.0.0.1:8000/?loadBalanced=true}"
    export MULTI_MONGOS_LB_URI="${MULTI_MONGOS_LB_URI:-mongodb://127.0.0.1:8001/?loadBalanced=true}"
    export TEST_SUITES="load_balancer"
fi

if [ "$SSL" != "nossl" ]; then
    export CLIENT_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
    export CA_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"

    if [ -n "$TEST_LOADBALANCER" ]; then
        export SINGLE_MONGOS_LB_URI="${SINGLE_MONGOS_LB_URI}&tls=true"
        export MULTI_MONGOS_LB_URI="${MULTI_MONGOS_LB_URI}&tls=true"
    fi
fi

if [ "$COMPRESSORS" = "snappy" ]; then
    UV_ARGS+=("--extra snappy")
elif [ "$COMPRESSORS" = "zstd" ]; then
    UV_ARGS+=("--extra zstandard")
fi

# PyOpenSSL test setup.
if [ -n "$TEST_PYOPENSSL" ]; then
    UV_ARGS+=("--extra ocsp")
fi

if [ -n "$TEST_ENCRYPTION" ] || [ -n "$TEST_FLE_AZURE_AUTO" ] || [ -n "$TEST_FLE_GCP_AUTO" ]; then
    # Check for libmongocrypt download.
    if [ ! -d "libmongocrypt" ]; then
        echo "Run encryption setup first!"
        exit 1
    fi

    UV_ARGS+=("--extra encryption")
    # TODO: Test with 'pip install pymongocrypt'
    UV_ARGS+=("--group pymongocrypt_source")

    # Use the nocrypto build to avoid dependency issues with older windows/python versions.
    BASE=$(pwd)/libmongocrypt/nocrypto
    if [ -f "${BASE}/lib/libmongocrypt.so" ]; then
        PYMONGOCRYPT_LIB=${BASE}/lib/libmongocrypt.so
    elif [ -f "${BASE}/lib/libmongocrypt.dylib" ]; then
        PYMONGOCRYPT_LIB=${BASE}/lib/libmongocrypt.dylib
    elif [ -f "${BASE}/bin/mongocrypt.dll" ]; then
        PYMONGOCRYPT_LIB=${BASE}/bin/mongocrypt.dll
        # libmongocrypt's windows dll is not marked executable.
        chmod +x $PYMONGOCRYPT_LIB
        PYMONGOCRYPT_LIB=$(cygpath -m $PYMONGOCRYPT_LIB)
    elif [ -f "${BASE}/lib64/libmongocrypt.so" ]; then
        PYMONGOCRYPT_LIB=${BASE}/lib64/libmongocrypt.so
    else
        echo "Cannot find libmongocrypt shared object file"
        exit 1
    fi
    export PYMONGOCRYPT_LIB
    # Ensure pymongocrypt is working properly.
    # shellcheck disable=SC2048
    uv run ${UV_ARGS[*]} python -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    # shellcheck disable=SC2048
    uv run ${UV_ARGS[*]} python -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is updated by configure-env.sh for access to mongocryptd.
fi

if [ -n "$TEST_ENCRYPTION" ]; then
    if [ -n "$TEST_ENCRYPTION_PYOPENSSL" ]; then
        UV_ARGS+=("--extra ocsp")
    fi

    if [ -n "$TEST_CRYPT_SHARED" ]; then
        CRYPT_SHARED_DIR=`dirname $CRYPT_SHARED_LIB_PATH`
        echo "using crypt_shared_dir $CRYPT_SHARED_DIR"
        export DYLD_FALLBACK_LIBRARY_PATH=$CRYPT_SHARED_DIR:$DYLD_FALLBACK_LIBRARY_PATH
        export LD_LIBRARY_PATH=$CRYPT_SHARED_DIR:$LD_LIBRARY_PATH
        export PATH=$CRYPT_SHARED_DIR:$PATH
    fi
    # Only run the encryption tests.
    TEST_SUITES="encryption"
fi

if [ -n "$TEST_FLE_AZURE_AUTO" ] || [ -n "$TEST_FLE_GCP_AUTO" ]; then
    if [[ -z "$SUCCESS" ]]; then
        echo "Must define SUCCESS"
        exit 1
    fi

    if echo "$MONGODB_URI" | grep -q "@"; then
      echo "MONGODB_URI unexpectedly contains user credentials in FLE test!";
      exit 1
    fi
    TEST_SUITES="csfle"
fi

if [ -n "$TEST_INDEX_MANAGEMENT" ]; then
    source $DRIVERS_TOOLS/.evergreen/atlas/secrets-export.sh
    export DB_USER="${DRIVERS_ATLAS_LAMBDA_USER}"
    set +x
    export DB_PASSWORD="${DRIVERS_ATLAS_LAMBDA_PASSWORD}"
    set -x
    TEST_SUITES="index_management"
fi

# shellcheck disable=SC2128
if [ -n "$TEST_DATA_LAKE" ] && [ -z "$TEST_ARGS" ]; then
    TEST_SUITES="data_lake"
fi

if [ -n "$TEST_ATLAS" ]; then
    TEST_SUITES="atlas"
fi

if [ -n "$TEST_OCSP" ]; then
    UV_ARGS+=("--extra ocsp")
    TEST_SUITES="ocsp"
fi

if [ -n "$TEST_AUTH_AWS" ]; then
    UV_ARGS+=("--extra aws")
    TEST_SUITES="auth_aws"
fi

if [ -n "$TEST_AUTH_OIDC" ]; then
    UV_ARGS+=("--extra aws")
    TEST_SUITES="auth_oidc"
fi

if [ -n "$PERF_TEST" ]; then
    UV_ARGS+=("--group perf")
    start_time=$(date +%s)
    TEST_SUITES="perf"
    # PYTHON-4769 Run perf_test.py directly otherwise pytest's test collection negatively
    # affects the benchmark results.
    TEST_ARGS+=("test/performance/perf_test.py")
fi

echo "Running $AUTH tests over $SSL with python $(uv python find)"
uv run --frozen python -c 'import sys; print(sys.version)'


# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.

# Run the tests with coverage if requested and coverage is installed.
# Only cover CPython. PyPy reports suspiciously low coverage.
if [ -n "$COVERAGE" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    # Keep in sync with combine-coverage.sh.
    # coverage >=5 is needed for relative_files=true.
    UV_ARGS+=("--group coverage")
    TEST_ARGS+=("--cov")
fi

if [ -n "$GREEN_FRAMEWORK" ]; then
    UV_ARGS+=("--group $GREEN_FRAMEWORK")
fi

# Show the installed packages
# shellcheck disable=SC2048
PIP_QUIET=0 uv run ${UV_ARGS[*]} --with pip pip list

if [ -z "$GREEN_FRAMEWORK" ]; then
    # Use --capture=tee-sys so pytest prints test output inline:
    # https://docs.pytest.org/en/stable/how-to/capture-stdout-stderr.html
    PYTEST_ARGS=("-v" "--capture=tee-sys" "--durations=5" "${TEST_ARGS[@]}")
    if [ -n "$TEST_SUITES" ]; then
      # Workaround until unittest -> pytest conversion is complete
      if [[ "$TEST_SUITES" == *"default_async"* ]]; then
        ASYNC_PYTEST_ARGS=("-m asyncio" "--junitxml=xunit-results/TEST-asyncresults.xml" "${PYTEST_ARGS[@]}")
      else
        ASYNC_PYTEST_ARGS=("-m asyncio and $TEST_SUITES" "--junitxml=xunit-results/TEST-asyncresults.xml" "${PYTEST_ARGS[@]}")
      fi
      PYTEST_ARGS=("-m $TEST_SUITES and not asyncio" "${PYTEST_ARGS[@]}")
    else
      ASYNC_PYTEST_ARGS=("-m asyncio" "--junitxml=xunit-results/TEST-asyncresults.xml" "${PYTEST_ARGS[@]}")
    fi
    # Workaround until unittest -> pytest conversion is complete
    set +o errexit
    # shellcheck disable=SC2048
    uv run ${UV_ARGS[*]} pytest "${PYTEST_ARGS[@]}"
    exit_code=$?

    # shellcheck disable=SC2048
    uv run ${UV_ARGS[*]} pytest "${ASYNC_PYTEST_ARGS[@]}" "--collect-only"
    collected=$?
    set -o errexit
    # If we collected at least one async test, run all collected tests
    if [ $collected -ne 5 ]; then
      # shellcheck disable=SC2048
      uv run ${UV_ARGS[*]} pytest "${ASYNC_PYTEST_ARGS[@]}"
    fi
    if [ $exit_code -ne 0 ]; then
      exit $exit_code
    fi
else
    # shellcheck disable=SC2048
    uv run ${UV_ARGS[*]} green_framework_test.py $GREEN_FRAMEWORK -v "${TEST_ARGS[@]}"
fi

# Handle perf test post actions.
if [ -n "$PERF_TEST" ]; then
    end_time=$(date +%s)
    elapsed_secs=$((end_time-start_time))

    cat results.json

    echo "{\"failures\": 0, \"results\": [{\"status\": \"pass\", \"exit_code\": 0, \"test_file\": \"BenchMarkTests\", \"start\": $start_time, \"end\": $end_time, \"elapsed\": $elapsed_secs}]}" > report.json

    cat report.json
fi

# Handle coverage post actions.
if [ -n "$COVERAGE" ]; then
    rm -rf .pytest_cache
fi

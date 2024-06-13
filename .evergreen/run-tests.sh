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
TEST_ARGS="${*:1}"

export PIP_QUIET=1  # Quiet by default
export PIP_PREFER_BINARY=1 # Prefer binary dists by default

python -c "import sys; sys.exit(sys.prefix == sys.base_prefix)" || (echo "Not inside a virtual env!"; exit 1)

# Try to source local Drivers Secrets
if [ -f ./secrets-export.sh ]; then
  echo "Sourcing secrets"
  source ./secrets-export.sh
else
  echo "Not sourcing secrets"
fi

if [ "$AUTH" != "noauth" ]; then
    set +x
    if [ ! -z "$TEST_DATA_LAKE" ]; then
        export DB_USER="mhuser"
        export DB_PASSWORD="pencil"
    elif [ ! -z "$TEST_SERVERLESS" ]; then
        source ${DRIVERS_TOOLS}/.evergreen/serverless/secrets-export.sh
        export DB_USER=$SERVERLESS_ATLAS_USER
        export DB_PASSWORD=$SERVERLESS_ATLAS_PASSWORD
        export MONGODB_URI="$SERVERLESS_URI"
        echo "MONGODB_URI=$MONGODB_URI"
        export SINGLE_MONGOS_LB_URI=$MONGODB_URI
        export MULTI_MONGOS_LB_URI=$MONGODB_URI
    elif [ ! -z "$TEST_AUTH_OIDC" ]; then
        export DB_USER=$OIDC_ADMIN_USER
        export DB_PASSWORD=$OIDC_ADMIN_PWD
        export DB_IP="$MONGODB_URI"
    else
        export DB_USER="bob"
        export DB_PASSWORD="pwd123"
    fi
    echo "Added auth, DB_USER: $DB_USER"
    set -x
fi

if [ -n "$TEST_ENTERPRISE_AUTH" ]; then
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
fi

if [ -n "$TEST_LOADBALANCER" ]; then
    export LOAD_BALANCER=1
    export SINGLE_MONGOS_LB_URI="${SINGLE_MONGOS_LB_URI:-mongodb://127.0.0.1:8000/?loadBalanced=true}"
    export MULTI_MONGOS_LB_URI="${MULTI_MONGOS_LB_URI:-mongodb://127.0.0.1:8001/?loadBalanced=true}"
    export TEST_ARGS="test/test_load_balancer.py"
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
    python -m pip install '.[snappy]'
elif [ "$COMPRESSORS" = "zstd" ]; then
    python -m pip install zstandard
fi

# PyOpenSSL test setup.
if [ -n "$TEST_PYOPENSSL" ]; then
    python -m pip install '.[ocsp]'
fi

if [ -n "$TEST_ENCRYPTION" ] || [ -n "$TEST_FLE_AZURE_AUTO" ] || [ -n "$TEST_FLE_GCP_AUTO" ]; then

    python -m pip install '.[encryption]'

    # Install libmongocrypt if necessary.
    if [ ! -d "libmongocrypt" ]; then
        bash ./.evergreen/setup-libmongocrypt.sh
    fi

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

    # TODO: Test with 'pip install pymongocrypt'
    if [ ! -d "libmongocrypt_git" ]; then
      git clone https://github.com/mongodb/libmongocrypt.git libmongocrypt_git
    fi
    python -m pip install -U setuptools
    python -m pip install ./libmongocrypt_git/bindings/python
    python -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    python -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is updated by PREPARE_SHELL for access to mongocryptd.
fi

if [ -n "$TEST_ENCRYPTION" ]; then
    if [ -n "$TEST_ENCRYPTION_PYOPENSSL" ]; then
        python -m pip install '.[ocsp]'
    fi

    if [ -n "$TEST_CRYPT_SHARED" ]; then
        CRYPT_SHARED_DIR=`dirname $CRYPT_SHARED_LIB_PATH`
        echo "using crypt_shared_dir $CRYPT_SHARED_DIR"
        export DYLD_FALLBACK_LIBRARY_PATH=$CRYPT_SHARED_DIR:$DYLD_FALLBACK_LIBRARY_PATH
        export LD_LIBRARY_PATH=$CRYPT_SHARED_DIR:$LD_LIBRARY_PATH
        export PATH=$CRYPT_SHARED_DIR:$PATH
    fi
    # Only run the encryption tests.
    if [ -z "$TEST_ARGS" ]; then
        TEST_ARGS="test/test_encryption.py"
    fi
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

    if [ -z "$TEST_ARGS" ]; then
        TEST_ARGS="test/test_on_demand_csfle.py"
    fi
fi

if [ -n "$TEST_INDEX_MANAGEMENT" ]; then
    source $DRIVERS_TOOLS/.evergreen/atlas/secrets-export.sh
    export DB_USER="${DRIVERS_ATLAS_LAMBDA_USER}"
    set +x
    export DB_PASSWORD="${DRIVERS_ATLAS_LAMBDA_PASSWORD}"
    set -x
    TEST_ARGS="test/test_index_management.py"
fi

if [ -n "$TEST_DATA_LAKE" ] && [ -z "$TEST_ARGS" ]; then
    TEST_ARGS="test/test_data_lake.py"
fi

if [ -n "$TEST_ATLAS" ]; then
    TEST_ARGS="test/atlas/test_connection.py"
fi

if [ -n "$TEST_OCSP" ]; then
    python -m pip install ".[ocsp]"
    TEST_ARGS="test/ocsp/test_ocsp.py"
fi

if [ -n "$TEST_AUTH_AWS" ]; then
    python -m pip install ".[aws]"
    TEST_ARGS="test/auth_aws/test_auth_aws.py"
fi

if [ -n "$TEST_AUTH_OIDC" ]; then
    python -m pip install ".[aws]"
    TEST_ARGS="test/auth_oidc/test_auth_oidc.py $TEST_ARGS"
fi

if [ -n "$PERF_TEST" ]; then
    python -m pip install simplejson
    start_time=$(date +%s)
    TEST_ARGS="test/performance/perf_test.py"
fi

echo "Running $AUTH tests over $SSL with python $(which python)"
python -c 'import sys; print(sys.version)'


# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.

# Run the tests with coverage if requested and coverage is installed.
# Only cover CPython. PyPy reports suspiciously low coverage.
PYTHON_IMPL=$(python -c "import platform; print(platform.python_implementation())")
if [ -n "$COVERAGE" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    # Keep in sync with combine-coverage.sh.
    # coverage >=5 is needed for relative_files=true.
    python -m pip install pytest-cov "coverage>=5,<=7.5"
    TEST_ARGS="$TEST_ARGS --cov"
fi

if [ -n "$GREEN_FRAMEWORK" ]; then
    python -m pip install $GREEN_FRAMEWORK
fi

# Show the installed packages
PIP_QUIET=0 python -m pip list

if [ -z "$GREEN_FRAMEWORK" ]; then
    # Use --capture=tee-sys so pytest prints test output inline:
    # https://docs.pytest.org/en/stable/how-to/capture-stdout-stderr.html
    python -m pytest -v --capture=tee-sys --durations=5 --maxfail=10 $TEST_ARGS
else
    python green_framework_test.py $GREEN_FRAMEWORK -v $TEST_ARGS
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

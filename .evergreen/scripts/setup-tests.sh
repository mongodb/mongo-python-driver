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
#  MONGODB_URI          If non-empty, use as the MONGODB_URI in tests
#  SUCCESS              Flag used to indicate whether a test is expected to pass or fail.
#  PYTHON_BINARY        The python binary to use in tests.

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
ROOT_DIR="$(dirname $(dirname $SCRIPT_DIR))"

pushd $ROOT_DIR > /dev/null

# Clear any existing files.
rm -rf $ROOT_DIR/secrets-export.sh
rm -rf $SCRIPT_DIR/test-env.sh

# Handle default values
AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
TEST_ENCRYPTION="${TEST_ENCRYPTION:-}"
TEST_ENCRYPTION_PYOPENSSL="${TEST_ENCRYPTION_PYOPENSSL:-}"
TEST_CRYPT_SHARED="${TEST_CRYPT_SHARED:-}"
TEST_PYOPENSSL="${TEST_PYOPENSSL:-}"
TEST_LOAD_BALANCER="${TEST_LOAD_BALANCER:-}"
TEST_SERVERLESS="${TEST_SERVERLESS:-}"
TEST_INDEX_MANAGEMENT="${TEST_INDEX_MANAGEMENT:-}"
TEST_ENTERPRISE_AUTH="${TEST_ENTERPRISE_AUTH:-}"
TEST_DATA_LAKE="${TEST_DATA_LAKE:-}"
COMPRESSORS="${COMPRESSORS:-}"
SUCCESS="${SUCCESS:-}"
MONGODB_URI="${MONGODB_URI:-}"
PERF_TEST="${PERF_TEST:-}"
GREEN_FRAMEWORK="${GREEN_FRAMEWORK:-}"
PYTHON_BINARY="${PYTHON_BINARY:-}"

function _write_env() {
  echo "export $1=${2:-}" >> $SCRIPT_DIR/test-env.sh
}

_write_env TEST_ENCRYPTION $TEST_ENCRYPTION
_write_env TEST_ENCRYPTION_PYOPENSSL $TEST_ENCRYPTION_PYOPENSSL
_write_env TEST_CRYPT_SHARED $TEST_CRYPT_SHARED
_write_env TEST_PYOPENSSL $TEST_PYOPENSSL
_write_env TEST_LOAD_BALANCER $TEST_LOAD_BALANCER
_write_env TEST_SERVERLESS $TEST_SERVERLESS
_write_env TEST_INDEX_MANAGEMENT $TEST_INDEX_MANAGEMENT
_write_env TEST_ENTERPRISE_AUTH $TEST_ENTERPRISE_AUTH
_write_env TEST_DATA_LAKE $TEST_DATA_LAKE
_write_env PYTHON_BINARY $PYTHON_BINARY
_write_env COMPRESSORS $COMPRESSORS
_write_env SUCCESS $SUCCESS
_write_env MONGODB_URI $MONGODB_URI
_write_env PERF_TEST $PERF_TEST
_write_env GREEN_FRAMEWORK $GREEN_FRAMEWORK
_write_env PYTHON_BINARY $PYTHON_BINARY

chmod +x "$SCRIPT_DIR"/test-env.sh

TEST_SUITES=${TEST_SUITES:-}
TEST_ARGS="${*:1}"
# Start compiling the args we'll pass to uv.
# Run in an isolated environment so as not to pollute the base venv.
UV_ARGS=("--isolated --extra test")

if [ "$AUTH" != "noauth" ]; then
    if [ -n "$TEST_DATA_LAKE" ]; then
        DB_USER="mhuser"
        _write_env DB_PASSWORD "pencil"
    elif [ -n "$TEST_SERVERLESS" ]; then
        source "${DRIVERS_TOOLS}"/.evergreen/serverless/secrets-export.sh
        DB_USER=$SERVERLESS_ATLAS_USER
        _write_env DB_PASSWORD $SERVERLESS_ATLAS_PASSWORD
        _write_env MONGODB_URI $SERVERLESS_URI
        _write_env SINGLE_MONGOS_LB_URI $MONGODB_URI
        _write_env MULTI_MONGOS_LB_URI $MONGODB_URI
    elif [ -n "$TEST_AUTH_OIDC" ]; then
        DB_USER=$OIDC_ADMIN_USER
        _write_env DB_PASSWORD $OIDC_ADMIN_PWD
        _write_env DB_IP "$MONGODB_URI"
    elif [ -n "$TEST_INDEX_MANAGEMENT" ]; then
        source $DRIVERS_TOOLS/.evergreen/atlas/secrets-export.sh
        DB_USER="${DRIVERS_ATLAS_LAMBDA_USER}"
        _write_env DB_PASSWORD "${DRIVERS_ATLAS_LAMBDA_PASSWORD}"
    else
        DB_USER="bob"
        _write_env DB_PASSWORD "pwd123"
    fi
    _write_env DB_USER $DB_USER
    echo "Added auth, DB_USER: $DB_USER"
fi

if [ -n "$TEST_ENTERPRISE_AUTH" ]; then
    UV_ARGS+=("--extra gssapi")
    if [ "Windows_NT" = "$OS" ]; then
        echo "Setting GSSAPI_PASS"
        _write_env GSSAPI_PASS ${SASL_PASS}
        _write_env GSSAPI_CANONICALIZE "true"
    else
        # BUILD-3830
        touch krb5.conf.empty
        _write_env KRB5_CONFIG ${ROOT_DIR}/.evergreen/krb5.conf.empty

        echo "Writing keytab"
        echo ${KEYTAB_BASE64} | base64 -d > ${ROOT_DIR}/.evergreen/drivers.keytab
        echo "Running kinit"
        kinit -k -t ${ROOT_DIR}/.evergreen/drivers.keytab -p ${PRINCIPAL}
    fi
    echo "Setting GSSAPI variables"
    _write_env GSSAPI_HOST ${SASL_HOST}
    _write_env GSSAPI_PORT ${SASL_PORT}
    _write_env GSSAPI_PRINCIPAL ${PRINCIPAL}

    TEST_SUITES="auth"
fi

if [ -n "$TEST_LOADBALANCER" ]; then
    _write_env LOAD_BALANCER 1
    _write_env SINGLE_MONGOS_LB_URI "${SINGLE_MONGOS_LB_URI:-mongodb://127.0.0.1:8000/?loadBalanced=true}"
    _write_env MULTI_MONGOS_LB_URI "${MULTI_MONGOS_LB_URI:-mongodb://127.0.0.1:8001/?loadBalanced=true}"
    TEST_SUITES="load_balancer"
    MONGODB_URI=${MONGODB_URI} bash "${DRIVERS_TOOLS}"/.evergreen/run-load-balancer.sh start
fi

if [ "$SSL" != "nossl" ]; then
    _write_env CLIENT_PEM "$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
    _write_env CA_PEM "$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"

    if [ -n "$TEST_LOADBALANCER" ]; then
        _write_env SINGLE_MONGOS_LB_URI "${SINGLE_MONGOS_LB_URI}&tls=true"
        _write_env MULTI_MONGOS_LB_URI "${MULTI_MONGOS_LB_URI}&tls=true"
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
        bash $SCRIPT_DIR/setup-libmongocrypt.sh
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
    _write_env PYMONGOCRYPT_LIB $PYMONGOCRYPT_LIB
    # PATH is updated by configure-env.sh for access to mongocryptd.
fi

if [ -n "$TEST_ENCRYPTION" ]; then
    bash "${DRIVERS_TOOLS}"/.evergreen/csfle/setup-secrets.sh
    bash "${DRIVERS_TOOLS}"/.evergreen/csfle/start-servers.sh

    if [ -n "$TEST_ENCRYPTION_PYOPENSSL" ]; then
        UV_ARGS+=("--extra ocsp")
    fi

    if [ -n "$TEST_CRYPT_SHARED" ]; then
        CRYPT_SHARED_DIR=`dirname $CRYPT_SHARED_LIB_PATH`
        echo "using crypt_shared_dir $CRYPT_SHARED_DIR"
        _write_env DYLD_FALLBACK_LIBRARY_PATH $CRYPT_SHARED_DIR:$DYLD_FALLBACK_LIBRARY_PATH
        _write_env LD_LIBRARY_PATH $CRYPT_SHARED_DIR:$LD_LIBRARY_PATH
        _write_env PATH "$CRYPT_SHARED_DIR:$PATH"
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
    TEST_SUITES="index_management"
fi

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
    TEST_SUITES="perf"
    # PYTHON-4769 Run perf_test.py directly otherwise pytest's test collection negatively
    # affects the benchmark results.
    TEST_ARGS="test/performance/perf_test.py $TEST_ARGS"
fi

# Add coverage if requested.
# Only cover CPython. PyPy reports suspiciously low coverage.
if [ -n "$COVERAGE" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    # Keep in sync with combine-coverage.sh.
    # coverage >=5 is needed for relative_files=true.
    UV_ARGS+=("--group coverage")
    TEST_ARGS="$TEST_ARGS --cov"
fi

if [ -n "$GREEN_FRAMEWORK" ]; then
    UV_ARGS+=("--group $GREEN_FRAMEWORK")
fi

if [ -z "$GREEN_FRAMEWORK" ]; then
    # Use --capture=tee-sys so pytest prints test output inline:
    # https://docs.pytest.org/en/stable/how-to/capture-stdout-stderr.html
    TEST_ARGS="-v --capture=tee-sys --durations=5 $TEST_ARGS"
    if [ -n "$TEST_SUITES" ]; then
      TEST_ARGS="-m $TEST_SUITES $TEST_ARGS"
    fi
fi

_write_env TEST_ARGS $TEST_ARGS
_write_env UV_ARGS "${UV_ARGS[*]}"

popd > /dev/null

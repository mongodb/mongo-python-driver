#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

# Note: It is assumed that you have already set up a virtual environment before running this file.

# Supported/used environment variables:
#  SET_XTRACE_ON        Set to non-empty to write all commands first to stderr.
#  AUTH                 Set to enable authentication. Defaults to "noauth"
#  SSL                  Set to enable SSL. Defaults to "nossl"
#  GREEN_FRAMEWORK      The green framework to test with, if any.
#  C_EXTENSIONS         If non-empty, c extensions are enabled.
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
#  TEST_ENCRYPTION_PYOPENSSL    If non-empy, test encryption with PyOpenSSL

if [ -n "${SET_XTRACE_ON}" ]; then
    set -o xtrace
else
    set +x
fi

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
TEST_ARGS="$1"
PYTHON=$(which python)

python -c "import sys; sys.exit(sys.prefix == sys.base_prefix)" || (echo "Not inside a virtual env!"; exit 1)

if [ "$AUTH" != "noauth" ]; then
    if [ ! -z "$TEST_DATA_LAKE" ]; then
        export DB_USER="mhuser"
        export DB_PASSWORD="pencil"
    elif [ ! -z "$TEST_SERVERLESS" ]; then
        export DB_USER=$SERVERLESS_ATLAS_USER
        export DB_PASSWORD=$SERVERLESS_ATLAS_PASSWORD
    else
        export DB_USER="bob"
        export DB_PASSWORD="pwd123"
    fi
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
    pip install '.[snappy]'
    PYTHON=python
elif [ "$COMPRESSORS" = "zstd" ]; then
    pip install zstandard
fi

# PyOpenSSL test setup.
if [ -n "$TEST_PYOPENSSL" ]; then
    pip install '.[ocsp]'
fi

if [ -n "$TEST_ENCRYPTION" ] || [ -n "$TEST_FLE_AZURE_AUTO" ] || [ -n "$TEST_FLE_GCP_AUTO" ]; then

    # Work around for root certifi not being installed.
    # TODO: Remove after PYTHON-3827
    if [ "$(uname -s)" = "Darwin" ]; then
        pip install certifi
        CERT_PATH=$(python -c "import certifi; print(certifi.where())")
        export SSL_CERT_FILE=${CERT_PATH}
        export REQUESTS_CA_BUNDLE=${CERT_PATH}
        export AWS_CA_BUNDLE=${CERT_PATH}
    fi

    pip install '.[encryption]'

    if [ "Windows_NT" = "$OS" ]; then # Magic variable in cygwin
        # PYTHON-2808 Ensure this machine has the CA cert for google KMS.
        powershell.exe "Invoke-WebRequest -URI https://oauth2.googleapis.com/" > /dev/null || true
    fi

    if [ -z "$LIBMONGOCRYPT_URL" ]; then
        echo "Cannot test client side encryption without LIBMONGOCRYPT_URL!"
        exit 1
    fi
    curl -O "$LIBMONGOCRYPT_URL"
    mkdir libmongocrypt
    tar xzf libmongocrypt.tar.gz -C ./libmongocrypt
    ls -la libmongocrypt
    ls -la libmongocrypt/nocrypto
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
    git clone https://github.com/mongodb/libmongocrypt.git libmongocrypt_git
    python -m pip install --prefer-binary -r .evergreen/test-encryption-requirements.txt
    python -m pip install ./libmongocrypt_git/bindings/python
    python -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    python -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is updated by PREPARE_SHELL for access to mongocryptd.
fi

if [ -n "$TEST_ENCRYPTION" ]; then
    if [ -n "$TEST_ENCRYPTION_PYOPENSSL" ]; then
        pip install '.[ocsp]'
    fi

    # Get access to the AWS temporary credentials:
    # CSFLE_AWS_TEMP_ACCESS_KEY_ID, CSFLE_AWS_TEMP_SECRET_ACCESS_KEY, CSFLE_AWS_TEMP_SESSION_TOKEN
    . $DRIVERS_TOOLS/.evergreen/csfle/set-temp-creds.sh

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
    TEST_ARGS="test/test_index_management.py"
fi

if [ -n "$TEST_DATA_LAKE" ] && [ -z "$TEST_ARGS" ]; then
    TEST_ARGS="test/test_data_lake.py"
fi

echo "Running $AUTH tests over $SSL with python $PYTHON"
python -c 'import sys; print(sys.version)'

# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.

# Run the tests with coverage if requested and coverage is installed.
# Only cover CPython. PyPy reports suspiciously low coverage.
PYTHON_IMPL=$($PYTHON -c "import platform; print(platform.python_implementation())")
if [ -n "$COVERAGE" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    python -m pip install pytest-cov
    TEST_ARGS="$TEST_ARGS --cov pymongo --cov-branch --cov-report term-missing:skip-covered"
fi

if [ -z "$GREEN_FRAMEWORK" ]; then
    if [ -z "$C_EXTENSIONS" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
        python setup.py build_ext -i
        # This will set a non-zero exit status if either import fails,
        # causing this script to exit.
        python -c "from bson import _cbson; from pymongo import _cmessage"
    fi

    python -m pytest -v $TEST_ARGS
else
    python -m pip install $GREEN_FRAMEWORK
    python green_framework_test.py $GREEN_FRAMEWORK
fi

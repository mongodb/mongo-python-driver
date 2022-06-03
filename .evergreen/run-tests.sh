#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  SET_XTRACE_ON      Set to non-empty to write all commands first to stderr.
#  AUTH               Set to enable authentication. Defaults to "noauth"
#  SSL                Set to enable SSL. Defaults to "nossl"
#  PYTHON_BINARY      The Python version to use. Defaults to whatever is available
#  GREEN_FRAMEWORK    The green framework to test with, if any.
#  C_EXTENSIONS       Pass --no_ext to setup.py, or not.
#  COVERAGE           If non-empty, run the test suite with coverage.
#  TEST_ENCRYPTION    If non-empty, install pymongocrypt.
#  LIBMONGOCRYPT_URL  The URL to download libmongocrypt.
#  TEST_CRYPT_SHARED  If non-empty, install crypt_shared lib.

if [ -n "${SET_XTRACE_ON}" ]; then
    set -o xtrace
else
    set +x
fi

AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
PYTHON_BINARY=${PYTHON_BINARY:-}
GREEN_FRAMEWORK=${GREEN_FRAMEWORK:-}
C_EXTENSIONS=${C_EXTENSIONS:-}
COVERAGE=${COVERAGE:-}
COMPRESSORS=${COMPRESSORS:-}
MONGODB_API_VERSION=${MONGODB_API_VERSION:-}
TEST_ENCRYPTION=${TEST_ENCRYPTION:-}
TEST_CRYPT_SHARED=${TEST_CRYPT_SHARED:-}
LIBMONGOCRYPT_URL=${LIBMONGOCRYPT_URL:-}
DATA_LAKE=${DATA_LAKE:-}
TEST_ARGS=""

if [ -n "$COMPRESSORS" ]; then
    export COMPRESSORS=$COMPRESSORS
fi

if [ -n "$MONGODB_API_VERSION" ]; then
    export MONGODB_API_VERSION=$MONGODB_API_VERSION
fi

if [ "$AUTH" != "noauth" ]; then
    if [ ! -z "$DATA_LAKE" ]; then
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

# For createvirtualenv.
. .evergreen/utils.sh

if [ -z "$PYTHON_BINARY" ]; then
    # Use Python 3 from the server toolchain to test on ARM, POWER or zSeries if a
    # system python3 doesn't exist or exists but is older than 3.7.
    if is_python_37 "$(command -v python3)"; then
        PYTHON=$(command -v python3)
    elif is_python_37 "$(command -v /opt/mongodbtoolchain/v3/bin/python3)"; then
        PYTHON=$(command -v /opt/mongodbtoolchain/v3/bin/python3)
    else
        echo "Cannot test without python3.7+ installed!"
    fi
elif [ "$COMPRESSORS" = "snappy" ]; then
    createvirtualenv $PYTHON_BINARY snappytest
    trap "deactivate; rm -rf snappytest" EXIT HUP
    python -m pip install python-snappy
    PYTHON=python
elif [ "$COMPRESSORS" = "zstd" ]; then
    createvirtualenv $PYTHON_BINARY zstdtest
    trap "deactivate; rm -rf zstdtest" EXIT HUP
    python -m pip install zstandard
    PYTHON=python
else
    PYTHON="$PYTHON_BINARY"
fi

# PyOpenSSL test setup.
if [ -n "$TEST_PYOPENSSL" ]; then
    createvirtualenv $PYTHON pyopenssltest
    trap "deactivate; rm -rf pyopenssltest" EXIT HUP
    PYTHON=python

    python -m pip install --prefer-binary pyopenssl requests service_identity
fi

if [ -n "$TEST_ENCRYPTION" ]; then
    createvirtualenv $PYTHON venv-encryption
    trap "deactivate; rm -rf venv-encryption" EXIT HUP
    PYTHON=python

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
    git clone --branch PYTHON-3285 https://github.com/ShaneHarvey/libmongocrypt.git libmongocrypt_git
    python -m pip install --prefer-binary -r .evergreen/test-encryption-requirements.txt
    python -m pip install ./libmongocrypt_git/bindings/python
    python -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    python -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is updated by PREPARE_SHELL for access to mongocryptd.

    # Get access to the AWS temporary credentials:
    # CSFLE_AWS_TEMP_ACCESS_KEY_ID, CSFLE_AWS_TEMP_SECRET_ACCESS_KEY, CSFLE_AWS_TEMP_SESSION_TOKEN
    . $DRIVERS_TOOLS/.evergreen/csfle/set-temp-creds.sh

    if [ -n "$TEST_CRYPT_SHARED" ]; then
        echo "Testing CSFLE with crypt_shared lib"
        $PYTHON $DRIVERS_TOOLS/.evergreen/mongodl.py --component crypt_shared \
            --version latest --out ../crypt_shared/
        export DYLD_FALLBACK_LIBRARY_PATH=../crypt_shared/lib/:$DYLD_FALLBACK_LIBRARY_PATH
        export LD_LIBRARY_PATH=../crypt_shared/lib:$LD_LIBRARY_PATH
        export PATH=../crypt_shared/bin:$PATH
    fi
    # Only run the encryption tests.
    TEST_ARGS="-s test.test_encryption"
fi

if [ -n "$DATA_LAKE" ]; then
    TEST_ARGS="-s test.test_data_lake"
fi

# Don't download unittest-xml-reporting from pypi, which often fails.
if $PYTHON -c "import xmlrunner"; then
    # The xunit output dir must be a Python style absolute path.
    XUNIT_DIR="$(pwd)/xunit-results"
    if [ "Windows_NT" = "$OS" ]; then # Magic variable in cygwin
        XUNIT_DIR=$(cygpath -m $XUNIT_DIR)
    fi
    OUTPUT="--xunit-output=${XUNIT_DIR}"
else
    OUTPUT=""
fi

echo "Running $AUTH tests over $SSL with python $PYTHON"
$PYTHON -c 'import sys; print(sys.version)'

# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.

# Run the tests with coverage if requested and coverage is installed.
# Only cover CPython. PyPy reports suspiciously low coverage.
PYTHON_IMPL=$($PYTHON -c "import platform; print(platform.python_implementation())")
COVERAGE_ARGS=""
if [ -n "$COVERAGE" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    if $PYTHON -m coverage --version; then
        echo "INFO: coverage is installed, running tests with coverage..."
        COVERAGE_ARGS="-m coverage run --branch"
    else
        echo "INFO: coverage is not installed, running tests without coverage..."
    fi
fi

$PYTHON setup.py clean
if [ -z "$GREEN_FRAMEWORK" ]; then
    if [ -z "$C_EXTENSIONS" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
        # Fail if the C extensions fail to build.

        # This always sets 0 for exit status, even if the build fails, due
        # to our hack to install PyMongo without C extensions when build
        # deps aren't available.
        $PYTHON setup.py build_ext -i
        # This will set a non-zero exit status if either import fails,
        # causing this script to exit.
        $PYTHON -c "from bson import _cbson; from pymongo import _cmessage"
    fi

    $PYTHON $COVERAGE_ARGS setup.py $C_EXTENSIONS test $TEST_ARGS $OUTPUT
else
    # --no_ext has to come before "test" so there is no way to toggle extensions here.
    $PYTHON green_framework_test.py $GREEN_FRAMEWORK $OUTPUT
fi

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
#  SETDEFAULTENCODING The encoding to set via sys.setdefaultencoding.

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
LIBMONGOCRYPT_URL=${LIBMONGOCRYPT_URL:-}
SETDEFAULTENCODING=${SETDEFAULTENCODING:-}
DATA_LAKE=${DATA_LAKE:-}

if [ -n "$COMPRESSORS" ]; then
    export COMPRESSORS=$COMPRESSORS
fi

if [ -n "$MONGODB_API_VERSION" ]; then
    export MONGODB_API_VERSION=$MONGODB_API_VERSION
fi


export JAVA_HOME=/opt/java/jdk8

if [ "$AUTH" != "noauth" ]; then
    if [ -z "$DATA_LAKE" ]; then
        export DB_USER="bob"
        export DB_PASSWORD="pwd123"
    else
        export DB_USER="mhuser"
        export DB_PASSWORD="pencil"
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
    VIRTUALENV=$(command -v virtualenv) || true
    if [ -z "$VIRTUALENV" ]; then
        PYTHON=$(command -v python || command -v python3) || true
        if [ -z "$PYTHON" ]; then
            echo "Cannot test without python or python3 installed!"
            exit 1
        fi
    else
        $VIRTUALENV --never-download pymongotestvenv
        . pymongotestvenv/bin/activate
        PYTHON=python
        trap "deactivate; rm -rf pymongotestvenv" EXIT HUP
    fi
elif [ "$COMPRESSORS" = "snappy" ]; then
    $PYTHON_BINARY -m virtualenv --system-site-packages --never-download snappytest
    . snappytest/bin/activate
    trap "deactivate; rm -rf snappytest" EXIT HUP
    # 0.5.2 has issues in pypy3(.5)
    pip install python-snappy==0.5.1
    PYTHON=python
elif [ "$COMPRESSORS" = "zstd" ]; then
    $PYTHON_BINARY -m virtualenv --system-site-packages --never-download zstdtest
    . zstdtest/bin/activate
    trap "deactivate; rm -rf zstdtest" EXIT HUP
    pip install zstandard
    PYTHON=python
elif [ -n "$SETDEFAULTENCODING" ]; then
    $PYTHON_BINARY -m virtualenv --system-site-packages --never-download encodingtest
    . encodingtest/bin/activate
    trap "deactivate; rm -rf encodingtest" EXIT HUP
    mkdir test-sitecustomize
    cat <<EOT > test-sitecustomize/sitecustomize.py
import sys
sys.setdefaultencoding("$SETDEFAULTENCODING")
EOT
    export PYTHONPATH="$(pwd)/test-sitecustomize"
    PYTHON=python
else
    PYTHON="$PYTHON_BINARY"
fi

# PyOpenSSL test setup.
if [ -n "$TEST_PYOPENSSL" ]; then
    createvirtualenv $PYTHON pyopenssltest
    trap "deactivate; rm -rf pyopenssltest" EXIT HUP
    PYTHON=python

    python -m pip install --prefer-binary -r .evergreen/test-pyopenssl-requirements.txt
fi

if [ -n "$TEST_ENCRYPTION" ]; then
    createvirtualenv $PYTHON venv-encryption
    trap "deactivate; rm -rf venv-encryption" EXIT HUP
    PYTHON=python

    if [ "Windows_NT" = "$OS" ]; then # Magic variable in cygwin
        $PYTHON -m pip install -U setuptools
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
        export PYMONGOCRYPT_LIB=${BASE}/lib/libmongocrypt.so
    elif [ -f "${BASE}/lib/libmongocrypt.dylib" ]; then
        export PYMONGOCRYPT_LIB=${BASE}/lib/libmongocrypt.dylib
    elif [ -f "${BASE}/bin/mongocrypt.dll" ]; then
        PYMONGOCRYPT_LIB=${BASE}/bin/mongocrypt.dll
        # libmongocrypt's windows dll is not marked executable.
        chmod +x $PYMONGOCRYPT_LIB
        export PYMONGOCRYPT_LIB=$(cygpath -m $PYMONGOCRYPT_LIB)
    elif [ -f "${BASE}/lib64/libmongocrypt.so" ]; then
        export PYMONGOCRYPT_LIB=${BASE}/lib64/libmongocrypt.so
    else
        echo "Cannot find libmongocrypt shared object file"
        exit 1
    fi

    # TODO: Test with 'pip install pymongocrypt'
    git clone --branch master https://github.com/mongodb/libmongocrypt.git libmongocrypt_git
    python -m pip install --prefer-binary -r .evergreen/test-encryption-requirements.txt
    python -m pip install ./libmongocrypt_git/bindings/python
    python -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    python -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is updated by PREPARE_SHELL for access to mongocryptd.

    # Get access to the AWS temporary credentials:
    # CSFLE_AWS_TEMP_ACCESS_KEY_ID, CSFLE_AWS_TEMP_SECRET_ACCESS_KEY, CSFLE_AWS_TEMP_SESSION_TOKEN
    . $DRIVERS_TOOLS/.evergreen/csfle/set-temp-creds.sh
fi

PYTHON_IMPL=$($PYTHON -c "import platform, sys; sys.stdout.write(platform.python_implementation())")
if [ $PYTHON_IMPL = "Jython" ]; then
    PYTHON_ARGS="-J-XX:-UseGCOverheadLimit -J-Xmx4096m"
else
    PYTHON_ARGS=""
fi

if [ -z "$DATA_LAKE" ]; then
    TEST_ARGS=""
else
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
# Only cover CPython. Jython and PyPy report suspiciously low coverage.
# Also skip CPython 3.4. It's not supported by coverage 5+, which uses
# a new and incompatible data format.
PYTHON_VERSION=$($PYTHON -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
COVERAGE_ARGS=""
if [ -n "$COVERAGE" -a $PYTHON_IMPL = "CPython" -a $PYTHON_VERSION != "3.4" ]; then
    if $PYTHON -m coverage --version; then
        echo "INFO: coverage is installed, running tests with coverage..."
        COVERAGE_ARGS="-m coverage run --branch"
    else
        echo "INFO: coverage is not installed, running tests without coverage..."
    fi
fi

$PYTHON setup.py clean
if [ -z "$GREEN_FRAMEWORK" ]; then
    if [ -z "$C_EXTENSIONS" -a $PYTHON_IMPL = "CPython" ]; then
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

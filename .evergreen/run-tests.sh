#!/bin/sh
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
TEST_ENCRYPTION=${TEST_ENCRYPTION:-}
LIBMONGOCRYPT_URL=${LIBMONGOCRYPT_URL:-}
SETDEFAULTENCODING=${SETDEFAULTENCODING:-}

if [ -n "$COMPRESSORS" ]; then
    export COMPRESSORS=$COMPRESSORS
fi

export JAVA_HOME=/opt/java/jdk8

if [ "$AUTH" != "noauth" ]; then
    export DB_USER="bob"
    export DB_PASSWORD="pwd123"
fi

if [ "$SSL" != "nossl" ]; then
    export CLIENT_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
    export CA_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"
fi

if [ -z "$PYTHON_BINARY" ]; then
    VIRTUALENV=$(command -v virtualenv) || true
    if [ -z "$VIRTUALENV" ]; then
        PYTHON=$(command -v python || command -v python3) || true
        if [ -z "$PYTHON" ]; then
            echo "Cannot test without python or python3 installed!"
            exit 1
        fi
    else
        $VIRTUALENV pymongotestvenv
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
    if $PYTHON -m virtualenv --version; then
        VIRTUALENV="$PYTHON -m virtualenv"
    elif command -v virtualenv; then
        # We can remove this fallback after:
        # https://github.com/10gen/mongo-python-toolchain/issues/8
        VIRTUALENV="$(command -v virtualenv) -p $PYTHON"
    else
        echo "Cannot test without virtualenv"
        exit 1
    fi

    $VIRTUALENV pyopenssltest
    if [ "Windows_NT" = "$OS" ]; then
        . pyopenssltest/Scripts/activate
    else
        . pyopenssltest/bin/activate
    fi
    trap "deactivate; rm -rf pyopenssltest" EXIT HUP

    IS_PYTHON_2=$(python -c "import sys; sys.stdout.write('1' if sys.version_info < (3,) else '0')")
    if [ $IS_PYTHON_2 = "1" ]; then
        echo "Using a Python 2"
        # Upgrade pip to install the cryptography wheel and not the tar.
        # <20.1 because 20.0.2 says a future release may drop support for 2.7.
        python -m pip install --upgrade 'pip<20.1'
        # Upgrade setuptools because cryptography requires 18.5+.
        # <45 because 45.0 dropped support for 2.7.
        python -m pip install --upgrade 'setuptools<45'
    fi

    python -m pip install pyopenssl requests service_identity
fi

if [ -n "$TEST_ENCRYPTION" ]; then
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

    git clone --branch master git@github.com:mongodb/libmongocrypt.git libmongocrypt_git
    $PYTHON -m pip install --upgrade ./libmongocrypt_git/bindings/python
    # TODO: use a virtualenv
    trap "$PYTHON -m pip uninstall -y pymongocrypt" EXIT HUP
    $PYTHON -c "import pymongocrypt; print('pymongocrypt version: '+pymongocrypt.__version__)"
    $PYTHON -c "import pymongocrypt; print('libmongocrypt version: '+pymongocrypt.libmongocrypt_version())"
    # PATH is set by PREPARE_SHELL.
fi

PYTHON_IMPL=$($PYTHON -c "import platform, sys; sys.stdout.write(platform.python_implementation())")
if [ $PYTHON_IMPL = "Jython" ]; then
    EXTRA_ARGS="-J-XX:-UseGCOverheadLimit -J-Xmx4096m"
else
    EXTRA_ARGS=""
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
COVERAGE_OR_PYTHON="$PYTHON"
COVERAGE_ARGS=""
if [ -n "$COVERAGE" -a $PYTHON_IMPL = "CPython" ]; then
    COVERAGE_BIN="$(dirname "$PYTHON")/coverage"
    if $COVERAGE_BIN --version; then
        echo "INFO: coverage is installed, running tests with coverage..."
        COVERAGE_OR_PYTHON="$COVERAGE_BIN"
        COVERAGE_ARGS="run --branch"
    else
        echo "INFO: coverage is not installed, running tests without coverage..."
    fi
fi

if $PYTHON -c 'import dns'; then
  # Trying with/without --user to avoid:
  # ERROR: Can not perform a '--user' install. User site-packages are not visible in this virtualenv.
  $PYTHON -m pip install --upgrade --user 'dnspython<2.0.0' || $PYTHON -m pip install --upgrade 'dnspython<2.0.0'
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
    $COVERAGE_OR_PYTHON $EXTRA_ARGS $COVERAGE_ARGS setup.py $C_EXTENSIONS test $OUTPUT
else
    # --no_ext has to come before "test" so there is no way to toggle extensions here.
    $PYTHON green_framework_test.py $GREEN_FRAMEWORK $OUTPUT
fi

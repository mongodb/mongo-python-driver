#!/bin/sh
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  SET_XTRACE_ON      Set to non-empty to write all commands first to stderr.
#  AUTH               Set to enable authentication. Defaults to "noauth"
#  SSL                Set to enable SSL. Defaults to "nossl"
#  PYTHON_BINARY      The Python version to use. Defaults to whatever is available
#  C_EXTENSIONS       Pass --no_ext to setup.py, or not.

if [ -n "${SET_XTRACE_ON}" ]; then
    set -o xtrace
else
    set +x
fi

PYTHON="$PYTHON_BINARY"
AUTH=${AUTH:-noauth}
PYTHON_BINARY=${PYTHON_BINARY:-}
C_EXTENSIONS=${C_EXTENSIONS:-}

if [ "$AUTH" != "noauth" ]; then
    export DB_USER="mhuser"
    export DB_PASSWORD="pencil"
fi

echo "Running $AUTH tests over $SSL with python $PYTHON"
$PYTHON -c 'import sys; print(sys.version)'

PYTHON_IMPL=$($PYTHON -c "import platform, sys; sys.stdout.write(platform.python_implementation())")

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

$PYTHON setup.py clean
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

echo "Running tests"
$PYTHON setup.py $C_EXTENSIONS test -s test.data_lake.test_data_lake $OUTPUT

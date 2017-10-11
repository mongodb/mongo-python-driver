#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Defaults to "noauth"
#       SSL                     Set to enable SSL. Defaults to "nossl"
#       PYTHON_BINARY           The Python version to use. Defaults to whatever is available
#       GREEN_FRAMEWORK         The green framework to test with, if any.
#       C_EXTENSIONS            Pass --no_ext to setup.py, or not.
#       COVERAGE                If non-empty, run the test suite with coverage.


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
PYTHON_BINARY=${PYTHON_BINARY:-}
GREEN_FRAMEWORK=${GREEN_FRAMEWORK:-}
C_EXTENSIONS=${C_EXTENSIONS:-}
COVERAGE=${COVERAGE:-}

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
    PYTHON=$(command -v python || command -v python3) || true
    if [ -z "$PYTHON" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
else
    PYTHON="$PYTHON_BINARY"
fi

PYTHON_IMPL=$($PYTHON -c "import platform, sys; sys.stdout.write(platform.python_implementation())")
if [ $PYTHON_IMPL = "Jython" ]; then
    EXTRA_ARGS="-J-XX:-UseGCOverheadLimit -J-Xmx4096m"
else
    EXTRA_ARGS=""
fi

# Don't download unittest-xml-reporting from pypi, which often fails.
HAVE_XMLRUNNER=$($PYTHON -c "import pkgutil, sys; sys.stdout.write('1' if pkgutil.find_loader('xmlrunner') else '0')")
if [ $HAVE_XMLRUNNER = "1" ]; then
    OUTPUT="--xunit-output=xunit-results"
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

#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Defaults to "noauth"
#       SSL                     Set to enable SSL. Defaults to "nossl"
#       PYTHON_BINARY           The Python version to use. Defaults to whatever is available
#       C_EXTENSIONS            Pass --no_ext to setup.py, or not.


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
PYTHON_BINARY=${PYTHON_BINARY:-}
C_EXTENSIONS=${C_EXTENSIONS:-}

export JAVA_HOME=/opt/java/jdk8

if [ "$AUTH" != "noauth" ]; then
    export DB_USER="bob"
    export DB_PASSWORD="pwd123"
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

PYTHON_VERSION=$($PYTHON -c 'import sys; sys.stdout.write(str(sys.version_info[0]))')
PLATFORM=$($PYTHON -c 'import platform, sys; sys.stdout.write(platform.system())')

if [ "$SSL" = "ssl" ]; then
    if [ "$PYTHON_VERSION" = "3" ]; then
        # We cannot pass arguments to the "nosetests" command with Python 3
        # because of the hacks in setup.py to work around nose/2to3 usage.
        # Instead, use the "test" command directly to run only test/test_ssl.py.
        # Unfortunately, this does not produce XML output.
        TEST_CMD="test --test-suite test.test_ssl"
    else
        # With Python 2 we use the nosetests command
        # Run only test/test_ssl.py and produces nosetests.xml output.
        TEST_CMD="nosetests --tests test/test_ssl.py"
    fi
else
    # Run all the tests and produces nosetests.xml output.
    TEST_CMD="nosetests"
fi

# Set verbose test output flag.
if [ "$PYTHON_VERSION" = "3" ]; then
    # With Python 3, the tests do not accept a "--verbosity=2" flag.
    TEST_VERBOSITY="-v"
else
    # With Python 2, the tests accepts a "-v" flag but only "--verbosity=2"
    # causes the verbose output we want.
    TEST_VERBOSITY="--verbosity=2"
fi

if [ "$PLATFORM" = "Java" ]; then
    # Keep Jython 2.5 from running out of memory.
    EXTRA_ARGS="-J-XX:-UseGCOverheadLimit -J-Xmx4096m"
else
    EXTRA_ARGS=""
fi

echo "Running $AUTH tests over $SSL with python $PYTHON"
$PYTHON -c 'import sys; print(sys.version)'

# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.

$PYTHON setup.py clean
$PYTHON $EXTRA_ARGS setup.py $C_EXTENSIONS $TEST_CMD $TEST_VERBOSITY

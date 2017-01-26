#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Defaults to "noauth"
#       SSL                     Set to enable SSL. Defaults to "nossl"
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       PYTHON_BINARY           The Python version to use. Defaults to whatever is available


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}
PYTHON_BINARY=${PYTHON_BINARY:-}


if [ "$AUTH" != "noauth" ]; then
    export DB_USER="bob"
    export DB_PASSWORD="pwd123"
fi

if [ "$SSL" != "nossl" ]; then
    export CLIENT_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
    export CA_PEM="$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"
fi

if [ -z "$PYTHON_BINARY" ]; then
    PYTHON=$(command -v python || command -v python3)
    if [ -z "$PYTHON" ]; then
        echo "Cannot test without python or python3 installed!"
        exit 1
    fi
else
    PYTHON="$PYTHON_BINARY"
fi


echo "Running $AUTH tests over $SSL with python $PYTHON, connecting to $MONGODB_URI"
$PYTHON -c 'import sys; print(sys.version)'

# Run the tests, and store the results in Evergreen compatible XUnit XML
# files in the xunit-results/ directory.

$PYTHON setup.py clean
$PYTHON setup.py test --xunit-output=xunit-results

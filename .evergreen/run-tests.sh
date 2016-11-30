#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#       AUTH                    Set to enable authentication. Defaults to "noauth"
#       SSL                     Set to enable SSL. Defaults to "nossl"
#       MONGODB_URI             Set the suggested connection MONGODB_URI (including credentials and topology info)
#       MARCH                   Machine Architecture. Defaults to lowercase uname -m


AUTH=${AUTH:-noauth}
SSL=${SSL:-nossl}
MONGODB_URI=${MONGODB_URI:-}

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
[ -z "$MARCH" ] && MARCH=$(uname -m | tr '[:upper:]' '[:lower:]')


if [ "$AUTH" != "noauth" ]; then
  export MONGOC_TEST_USER="bob"
  export MONGOC_TEST_PASSWORD="pwd123"
fi

if [ "$SSL" != "nossl" ]; then
   export MONGOC_TEST_SSL_PEM_FILE="$DRIVERS_TOOLS/.evergreen/x509gen/client.pem"
   export MONGOC_TEST_SSL_CA_FILE="$DRIVERS_TOOLS/.evergreen/x509gen/ca.pem"
fi

echo "Running $AUTH tests over $SSL, connecting to $MONGODB_URI"

# Run the tests, and store the results in a Evergreen compatible JSON results file
case "$OS" in
   cygwin*)
      make test
      ;;

   sunos)
      gmake -o test-libmongoc test TEST_ARGS="--no-fork -d -F test-results.json"
      ;;

   *)
      make -o test-libmongoc test TEST_ARGS="--no-fork -d -F test-results.json"
      ;;
esac


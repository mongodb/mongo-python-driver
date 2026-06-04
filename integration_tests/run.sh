#!/bin/bash
# Run all of the integration test files using `uv run`.
set -eu

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Point run-mongodb.sh (and async_client_context) at our test certificates so
# the server and client agree on the CA, regardless of the CI tool's defaults.
export TLS_PEM_KEY_FILE="$ROOT/test/certificates/server.pem"
export TLS_CA_FILE="$ROOT/test/certificates/ca.pem"
export TLS_CERT_KEY_FILE="$ROOT/test/certificates/client.pem"

for file in integration_tests/test_*.py ; do
    echo "-----------------"
    echo "Running $file..."
    uv run $file
    echo "Running $file...done."
    echo "-----------------"
done

#!/bin/bash
set -eu

SCRIPT_DIR=$(dirname ${BASH_SOURCE:-$0})
ROOT_DIR="$(dirname $(dirname $HERE))"

if [ ! -f $SCRIPT_DIR/test-env.sh ]; then
    exit 0
fi
if [ -f $SCRIPT_DIR/env.sh ]; then
    source $SCRIPT_DIR/env.sh
fi

source $SCRIPT_DIR/test-env.sh

# Shut down csfle servers if applicable
if [ -n "${TEST_ENCRYPTION:-}" ]; then
    bash ${DRIVERS_TOOLS}/.evergreen/csfle/stop-servers.sh
fi

# Shut down load balancer if applicable.
if [ -n "${TEST_LOADBALANCER:-}" ]; then
    bash "${DRIVERS_TOOLS}"/.evergreen/run-load-balancer.sh stop
fi

# Remove temporary test files.
rm -rf libmongocrypt/ libmongocrypt.tar.gz mongocryptd.pid > /dev/null

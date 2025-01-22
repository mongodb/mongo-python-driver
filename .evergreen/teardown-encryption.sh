#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail
set -o xtrace

if [ -z "${DRIVERS_TOOLS}" ]; then
    echo "Missing environment variable DRIVERS_TOOLS"
fi

bash ${DRIVERS_TOOLS}/.evergreen/csfle/stop-servers.sh
rm -rf libmongocrypt/ libmongocrypt.tar.gz mongocryptd.pid

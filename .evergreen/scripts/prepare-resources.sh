#!/bin/bash
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd $HERE
. env.sh

rm -rf $DRIVERS_TOOLS
git clone https://github.com/mongodb-labs/drivers-evergreen-tools.git $DRIVERS_TOOLS
echo "{ \"releases\": { \"default\": \"$MONGODB_BINARIES\" }}" >$MONGO_ORCHESTRATION_HOME/orchestration.config

popd

# Copy PyMongo's test certificates over driver-evergreen-tools'
cp ${PROJECT_DIRECTORY}/test/certificates/* ${DRIVERS_TOOLS}/.evergreen/x509gen/

# Replace MongoOrchestration's client certificate.
cp ${PROJECT_DIRECTORY}/test/certificates/client.pem ${MONGO_ORCHESTRATION_HOME}/lib/client.pem

if [ -w /etc/hosts ]; then
  SUDO=""
else
  SUDO="sudo"
fi

# Add 'server' and 'hostname_not_in_cert' as a hostnames
echo "127.0.0.1 server" | $SUDO tee -a /etc/hosts
echo "127.0.0.1 hostname_not_in_cert" | $SUDO tee -a /etc/hosts

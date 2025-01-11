#!/bin/bash
set -eu

# Copy PyMongo's test certificates over driver-evergreen-tools'
cp ${PROJECT_DIRECTORY}/test/certificates/* ${DRIVERS_TOOLS}/.evergreen/x509gen/

# Replace MongoOrchestration's client certificate.
cp ${PROJECT_DIRECTORY}/test/certificates/client.pem ${MONGO_ORCHESTRATION_HOME}/lib/client.pem

if [ -w /etc/hosts ]; then
  SUDO=""
else
  SUDO="sudo"
fi

# Install just.
if [ ! -f $HOME/.local/bin/just ]; then
  curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to "$HOME/.local/bin" || {
    # CARGO_HOME is defined in configure-env.sh
    export CARGO_HOME=${CARGO_HOME:-$HOME/.cargo/}
    export RUSTUP_HOME="${CARGO_HOME}/.rustup"
    ${DRIVERS_TOOLS}/.evergreen/install-rust.sh
    cargo install just
    mv $CARGO_HOME/just $HOME/.local/bin
  }
fi

# Add 'server' and 'hostname_not_in_cert' as a hostnames
echo "127.0.0.1 server" | $SUDO tee -a /etc/hosts
echo "127.0.0.1 hostname_not_in_cert" | $SUDO tee -a /etc/hosts

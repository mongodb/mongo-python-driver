#!/bin/bash
# Script run on an ECS host to test MONGODB-AWS.
set -eu

############################################
#            Main Program                  #
############################################

if [[ -z "$1" ]]; then
    echo "usage: $0 <MONGODB_URI>"
    exit 1
fi
export MONGODB_URI="$1"

if echo "$MONGODB_URI" | grep -q "@"; then
  echo "MONGODB_URI unexpectedly contains user credentials in container test!";
  exit 1
fi
# Now we can safely enable xtrace
set -o xtrace

# Install python with pip.
PYTHON_VER="python3.11"
apt-get -qq update  < /dev/null > /dev/null
apt-get -qq install $PYTHON_VER $PYTHON_VER-venv build-essential $PYTHON_VER-dev -y  < /dev/null > /dev/null

export PYTHON_BINARY=$PYTHON_VER
export SET_XTRACE_ON=1
cd src
rm -rf .venv
rm -f .evergreen/scripts/test-env.sh || true
bash ./.evergreen/just.sh setup-tests auth_aws ecs-remote
bash .evergreen/just.sh run-tests

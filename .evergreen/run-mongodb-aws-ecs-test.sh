#!/bin/bash

# Don't trace since the URI contains a password that shouldn't show up in the logs
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

if [[ -z "$1" ]]; then
    echo "usage: $0 <MONGODB_URI>"
    exit 1
fi
export MONGODB_URI="$1"

if echo "$MONGODB_URI" | grep -q "@"; then
  echo "MONGODB_URI unexpectedly contains user credentials in ECS test!";
  exit 1
fi
# Now we can safely enable xtrace
set -o xtrace

# Install python with pip.
PYTHON_VER="python3.9"
apt-get -qq update  < /dev/null > /dev/null
apt-get -qq install $PYTHON_VER $PYTHON_VER-venv build-essential $PYTHON_VER-dev -y  < /dev/null > /dev/null

export PYTHON_BINARY=$PYTHON_VER
export TEST_AUTH_AWS=1
export AUTH="auth"
export SET_XTRACE_ON=1
cd src
rm -rf .venv
bash .evergreen/hatch.sh test:test-eg

#!/bin/bash -ex

set -o xtrace

# Make sure DRIVERS_TOOLS is set.
if [ -z "$DRIVERS_TOOLS" ]; then
    echo "Must specify DRIVERS_TOOLS"
    exit 1
fi

# Import venvcreate from $DRIVERS_TOOLS
. $DRIVERS_TOOLS/.evergreen/venv-utils.sh

# Usage:
# testinstall /path/to/python /path/to/.whl ["no-virtualenv"]
# * param1: Python binary to test
# * param2: Path to the wheel to install
# * param3 (optional): If set to a non-empty string, don't create a virtualenv. Used in manylinux containers.
testinstall () {
    PYTHON=$1
    RELEASE=$2
    NO_VIRTUALENV=$3

    if [ -z "$NO_VIRTUALENV" ]; then
        venvcreate $PYTHON venvtestinstall
        PYTHON=python
    fi

    $PYTHON -m pip install --upgrade $RELEASE
    cd tools
    $PYTHON fail_if_no_c.py
    $PYTHON -m pip uninstall -y pymongo
    cd ..

    if [ -z "$NO_VIRTUALENV" ]; then
        deactivate
        rm -rf venvtestinstall
    fi
}

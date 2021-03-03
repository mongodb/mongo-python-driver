#!/bin/bash -ex

# Usage:
# createvirtualenv /path/to/python /output/path/for/venv
# * param1: Python binary to use for the virtualenv
# * param2: Path to the virtualenv to create
createvirtualenv () {
    PYTHON=$1
    VENVPATH=$2
    if $PYTHON -m virtualenv --version; then
        VIRTUALENV="$PYTHON -m virtualenv --never-download"
    elif $PYTHON -m venv -h>/dev/null; then
        # System virtualenv might not be compatible with the python3 on our path
        VIRTUALENV="$PYTHON -m venv"
    else
        echo "Cannot test without virtualenv"
        exit 1
    fi
    $VIRTUALENV $VENVPATH
    if [ "Windows_NT" = "$OS" ]; then
        . $VENVPATH/Scripts/activate
    else
        . $VENVPATH/bin/activate
    fi
    python -m pip install --upgrade pip
    python -m pip install --upgrade setuptools wheel
}

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
        createvirtualenv $PYTHON venvtestinstall
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

# Function that returns success if the provided Python binary is version 3.6 or later
# Usage:
# is_python_36 /path/to/python
# * param1: Python binary
is_python_36() {
    if [ -z "$1" ]; then
        return 1
    elif $1 -c "import sys; exit(sys.version_info[:2] < (3, 6))"; then
        # runs when sys.version_info[:2] >= (3, 6)
        return 0
    else
        return 1
    fi
}

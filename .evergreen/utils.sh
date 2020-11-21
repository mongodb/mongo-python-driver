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
    elif [[ $(uname -s) == "Darwin" || "Windows_NT" == "$OS" ]] && $PYTHON -m venv -h >/dev/null; then
        # Only use venv on macOS or Windows, venv fails on some Linux distros.
        VIRTUALENV="$PYTHON -m venv"
    elif command -v virtualenv; then
        VIRTUALENV="$(command -v virtualenv) -p $PYTHON --never-download"
    else
        echo "Cannot test without virtualenv"
        exit 1
    fi
    $VIRTUALENV --system-site-packages $VENVPATH
    if [ "Windows_NT" = "$OS" ]; then
        # Workaround https://bugs.python.org/issue32451:
        # mongovenv/Scripts/activate: line 3: $'\r': command not found
        . $VENVPATH/Scripts/activate || (dos2unix $VENVPATH/Scripts/activate && . $VENVPATH/Scripts/activate)
    else
        . $VENVPATH/bin/activate
    fi
}

# Usage:
# testinstall /path/to/python /path/to/.whl/or/.egg ["no-virtualenv"]
# * param1: Python binary to test
# * param2: Path to the wheel or egg file to install
# * param3 (optional): If set to a non-empty string, don't create a virtualenv. Used in manylinux containers.
testinstall () {
    PYTHON=$1
    RELEASE=$2
    NO_VIRTUALENV=$3

    if [ -z "$NO_VIRTUALENV" ]; then
        createvirtualenv $PYTHON venvtestinstall
        PYTHON=python
    fi

    if [[ $RELEASE == *.egg ]]; then
        $PYTHON -m easy_install $RELEASE
    else
        $PYTHON -m pip install --upgrade $RELEASE
    fi
    cd tools
    $PYTHON fail_if_no_c.py
    $PYTHON -m pip uninstall -y pymongo
    cd ..

    if [ -z "$NO_VIRTUALENV" ]; then
        deactivate
        rm -rf venvtestinstall
    fi
}

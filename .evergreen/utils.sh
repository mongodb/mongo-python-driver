#!/bin/bash
# Utility functions used by pymongo evergreen scripts.
set -eu

find_python3() {
    PYTHON=""
    # Find a suitable toolchain version, if available.
    if [ "$(uname -s)" = "Darwin" ]; then
        PYTHON="/Library/Frameworks/Python.Framework/Versions/3.10/bin/python3"
    elif [ "Windows_NT" = "${OS:-}" ]; then # Magic variable in cygwin
        PYTHON="C:/python/Python310/python.exe"
    else
        # Prefer our own toolchain, fall back to mongodb toolchain if it has Python 3.10+.
        if [ -f "/opt/python/3.10/bin/python3" ]; then
            PYTHON="/opt/python/Current/bin/python3"
        elif is_python_310 "$(command -v /opt/mongodbtoolchain/v5/bin/python3)"; then
            PYTHON="/opt/mongodbtoolchain/v5/bin/python3"
        elif is_python_310 "$(command -v /opt/mongodbtoolchain/v4/bin/python3)"; then
            PYTHON="/opt/mongodbtoolchain/v4/bin/python3"
        elif is_python_310 "$(command -v /opt/mongodbtoolchain/v3/bin/python3)"; then
            PYTHON="/opt/mongodbtoolchain/v3/bin/python3"
        fi
    fi
    # Add a fallback system python3 if it is available and Python 3.10+.
    if [ -z "$PYTHON" ]; then
        if is_python_310 "$(command -v python3)"; then
            PYTHON="$(command -v python3)"
        fi
    fi
    if [ -z "$PYTHON" ]; then
        echo "Cannot test without python3.10+ installed!"
        exit 1
    fi
    echo "$PYTHON"
}

# Usage:
# createvirtualenv /path/to/python /output/path/for/venv
# * param1: Python binary to use for the virtualenv
# * param2: Path to the virtualenv to create
createvirtualenv () {
    PYTHON=$1
    VENVPATH=$2

    # Prefer venv
    VENV="$PYTHON -m venv"
    if [ "$(uname -s)" = "Darwin" ]; then
        VIRTUALENV="$PYTHON -m virtualenv"
    else
        VIRTUALENV=$(command -v virtualenv 2>/dev/null || echo "$PYTHON -m virtualenv")
        VIRTUALENV="$VIRTUALENV -p $PYTHON"
    fi
    if ! $VENV $VENVPATH 2>/dev/null; then
        # Workaround for bug in older versions of virtualenv.
        $VIRTUALENV $VENVPATH 2>/dev/null || $VIRTUALENV $VENVPATH
    fi
    if [ "Windows_NT" = "${OS:-}" ]; then
        # Workaround https://bugs.python.org/issue32451:
        # mongovenv/Scripts/activate: line 3: $'\r': command not found
        dos2unix $VENVPATH/Scripts/activate || true
        . $VENVPATH/Scripts/activate
    else
        . $VENVPATH/bin/activate
    fi

    export PIP_QUIET=1
    python -m pip install --upgrade pip
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
    PYTHON_IMPL=$(python -c "import platform; print(platform.python_implementation())")

    if [ -z "$NO_VIRTUALENV" ]; then
        createvirtualenv $PYTHON venvtestinstall
        PYTHON=python
    fi

    $PYTHON -m pip install --upgrade $RELEASE
    cd tools

    if [ "$PYTHON_IMPL" = "CPython" ]; then
        $PYTHON fail_if_no_c.py
    fi

    $PYTHON -m pip uninstall -y pymongo
    cd ..

    if [ -z "$NO_VIRTUALENV" ]; then
        deactivate
        rm -rf venvtestinstall
    fi
}

# Function that returns success if the provided Python binary is version 3.10 or later
# Usage:
# is_python_310 /path/to/python
# * param1: Python binary
is_python_310() {
    if [ -z "$1" ]; then
        return 1
    elif $1 -c "import sys; exit(sys.version_info[:2] < (3, 10))"; then
        # runs when sys.version_info[:2] >= (3, 10)
        return 0
    else
        return 1
    fi
}


# Function that gets a python binary given a python version string.
# Versions can be of the form 3.xx or pypy3.xx.
get_python_binary() {
    version=$1
    if [ "$(uname -s)" = "Darwin" ]; then
        if [[ "$version" == *"t"* ]]; then
            binary_name="python3t"
            framework_dir="PythonT"
        else
            binary_name="python3"
            framework_dir="Python"
        fi
        version=$(echo "$version" | sed 's/t//g')
        PYTHON="/Library/Frameworks/$framework_dir.Framework/Versions/$version/bin/$binary_name"
    elif [ "Windows_NT" = "${OS:-}" ]; then
        version=$(echo $version | cut -d. -f1,2 | sed 's/\.//g; s/t//g')
        if [ -n "${IS_WIN32:-}" ]; then
            PYTHON="C:/python/32/Python$version/python.exe"
        else
            PYTHON="C:/python/Python$version/python.exe"
        fi
    else
        PYTHON="/opt/python/$version/bin/python3"
    fi
    if is_python_310 "$(command -v $PYTHON)"; then
        echo "$PYTHON"
    else
        echo "Could not find suitable python binary for '$version'"  >&2
        return 1
    fi
}

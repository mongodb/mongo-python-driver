#!/bin/bash -ex

# Get access to testinstall.
. .evergreen/utils.sh

regex="cp3([[:digit:]]+)-.*universal2"

# Test each universal wheel.
for release in releases/*; do
    # Extract the python version from the file name.
    if [[ $release =~ $regex ]]; then
        VERSION="3.${BASH_REMATCH[1]}"
        PYTHON=/Library/Frameworks/Python.framework/Versions/$VERSION/bin/python3
        createvirtualenv $PYTHON releasevenv
        testinstall $PYTHON $release
        rm -rf releasevenv
    fi
done

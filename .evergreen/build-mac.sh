#!/bin/bash -ex

# Get access to testinstall.
. .evergreen/utils.sh

# Create temp directory for validated files.
rm -rf validdist
mkdir -p validdist
mv dist/* validdist || true

for VERSION in 2.7 3.4 3.5 3.6 3.7 3.8; do
    if [[ $VERSION == "2.7" ]]; then
        PYTHON=/System/Library/Frameworks/Python.framework/Versions/2.7/bin/python
        rm -rf build
        $PYTHON setup.py bdist_egg
    else
        PYTHON=/Library/Frameworks/Python.framework/Versions/$VERSION/bin/python3
    fi
    rm -rf build
    $PYTHON setup.py bdist_wheel
    for release in dist/*; do
        testinstall $PYTHON $release
        mv $release validdist/
    done
done

mv validdist/* dist
rm -rf validdist
ls dist

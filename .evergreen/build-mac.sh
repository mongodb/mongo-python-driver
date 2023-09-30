#!/bin/bash -ex

# Get access to testinstall.
. .evergreen/utils.sh

# Create temp directory for validated files.
rm -rf validdist
mkdir -p validdist
mv dist/* validdist || true

VERSION=${VERSION:-3.10}

PYTHON=/Library/Frameworks/Python.framework/Versions/$VERSION/bin/python3
rm -rf build

createvirtualenv $PYTHON releasevenv
python -m pip install build
python -m build --wheel .
deactivate || true
rm -rf releasevenv

# Test that each wheel is installable.
for release in dist/*; do
    testinstall $PYTHON $release
    mv $release validdist/
done

mv validdist/* dist
rm -rf validdist
ls dist

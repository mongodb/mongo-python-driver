#!/bin/bash -ex

# Get access to testinstall.
. .evergreen/utils.sh

# Create temp directory for validated files.
rm -rf validdist
mkdir -p validdist
mv dist/* validdist || true

VERSION=${VERSION:-3.10}
if [[ $VERSION == "2.7" ]]; then
    PYTHON=/System/Library/Frameworks/Python.framework/Versions/2.7/bin/python
    rm -rf build
    $PYTHON setup.py bdist_egg
else
    PYTHON=/Library/Frameworks/Python.framework/Versions/$VERSION/bin/python3
fi

rm -rf build

# Install wheel if not already there.
if ! $PYTHON -m wheel version; then
    createvirtualenv $PYTHON releasevenv
    WHEELPYTHON=python
    pip install --upgrade wheel setuptools==63.2.0
else
    WHEELPYTHON=$PYTHON
fi

$WHEELPYTHON setup.py bdist_wheel
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

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
python -m pip install --upgrade wheel
python -m pip install setuptools==63.2.0
python setup.py bdist_wheel
deactivate || true
rm -rf releasevenv

# Test that each wheel is installable.
for release in dist/*; do
    if [[ "$release" == "*universal2*" ]]; then
        export ENSURE_UNIVERSAL2="1"
    fi
    testinstall $PYTHON $release
    mv $release validdist/
done

mv validdist/* dist
rm -rf validdist
ls dist

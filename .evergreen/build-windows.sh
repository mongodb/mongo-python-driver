#!/bin/bash -ex

# Get access to testinstall.
. .evergreen/utils.sh

# Create temp directory for validated files.
rm -rf validdist
mkdir -p validdist
mv dist/* validdist || true

for VERSION in 36 37 38 39 310; do
    _pythons=(C:/Python/Python${VERSION}/python.exe \
              C:/Python/32/Python${VERSION}/python.exe)
    for PYTHON in "${_pythons[@]}"; do
        rm -rf build
        $PYTHON setup.py bdist_wheel

        # Test that each wheel is installable.
        for release in dist/*; do
            testinstall $PYTHON $release
            mv $release validdist/
        done
    done
done

mv validdist/* dist
rm -rf validdist
ls dist

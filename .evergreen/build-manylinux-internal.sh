#!/bin/bash -ex
cd /src

# Get access to testinstall.
. .evergreen/utils.sh

# Create temp directory for validated files.
rm -rf validdist
mkdir -p validdist
mv dist/* validdist || true

# Compile wheels
for PYTHON in /opt/python/*/bin/python; do
    if [[ ! $PYTHON =~ (cp34|cp35|cp36|cp37|cp38|cp39) ]]; then
        continue
    fi
    # https://github.com/pypa/manylinux/issues/49
    rm -rf build
    $PYTHON setup.py bdist_wheel
    rm -rf build

    # Audit wheels and write multilinux tag
    for whl in dist/*.whl; do
        # Skip already built manylinux1 wheels.
        if [[ "$whl" != *"manylinux"* ]]; then
            auditwheel repair $whl -w dist
            rm $whl
        fi
    done

    # Test that each wheel is installable.
    # Test without virtualenv because it's not present on manylinux containers.
    for release in dist/*; do
        testinstall $PYTHON $release "without-virtualenv"
        mv $release validdist/
    done
done

mv validdist/* dist
rm -rf validdist
ls dist

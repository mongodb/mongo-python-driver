#!/bin/bash -ex
cd /pymongo

# Compile wheels
for PYBIN in /opt/python/*/bin; do
    # Skip Python 3.3 and 3.9.
    if [[ "$PYBIN" == *"cp33"* || "$PYBIN" == *"cp39"* ]]; then
        continue
    fi
    # https://github.com/pypa/manylinux/issues/49
    rm -rf build
    ${PYBIN}/python setup.py bdist_wheel
done

# https://github.com/pypa/manylinux/issues/49
rm -rf build

# Audit wheels and write multilinux1 tag
for whl in dist/*.whl; do
    # Skip already built manylinux1 wheels.
    if [[ "$whl" != *"manylinux"* ]]; then
        auditwheel repair $whl -w dist
        rm $whl
    fi
done

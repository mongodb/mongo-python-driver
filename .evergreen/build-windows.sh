#!/bin/bash -ex

for VERSION in 27 34 35 36 37 38; do
    PYTHON=C:/Python/Python${VERSION}/python.exe
    PYTHON32=C:/Python/32/Python${VERSION}/python.exe
    if [[ $VERSION == "2.7" ]]; then
        rm -rf build
        $PYTHON setup.py bdist_egg
        rm -rf build
        $PYTHON32 setup.py bdist_egg
    fi
    rm -rf build
    $PYTHON setup.py bdist_wheel
    rm -rf build
    $PYTHON32 setup.py bdist_wheel
done

ls dist

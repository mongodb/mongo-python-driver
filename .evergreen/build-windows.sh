#!/bin/bash -ex

for VERSION in 27 34 35 36 37 38; do
    _pythons=(C:/Python/Python${VERSION}/python.exe \
              C:/Python/32/Python${VERSION}/python.exe)
    for PYTHON in "${_pythons[@]}"; do
        if [[ $VERSION == "2.7" ]]; then
            rm -rf build
            $PYTHON setup.py bdist_egg
        fi
        rm -rf build
        $PYTHON setup.py bdist_wheel
    done
done

ls dist

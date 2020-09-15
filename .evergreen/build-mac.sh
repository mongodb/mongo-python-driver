#!/bin/bash -ex

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
done

ls dist

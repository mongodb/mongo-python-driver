#!/bin/bash -ex

for VERSION in 2.7 3.4 3.5 3.6 3.7 3.8; do
    if [[ $VERSION == "2.7" ]]; then
        rm -rf build
        python$VERSION setup.py bdist_egg
    fi
    rm -rf build
    python$VERSION setup.py bdist_wheel
done

ls dist

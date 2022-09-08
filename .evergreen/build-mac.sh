#!/bin/bash -ex

# Get access to testinstall.
. .evergreen/utils.sh

# Create temp directory for validated files.
rm -rf validdist
mkdir -p validdist
mv dist/* validdist || true

for VERSION in 3.10; do
    PYTHON=/Library/Frameworks/Python.framework/Versions/$VERSION/bin/python3
    rm -rf build

    # # Set the arch flags appropriately for the target platform.
    # PLATFORM=$($PYTHON -c "import sysconfig;print(sysconfig.get_platform())")
    # if [[ $PLATFORM =~ "universal2" ]]; then
    #     _PYTHON_HOST_PLATFORM=macosx-10.9-universal2
    #     ARCHFLAGS="-arch arm64 -arch x86_64"
    # else
    #     unset _PYTHON_HOST_PLATFORM
    #     ARCHFLAGS="-arch x86_64"
    # fi

    createvirtualenv $PYTHON releasevenv
    python -m pip install --upgrade wheel
    python -m pip install setuptools==63.2.0
    python setup.py bdist_wheel
    deactivate || true
    rm -rf releasevenv

    # Test that each wheel is installable.
    for release in dist/*; do
        testinstall $PYTHON $release
        mv $release validdist/
    done
done

mv validdist/* dist
rm -rf validdist
ls dist

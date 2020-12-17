#!/bin/bash -ex

if [ $(uname -s) = "Darwin" ]; then
    .evergreen/build-mac.sh
elif [ "Windows_NT" = "$OS" ]; then # Magic variable in cygwin
    .evergreen/build-windows.sh
else
    .evergreen/build-manylinux.sh
fi

#!/bin/bash
# Get the debug data for an evergreen task.
set -eu

. ${DRIVERS_TOOLS}/.evergreen/get-distro.sh || true
get_distro || true
echo $DISTRO
echo $MARCH
echo $OS

set -x
uname -a || true
ls /etc/*release* || true
cc --version || true
gcc --version || true
clang --version || true
gcov --version || true
lcov --version || true
llvm-cov --version || true
echo $PATH
ls -la /usr/local/Cellar/llvm/*/bin/ || true
ls -la /usr/local/Cellar/ || true
scan-build --version || true
genhtml --version || true
valgrind --version || true
set +x

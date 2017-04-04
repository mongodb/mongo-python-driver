#!/bin/bash

set -o xtrace
set -o errexit

virtualenv -p ${PYTHON_BINARY} cdecimaltest
trap "deactivate; rm -rf cdecimaltest" EXIT HUP
. cdecimaltest/bin/activate
# No cdecimal tarballs on pypi.
pip install http://www.bytereef.org/software/mpdecimal/releases/cdecimal-2.3.tar.gz
python -c 'import sys; print(sys.version)'
python cdecimal_test.py

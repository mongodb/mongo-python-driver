#!/bin/bash -ex

cd /src
PYTHON=/opt/python/cp311-cp311/bin/python
$PYTHON -m pip install -v -e .

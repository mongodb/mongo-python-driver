#!/bin/bash -ex

cd /src
PYTHON=/opt/python/cp39-cp39/bin/python
$PYTHON -m pip install -v -e .

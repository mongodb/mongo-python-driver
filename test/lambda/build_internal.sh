#!/bin/bash -ex

cd /src
PYTHON=/opt/python/cp310-cp310/bin/python
$PYTHON -m pip install -v -e .

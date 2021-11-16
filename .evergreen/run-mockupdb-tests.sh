#!/bin/bash
# Must be run from pymongo repo root
set -o xtrace
set -o errexit

. .evergreen/utils.sh

${PYTHON_BINARY} setup.py clean

createvirtualenv ${PYTHON_BINARY} mockuptests
trap "deactivate, rm -rf mockuptests" EXIT HUP

# Install PyMongo from git clone so mockup-tests don't
# download it from pypi.
python -m pip install .
python -m pip install --upgrade 'https://github.com/ajdavis/mongo-mockup-db/archive/master.zip'
cd ./test/mockupdb
python -m unittest discover -v

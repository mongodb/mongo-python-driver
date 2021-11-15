#!/bin/bash

set -o xtrace
set -o errexit

. .evergreen/utils.sh

${PYTHON_BINARY} setup.py clean

createvirtualenv ${PYTHON_BINARY} mockuptests
trap "deactivatei, rm -rf mockuptests" EXIT HUP

# Install PyMongo from git clone so mockup-tests don't
# download it from pypi.
${PYTHON_BINARY} -m pip install ${PROJECT_DIRECTORY}
${PYTHON_BINARY} -m pip install pymongo mockupdb bson
echo $PWD
${PYTHON_BINARY} -m unittest discover ../tests/mockupdb

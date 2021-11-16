#!/bin/bash

set -o xtrace
set -o errexit
cd $PROJECT_DIRECTORY
. .evergreen/utils.sh

${PYTHON_BINARY} setup.py clean

createvirtualenv ${PYTHON_BINARY} mockuptests
trap "deactivatei, rm -rf mockuptests" EXIT HUP

# Install PyMongo from git clone so mockup-tests don't
# download it from pypi.
python -m pip install ${PROJECT_DIRECTORY}
python -m pip install mockupdb
cd ${PROJECT_DIRECTORY}/test/mockupdb
python -m unittest discover

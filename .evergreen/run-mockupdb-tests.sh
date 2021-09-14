#!/bin/bash

set -o xtrace
set -o errexit

. .evergreen/utils.sh

${PYTHON_BINARY} setup.py clean
cd ..

createvirtualenv ${PYTHON_BINARY} mockuptests
trap "deactivatei, rm -rf mockuptests" EXIT HUP

# Install PyMongo from git clone so mockup-tests don't
# download it from pypi.
python -m pip install ${PROJECT_DIRECTORY}

git clone https://github.com/ajdavis/pymongo-mockup-tests.git
cd pymongo-mockup-tests
python setup.py test

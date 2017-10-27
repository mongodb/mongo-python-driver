#!/bin/bash

set -o xtrace
set -o errexit

${PYTHON_BINARY} setup.py clean
cd ..
${PYTHON_BINARY} -m virtualenv mockuptests
. mockuptests/bin/activate
trap "deactivate" EXIT HUP

# Install PyMongo from git clone so mockup-tests don't
# download it from pypi.
pip install ${PROJECT_DIRECTORY}

git clone https://github.com/ajdavis/pymongo-mockup-tests.git
cd pymongo-mockup-tests
python setup.py test

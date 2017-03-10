#!/bin/bash

set -o xtrace
set -o errexit

export PYTHONPATH=${PROJECT_DIRECTORY}

${PYTHON_BINARY} setup.py clean

cd ..
git clone https://github.com/ajdavis/pymongo-mockup-tests.git
cd pymongo-mockup-tests
${PYTHON_BINARY} setup.py test

#!/bin/bash

set -o xtrace
set -o errexit

${PYTHON_BINARY} -m pip install tox
${PYTHON_BINARY} -m tox -m doc-test

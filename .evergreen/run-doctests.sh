#!/bin/bash

set -o xtrace
set -o errexit

${PYTHON_BINARY} setup.py clean
${PYTHON_BINARY} setup.py doc -t

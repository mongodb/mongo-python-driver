#!/bin/bash

. .evergreen/scripts/env.sh
set -o xtrace
PYTHON_BINARY=${PYTHON_BINARY} bash ${PROJECT_DIRECTORY}/.evergreen/hatch.sh doctest:test

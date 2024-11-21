#!/bin/bash

set -o xtrace
PYTHON_BINARY=${PYTHON_BINARY} bash "${PROJECT_DIRECTORY}"/.evergreen/hatch.sh doctest:test

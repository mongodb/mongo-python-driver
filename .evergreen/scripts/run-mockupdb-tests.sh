#!/bin/bash

set -o xtrace
export PYTHON_BINARY=${PYTHON_BINARY}
bash "${PROJECT_DIRECTORY}"/.evergreen/just.sh test-mockupdb

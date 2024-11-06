#!/bin/bash

. .evergreen/scripts/env.sh
set -o xtrace
PYTHON_BINARY=${PYTHON_BINARY}
MOD_WSGI_VERSION=${MOD_WSGI_VERSION}
MOD_WSGI_EMBEDDED=${MOD_WSGI_EMBEDDED}
PROJECT_DIRECTORY=${PROJECT_DIRECTORY}
bash "${PROJECT_DIRECTORY}"/.evergreen/run-mod-wsgi-tests.sh

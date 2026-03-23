#!/bin/bash
#
# Coverage combine merges (and removes) all the coverage files and
# generates a new .coverage file in the current directory.

set -eu

# Set up the virtual env.
. .evergreen/scripts/setup-dev-env.sh
uv sync --group coverage
source .venv/bin/activate

ls -la coverage/

coverage combine coverage/coverage.*
coverage html -d htmlcov

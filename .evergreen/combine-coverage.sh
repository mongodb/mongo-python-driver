#!/bin/bash
#
# Coverage combine merges (and removes) all the coverage files and
# generates a new .coverage file in the current directory.

set -eu

# Export before sourcing so that any uv sync inside setup-dev-env.sh uses them.
export UV_NO_LOCK=1
export UV_EXCLUDE_NEWER="${UV_EXCLUDE_NEWER:-$(python3 -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"))')}"

# Set up the virtual env.
. .evergreen/scripts/setup-dev-env.sh
uv sync --group coverage
source .venv/bin/activate

ls -la coverage/

coverage combine coverage/coverage.*
coverage html -d htmlcov

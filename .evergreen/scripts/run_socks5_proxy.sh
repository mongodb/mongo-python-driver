#!/bin/bash
set -eu

source "${DRIVERS_TOOLS}/.evergreen/find-python3.sh"
PYTHON_BINARY="$(ensure_python3 2>/dev/null)"
exec "$PYTHON_BINARY" "$(dirname "${BASH_SOURCE[0]}")/run_socks5_proxy.py" "$@"

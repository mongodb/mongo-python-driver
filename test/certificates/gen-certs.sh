#!/usr/bin/env bash
# Thin wrapper — delegates certificate generation to gen-certs.py.
# See gen-certs.py for full documentation on the cert design.
#
# Usage: bash gen-certs.sh  (run from test/certificates/)
# Requires: uv

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
uv run gen-certs.py

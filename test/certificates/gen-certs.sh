#!/usr/bin/env bash
# Thin wrapper — delegates certificate generation to gen-certs.py.
# Using Python's cryptography library gives precise extension control;
# in particular it lets us add AKI to leaf certs without adding SKI to
# the CA cert, which avoids the macOS SecTrust hard-fail OCSP check.
#
# Usage: bash gen-certs.sh  (run from test/certificates/)
# Requires: pip install cryptography

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
python3 gen-certs.py

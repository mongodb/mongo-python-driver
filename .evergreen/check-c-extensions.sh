#!/bin/bash
set -o errexit  # Exit the script with error if any of the commands fail

# Supported/used environment variables:
#  C_EXTENSIONS         Pass --no_ext to skip installing the C extensions.

PYTHON_IMPL=$(python -c "import platform; print(platform.python_implementation())")
if [ -z "$C_EXTENSIONS" ] && [ "$PYTHON_IMPL" = "CPython" ]; then
    PYMONGO_C_EXT_MUST_BUILD=1 python setup.py build_ext -i
    python tools/fail_if_no_c.py
fi

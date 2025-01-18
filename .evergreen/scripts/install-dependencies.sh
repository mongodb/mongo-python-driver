#!/bin/bash

set -eu
file="$PROJECT_DIRECTORY/.evergreen/install-dependencies.sh"
# Don't use ${file} syntax here because evergreen treats it as an empty expansion.
[ -f "$file" ] && bash "$file"

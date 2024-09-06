#!/bin/bash

. .evergreen/scripts/env.sh
set -o xtrace
file="$PROJECT_DIRECTORY/.evergreen/install-dependencies.sh"
# Don't use ${file} syntax here because evergreen treats it as an empty expansion.
[ -f "$file" ] && bash $file || echo "$file not available, skipping"

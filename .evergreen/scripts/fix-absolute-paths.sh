#!/bin/bash

set +x
. src/.evergreen/scripts/env.sh
# shellcheck disable=SC2044
for filename in $(find $DRIVERS_TOOLS -name \*.json); do
    perl -p -i -e "s|ABSOLUTE_PATH_REPLACEMENT_TOKEN|$DRIVERS_TOOLS|g" $filename
done

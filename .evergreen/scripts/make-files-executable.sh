#!/bin/bash

set +x
. src/.evergreen/scripts/env.sh
for i in $(find $DRIVERS_TOOLS/.evergreen $PROJECT_DIRECTORY/.evergreen -name \*.sh); do
    chmod +x $i
done

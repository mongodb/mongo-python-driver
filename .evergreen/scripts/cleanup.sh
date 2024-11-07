#!/bin/bash

. .evergreen/scripts/env.sh
if [ -f "$DRIVERS_TOOLS"/.evergreen/csfle/secrets-export.sh ]; then
  . .evergreen/hatch.sh encryption:teardown
fi
rm -rf "${DRIVERS_TOOLS}" || true
rm -f ./secrets-export.sh || true

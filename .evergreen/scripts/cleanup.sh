#!/bin/bash

if [ -f "$DRIVERS_TOOLS"/.evergreen/csfle/secrets-export.sh ]; then
  bash .evergreen/teardown-encryption.sh
fi
rm -rf "${DRIVERS_TOOLS}" || true
rm -f ./secrets-export.sh || true

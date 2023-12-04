#!/bin/bash

# Clean up CSFLE kmip servers
cd "$DRIVERS_TOOLS"/.evergreen/csfle || exit
./activate-kmstlsvenv.sh

if [ -f "kmip_pids.pid" ]; then
  < kmip_pids.pid xargs kill -9
fi

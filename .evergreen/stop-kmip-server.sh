#!/bin/bash

# Clean up CSFLE kmip servers
cd "$DRIVERS_TOOLS"/.evergreen/csfle || exit

if [ -f "kmip_pids.pid" ]; then
  < kmip_pids.pid xargs kill -9
  rm kmip_pids.pid
fi

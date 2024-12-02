#!/bin/bash

cd "${DRIVERS_TOOLS}"/.evergreen || exit
DRIVERS_TOOLS=${DRIVERS_TOOLS}
bash "${DRIVERS_TOOLS}"/.evergreen/run-load-balancer.sh stop

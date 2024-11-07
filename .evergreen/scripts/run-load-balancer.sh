#!/bin/bash

. src/.evergreen/scripts/env.sh
MONGODB_URI=${MONGODB_URI} bash "${DRIVERS_TOOLS}"/.evergreen/run-load-balancer.sh start

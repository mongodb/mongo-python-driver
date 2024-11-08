#!/bin/bash

MONGODB_URI=${MONGODB_URI} bash "${DRIVERS_TOOLS}"/.evergreen/run-load-balancer.sh start

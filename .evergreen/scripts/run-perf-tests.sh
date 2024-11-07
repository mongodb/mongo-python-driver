#!/bin/bash

. .evergreen/scripts/env.sh
PROJECT_DIRECTORY=${PROJECT_DIRECTORY}
bash "${PROJECT_DIRECTORY}"/.evergreen/run-perf-tests.sh

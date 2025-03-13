#!/bin/bash

PROJECT_DIRECTORY=${PROJECT_DIRECTORY}
SUB_TEST_NAME=${SUB_TEST_NAME} bash "${PROJECT_DIRECTORY}"/.evergreen/run-perf-tests.sh

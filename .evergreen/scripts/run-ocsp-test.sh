#!/bin/bash

. .evergreen/scripts/env.sh

TEST_OCSP=1 \
CA_FILE="${DRIVERS_TOOLS}/.evergreen/ocsp/${OCSP_ALGORITHM}/ca.pem" \
OCSP_TLS_SHOULD_SUCCEED="${OCSP_TLS_SHOULD_SUCCEED}" \
bash "${PROJECT_DIRECTORY}"/.evergreen/hatch.sh test:test-eg
bash "${DRIVERS_TOOLS}"/.evergreen/ocsp/teardown.sh

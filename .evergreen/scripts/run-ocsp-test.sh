#!/bin/bash
set -eu

pushd "${PROJECT_DIRECTORY}/.evergreen"
bash scripts/setup-dev-env.sh
CA_FILE="${DRIVERS_TOOLS}/.evergreen/ocsp/${OCSP_ALGORITHM}/ca.pem" \
  OCSP_TLS_SHOULD_SUCCEED="${OCSP_TLS_SHOULD_SUCCEED}" \
  bash scripts/setup-tests.sh ocsp
bash run-tests.sh
bash "${DRIVERS_TOOLS}"/.evergreen/ocsp/teardown.sh

popd

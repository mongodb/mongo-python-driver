#!/bin/bash
# Upload a coverate report to codecov.
# See coverage job in .github/workflows/test-python.yml for more information.
set -eu

curl -Os https://cli.codecov.io/latest/linux/codecov
curl -Os https://cli.codecov.io/latest/linux/codecov.SHA256SUM
shasum -a 256 -c codecov.SHA256SUM
sudo chmod +x codecov
./codecov upload-process \
  --report-type test_results \
  --disable-search \
  --fail-on-error \
  --token ${CODECOV_TOKEN} \
  --file .coverage

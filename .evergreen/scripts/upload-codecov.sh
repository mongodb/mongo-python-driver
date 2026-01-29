#!/bin/bash
# shellcheck disable=SC2154
# Upload a coverate report to codecov.
set -eu

FNAME=xunit-results/TEST-results.xml

if [ -z "${github_pr_number:-}" ]; then
  echo "This is not a PR, not running codecov"
  exit 0
fi

if [ ! -f "$FNAME" ]; then
  echo "There are no XML test results, not running codecov"
  exit 0
fi

# TODO: handle linux and windows.
echo "Installing codecov..."
curl -Os https://cli.codecov.io/latest/linux/codecov
curl -Os https://cli.codecov.io/latest/linux/codecov.SHA256SUM
shasum -a 256 -c codecov.SHA256SUM
sudo chmod +x codecov
echo "Installing codecov... done."
echo "Uploading $FNAME..."
./codecov upload-process \
  --report-type test_results \
  --disable-search \
  --fail-on-error \
  --token ${CODECOV_TOKEN} \
  --pr ${github_pr_number} \
  --sha ${github_commit} \
  --branch "${github_author}:${github_pr_head_branch}" \
  --flag "${build_variant}-${task_name}" \
  --file $FNAME
echo "Uploading $FNAME... done."

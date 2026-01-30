#!/bin/bash
# shellcheck disable=SC2154
# Upload a coverate report to codecov.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
ROOT=$(dirname "$(dirname $HERE)")

pushd $ROOT > /dev/null
export FNAME=coverage.xml

if [ -z "${github_pr_number:-}" ]; then
  echo "This is not a PR, not running codecov"
  exit 0
fi

if [ ! -f "$FNAME" ]; then
  echo "There are no XML test results, not running codecov"
  exit 0
fi

echo "Uploading..."
set -x
uv tool run --from codecov-cli codecovcli do-upload \
  --report-type test_results \
  --disable-search \
  --fail-on-error \
  --git-service github \
  --token ${CODECOV_TOKEN} \
  --pr ${github_pr_number} \
  --sha ${github_commit} \
  --branch "${github_author}:${github_pr_head_branch}" \
  --flag "${build_variant}-${task_name}" \
  --file $FNAME
set +x
echo "Uploading...done."

popd > /dev/null

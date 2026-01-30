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
printf 'pr: %s\n' "$github_pr_number"
printf 'sha: %s\n' "$github_commit"
printf 'branch: %s:%s\n' "$github_author" "$github_pr_head_branch"
printf 'flag: %s-%s\n' "$build_variant" "$task_name"
printf 'file: %s\n' "$FNAME"
uv tool run coverage xml
uv tool run --from codecov-cli codecovcli upload-process \
  --report-type coverage \
  --disable-search \
  --fail-on-error \
  --git-service github \
  --token ${CODECOV_TOKEN} \
  --pr ${github_pr_number} \
  --sha ${github_commit} \
  --branch "${github_author}:${github_pr_head_branch}" \
  --flag "${build_variant}-${task_name}" \
  --file $FNAME
echo "Uploading...done."

popd > /dev/null

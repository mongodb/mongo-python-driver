#!/bin/bash
# shellcheck disable=SC2154
# Upload a coverate report to codecov.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
ROOT=$(dirname "$(dirname $HERE)")

pushd $ROOT > /dev/null
export FNAME=coverage.xml
REQUESTER=${requester:-}

if [ ! -f ".coverage" ]; then
  echo "There are no coverage results, not running codecov"
  exit 0
fi

if [[ "${REQUESTER}" == "github_pr" || "${REQUESTER}" == "commit" ]]; then
    echo "Uploading codecov for $REQUESTER..."
else
    echo "Error: requester must be 'github_pr' or 'commit', got '${REQUESTER}'" >&2
    exit 1
fi

printf 'sha: %s\n' "$github_commit"
printf 'flag: %s-%s\n' "$build_variant" "$task_name"
printf 'file: %s\n' "$FNAME"
uv tool run --with "coverage[toml]" coverage xml

codecov_args=(
  upload-process
  --report-type coverage
  --disable-search
  --fail-on-error
  --git-service github
  --token "${CODECOV_TOKEN}"
  --sha "${github_commit}"
  --flag "${build_variant}-${task_name}"
  --file "${FNAME}"
)

if [ -n "${github_pr_number:-}" ]; then
  printf 'branch: %s:%s\n' "$github_author" "$github_pr_head_branch"
  printf 'pr: %s\n' "$github_pr_number"
  uv tool run --from codecov-cli codecovcli \
    "${codecov_args[@]}" \
    --pr "${github_pr_number}" \
    --branch "${github_author}:${github_pr_head_branch}"
else
  printf 'branch: %s\n' "$branch_name"
  uv tool run --from codecov-cli codecovcli \
    "${codecov_args[@]}" \
    --branch "${branch_name}"
fi
echo "Uploading codecov for $REQUESTER... done."

popd > /dev/null

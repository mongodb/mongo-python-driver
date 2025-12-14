#!/bin/bash
# Generate a PR comment with the perf report

set -eu

VERSION_ID=${version_id}
BASE_SHA=${revision}
HEAD_SHA=${github_commit}
PROJECT=${project}

if [ "${PROJECT}" != "mongo-python-driver" ]; then
  echo "Skipping PR Perf comment"
  exit 0
fi


pushd $DRIVERS_TOOLS/.evergreen >/dev/null
CONTEXT="PyMongo triage context" TASK="${TASK_NAME}" VARIANT="performance-benchmarks" sh run-perf-comp.sh

if [[ -n "${BASE_SHA+set}" && -n "${HEAD_SHA+set}" && "$BASE_SHA" != "$HEAD_SHA" ]]; then
    # Make the PR comment.
    target=github_app/create_or_modify_comment.sh
    bash $target -m "## 🧪 Performance Results" -c "$(pwd)/perfcomp/perf-report.md" -h $HEAD_SHA -o "mongodb" -n $PROJECT
    rm ./perfcomp/perf-report.md
else
    # Skip comment if it isn't a PR run.
    echo "Skipping Perf PR comment"
fi

popd >/dev/null

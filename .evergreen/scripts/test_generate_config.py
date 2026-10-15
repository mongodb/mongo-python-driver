"""Tests for the Evergreen PR task rules (PYTHON-6112).

Run: pytest .evergreen/scripts/test_generate_config.py -m ""
(-m "" overrides the repo-wide default marker filter.)
Requires the same dependencies as the generate-config hook: shrub.py, pyyaml.
"""

from __future__ import annotations

import sys
from inspect import getmembers, isfunction
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import generate_config


def all_tasks():
    tasks = []
    for name, func in getmembers(generate_config, isfunction):
        if name.startswith("create_") and name.endswith("_tasks"):
            tasks.extend(func())
    return tasks


def task_vars(task):
    vars = {}
    for command in task.commands or []:
        vars.update(getattr(command, "vars", None) or {})
    return vars


def pr_tasks():
    return [task for task in all_tasks() if "pr" in (task.tags or [])]


def test_pr_tasks_run_on_latest_stable():
    """PR tasks must pin to latest-stable: the "latest" nightly is unstable."""
    offenders = [task.name for task in pr_tasks() if task_vars(task).get("VERSION") == "latest"]
    assert not offenders, f"PR tasks pinned to 'latest': {offenders}"


def test_pr_min_deps_tasks_also_run_coverage():
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("TEST_MIN_DEPS") == "1" and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"min-deps PR tasks without COVERAGE: {offenders}"


def test_standard_family_pr_budget():
    """The No C Ext and Compression variants select .test-standard, so their
    patch builds run one PR task per pr-tagged test-standard task. Keep the
    count at or below the pre-pin baseline (5), and at or below 3 for the
    ubuntu22 compression variant, which filters to Python 3.14 and newer."""
    tasks = [task for task in pr_tasks() if "test-standard" in (task.tags or [])]
    assert len(tasks) <= 5, f"too many test-standard PR tasks: {[t.name for t in tasks]}"
    newer = [
        task
        for task in tasks
        if any(
            tag in (task.tags or [])
            for tag in ("python-3.14", "python-3.14t", "python-3.15", "python-3.15t")
        )
    ]
    assert len(newer) <= 3, f"too many 3.14+ test-standard PR tasks: {[t.name for t in newer]}"

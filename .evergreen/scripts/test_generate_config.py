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
    """PR tasks must not download the "latest" nightly: it needs AWS credentials."""
    offenders = [task.name for task in pr_tasks() if task_vars(task).get("VERSION") == "latest"]
    assert not offenders, f"PR tasks pinned to 'latest': {offenders}"


def test_pr_min_deps_tasks_also_run_coverage():
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("TEST_MIN_DEPS") == "1" and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"min-deps PR tasks without COVERAGE: {offenders}"

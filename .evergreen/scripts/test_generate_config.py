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
import generate_config_utils


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


def test_pr_tasks_pin_max_version():
    """PR tasks must pin to MAX_VERSION: the "latest" nightly is unstable.
    The min-deps extreme is the exception: it pins to 4.4 to match its
    oldest dependencies."""
    offenders = [
        (task.name, task_vars(task).get("VERSION"))
        for task in pr_tasks()
        if task_vars(task).get("VERSION") not in (None, generate_config_utils.MAX_VERSION)
        and not (
            task_vars(task).get("VERSION") == "4.4" and task_vars(task).get("TEST_MIN_DEPS") == "1"
        )
    ]
    assert not offenders, f"PR tasks not pinned to MAX_VERSION: {offenders}"


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


def test_pr_tasks_run_coverage():
    """Every PR task that pins a server version runs with COVERAGE=1. The
    no-cov companions are helpers for the macOS/Win64 encryption variants,
    not PR tasks, and the non-test infrastructure tasks (coverage-report,
    check-import-time, kms-fail) do not pin a server version."""
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("VERSION") is not None
        and "test-non-standard-no-cov" not in (task.tags or [])
        and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"PR tasks without COVERAGE: {offenders}"


def test_rotated_tasks_keep_nightly_coverage():
    """Mainline batch coverage rides the rotation, independently of the
    pinned PR tasks: the rotated "latest" nightly tasks keep COVERAGE=1
    (min-deps excepted), as do the rotated max-Python auth-aws tasks."""
    offenders = []
    for task in all_tasks():
        tags = task.tags or []
        vars = task_vars(task)
        if vars.get("TEST_MIN_DEPS") == "1" or "pr" in tags:
            continue
        rotated_latest = "server-latest" in tags or ("ocsp-ecdsa" in tags and "latest" in tags)
        is_tested = ("test-standard" in tags and "server-latest" in tags) or (
            "test-non-standard" in tags
            and "server-latest" in tags
            and "sharded_cluster-auth-ssl" in tags
        )
        is_aws_max_python = (
            "auth-aws" in tags
            and "session-name" not in task.name
            and vars.get("UV_PYTHON") == generate_config_utils.MAX_PYTHON
        )
        if rotated_latest and is_tested and vars.get("COVERAGE") != "1":
            offenders.append(task.name)
        if is_aws_max_python and vars.get("COVERAGE") != "1":
            offenders.append(task.name)
    assert not offenders, f"rotated tasks without nightly COVERAGE: {offenders}"
    # The rotated "latest" ECDSA staples task keeps its nightly coverage;
    # the pinned PR task is emitted separately.
    latest_staples = [
        task.name
        for task in all_tasks()
        if "ocsp-ecdsa" in (task.tags or [])
        and "ocsp-staple" in (task.tags or [])
        and "latest" in (task.tags or [])
        and task_vars(task).get("COVERAGE") == "1"
    ]
    assert latest_staples, "no ECDSA staples task on the latest nightly with COVERAGE"
    # The 9.0 nightly OCSP tasks stay on the min-deps Python; only the
    # pinned PR task pins to MAX_VERSION.
    offenders = [
        task.name
        for task in all_tasks()
        if "ocsp" in (task.tags or [])
        and "9.0" in (task.tags or [])
        and "pr" not in (task.tags or [])
        and task_vars(task).get("TEST_MIN_DEPS") != "1"
    ]
    assert not offenders, f"nightly 9.0 OCSP tasks not on min-deps Python: {offenders}"


def test_non_standard_cov_tasks_have_no_cov_companions():
    """Each coverage task in the non-standard family also emits a
    non-coverage companion so the macOS/Win64 encryption variants (which
    filter out .cov due to timeout constraints) still have a task to
    activate in patch builds."""
    no_cov = {task.name for task in all_tasks() if "test-non-standard-no-cov" in (task.tags or [])}
    for task in all_tasks():
        if "test-non-standard-no-cov" in (task.tags or []):
            assert "pr" not in (task.tags or []), f"no-cov companion tagged pr: {task.name}"
        elif "cov" in (task.tags or []) and "test-non-standard" in (task.tags or []):
            companion = task.name.removesuffix("-cov")
            assert companion in no_cov, f"missing no-cov companion for {task.name}"

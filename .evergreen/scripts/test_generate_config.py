"""Tests for the Evergreen PR task rules (PYTHON-6112).

Run: pytest .evergreen/scripts/test_generate_config.py -m ""
(-m "" clears the repo-wide marker filter.)
Requires the generate-config hook's dependencies: pytest, pytest-asyncio, shrub.py, pyyaml.
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


def all_variants():
    variants = []
    for name, func in getmembers(generate_config, isfunction):
        if name.startswith("create_") and name.endswith("_variants"):
            variants.extend(func())
    return variants


def variant_selects(variant, task):
    """Whether the variant's task selectors match the task: "." selects a
    tag, "!" negates the token that follows, a bare token names a task."""
    tags = task.tags or []
    for ref in variant.tasks:
        includes, excludes, names = [], [], []
        for token in ref.name.split():
            if token.startswith("!."):
                excludes.append(token[2:])
            elif token.startswith("!"):
                excludes.append(token[1:])
            elif token.startswith("."):
                includes.append(token[1:])
            else:
                names.append(token)
        if any(tag in tags for tag in excludes):
            continue
        if not includes and not names:
            # A negation-only selector matches every remaining task.
            return True
        if names and task.name not in names:
            continue
        if includes and not all(tag in tags for tag in includes):
            continue
        return True
    return False


def test_pr_tasks_pin_max_version():
    """Task-level pins: pr tasks pin MAX_VERSION (the "latest" nightly is
    unstable); the explicit 4.4 min-deps pin is the exception. Variant-merged
    pins are checked by test_pr_tasks_effectively_pin_max_version."""
    offenders = [
        (task.name, task_vars(task).get("VERSION"))
        for task in pr_tasks()
        if task_vars(task).get("VERSION") not in (None, generate_config_utils.MAX_VERSION)
        and not (
            task_vars(task).get("VERSION") == "4.4" and task_vars(task).get("TEST_MIN_DEPS") == "1"
        )
    ]
    assert not offenders, f"PR tasks not pinned to MAX_VERSION: {offenders}"


def test_pr_tasks_effectively_pin_max_version():
    """Effective pins: a pr task without its own VERSION var inherits the
    selecting variant's version, so every pr task that runs a server must
    resolve to MAX_VERSION once variant expansions merge in. The explicit
    4.4 min-deps pin is the exception."""
    tasks = [task for task in all_tasks() if "pr" in (task.tags or [])]
    offenders = []
    for variant in all_variants():
        variant_version = (variant.expansions or {}).get("VERSION")
        for task in tasks:
            if not variant_selects(variant, task):
                continue
            task_vars_ = task_vars(task)
            if task_vars_.get("VERSION") == "4.4" and task_vars_.get("TEST_MIN_DEPS") == "1":
                continue
            effective = task_vars_.get("VERSION") or variant_version
            if effective is None:
                # Helpers that run the stable default server, like kms-fail.
                continue
            if effective != generate_config_utils.MAX_VERSION:
                offenders.append((variant.name, task.name, effective))
    assert not offenders, f"pr tasks not effectively pinned to MAX_VERSION: {offenders}"


def test_server_version_pr_tasks_selected_once():
    """The mongodb-* variants differ only in server version, so a pr task
    must be selected only by the variant running the server version it pins."""
    offenders = {}
    for task in pr_tasks():
        if "server-version" not in (task.tags or []):
            continue
        own = task_vars(task).get("VERSION")
        selecting = [variant.name for variant in all_variants() if variant_selects(variant, task)]
        matching = [
            variant.name
            for variant in all_variants()
            if (variant.expansions or {}).get("VERSION") == own
        ]
        if selecting != matching:
            offenders[task.name] = {"selected by": selecting, "pins": own}
    assert not offenders, (
        f"server-version pr tasks not selected by the variant matching their pin: {offenders}"
    )


def test_pr_tasks_cover_version_boundaries():
    """PRs test the server version boundaries: the newest numbered server
    with every Python boundary, the min server with its min-Python floor."""
    for version, pythons in (
        (
            generate_config_utils.MAX_VERSION,
            (generate_config_utils.MIN_PYTHON, generate_config_utils.MAX_PYTHON),
        ),
        (generate_config_utils.MIN_VERSION, (generate_config_utils.MIN_PYTHON,)),
    ):
        tasks = [
            task
            for task in pr_tasks()
            if "server-version" in (task.tags or []) and task_vars(task).get("VERSION") == version
        ]
        seen = {task_vars(task).get("UV_PYTHON") for task in tasks}
        assert seen == set(pythons), (
            f"{version} pr tasks pin {sorted(seen)}, want {sorted(pythons)}"
        )
        assert len(tasks) >= 2, f"{version} has too few server-version pr tasks"


def test_server_version_rotated_tasks_keep_coverage():
    """Per-commit coverage for this family rides the rotated boundary-Python
    tasks, not the pr tasks (which run only on MAX_VERSION)."""
    family = [task for task in all_tasks() if "server-version" in (task.tags or [])]
    cov = [task for task in family if task_vars(task).get("COVERAGE") == "1"]
    assert any("pr" not in (task.tags or []) for task in cov), (
        "the server-version family's only COVERAGE tasks are pr tasks"
    )
    pr_pythons = {
        combo: python
        for combos in generate_config.SERVER_VERSION_PR_PYTHONS.values()
        for combo, python in combos.items()
    }
    offenders = []
    for task in family:
        if "pr" in (task.tags or []):
            continue
        vars = task_vars(task)
        boundary = pr_pythons.get((vars["TOPOLOGY"], vars["AUTH"], vars["SSL"]))
        if vars.get("UV_PYTHON") == boundary and vars.get("COVERAGE") != "1":
            offenders.append(task.name)
    assert not offenders, f"rotated server-version boundary tasks without COVERAGE: {offenders}"


def test_pr_min_deps_tasks_also_run_coverage():
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("TEST_MIN_DEPS") == "1" and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"min-deps PR tasks without COVERAGE: {offenders}"


def test_standard_family_pr_budget():
    """Patch-build PR budget: at most 5 pr-tagged test-standard tasks (the
    pre-pin baseline), at most 3 on Python 3.14+ (the ubuntu22 compression
    filter)."""
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
    """Pr tasks that pin a server version run with COVERAGE=1; no-cov
    companions and infrastructure tasks (no VERSION) are exempt."""
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("VERSION") is not None
        and "test-non-standard-no-cov" not in (task.tags or [])
        and task_vars(task).get("COVERAGE") != "1"
    ]
    assert not offenders, f"PR tasks without COVERAGE: {offenders}"


def test_rotated_tasks_keep_nightly_coverage():
    """Batch coverage rides the rotation independently of the pr tasks:
    rotated "latest" tasks and max-Python auth-aws tasks keep COVERAGE=1
    (min-deps excepted)."""
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
    # The "latest" ECDSA staples task keeps its nightly coverage.
    latest_staples = [
        task.name
        for task in all_tasks()
        if "ocsp-ecdsa" in (task.tags or [])
        and "ocsp-staple" in (task.tags or [])
        and "latest" in (task.tags or [])
        and task_vars(task).get("COVERAGE") == "1"
    ]
    assert latest_staples, "no ECDSA staples task on the latest nightly with COVERAGE"
    # The 9.0 nightly OCSP tasks stay on the min-deps Python.
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
    """Each non-standard coverage task also emits a no-cov companion, so the
    macOS/Win64 encryption variants (which filter .cov) have a task to
    activate in patch builds."""
    no_cov = {task.name for task in all_tasks() if "test-non-standard-no-cov" in (task.tags or [])}
    for task in all_tasks():
        if "test-non-standard-no-cov" in (task.tags or []):
            assert "pr" not in (task.tags or []), f"no-cov companion tagged pr: {task.name}"
        elif "cov" in (task.tags or []) and "test-non-standard" in (task.tags or []):
            companion = task.name.removesuffix("-cov")
            assert companion in no_cov, f"missing no-cov companion for {task.name}"

"""Tests for the Evergreen config generation rules (PYTHON-6112).

Run: pytest .evergreen/scripts/test_generate_config.py -m ""
(-m "" overrides the repo-wide default marker filter.)
Requires the same dependencies as the generate-config hook: shrub.py, pyyaml.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest
from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import FunctionCall
from shrub.v3.evg_task import EvgTask, EvgTaskRef

sys.path.insert(0, str(Path(__file__).parent))

import generate_config
import generate_config_utils


def test_boundaries_derive_from_cpythons():
    cpy = generate_config_utils.CPYTHONS
    assert cpy[0] == generate_config_utils.MIN_PYTHON
    assert cpy[-1] == generate_config_utils.MAX_PYTHON
    assert [p for p in cpy if "t" in p][-1] == generate_config_utils.FREE_THREADED_PYTHON
    assert (
        cpy[0],
        cpy[-1],
        [p for p in cpy if "t" in p][-1],
    ) == generate_config_utils.PR_PYTHONS


def test_is_free_threaded():
    assert generate_config_utils.is_free_threaded("3.15t")
    assert not generate_config_utils.is_free_threaded("3.15")


def python_of(task):
    return next(tag[len("python-") :] for tag in task.tags if tag.startswith("python-"))


def pr_tasks_of(family):
    return [
        task
        for task in generate_config.task_list()
        if task.name.startswith(family) and "pr" in (task.tags or [])
    ]


def test_rotation_families_pin_boundary_pythons():
    for family in generate_config.PR_FAMILIES:
        pythons = sorted(python_of(task) for task in pr_tasks_of(family))
        assert pythons, f"no PR tasks in {family}"
        assert all(python in generate_config_utils.PR_PYTHONS for python in pythons), (
            f"{family}: {pythons} not pinned to {generate_config_utils.PR_PYTHONS}"
        )


def test_pr_pins_are_stable_under_cpython_change():
    """Adding a CPython must not reshuffle the rotation families' PR tasks."""

    def pr_names():
        return sorted(
            task.name for family in generate_config.PR_FAMILIES for task in pr_tasks_of(family)
        )

    before = pr_names()
    with (
        mock.patch.object(generate_config, "CPYTHONS", [*generate_config_utils.CPYTHONS, "3.16"]),
        mock.patch.object(
            generate_config, "ALL_PYTHONS", [*generate_config_utils.ALL_PYTHONS, "3.16"]
        ),
    ):
        after = pr_names()
    assert before == after


def test_full_config_passes_validation():
    generate_config.validate_pr_and_coverage(
        generate_config.task_list(), generate_config.variant_list()
    )


def test_rejects_rotated_pr_python():
    task = EvgTask(
        name="test-standard-latest-python3.12-sync-noauth-nossl-standalone",
        tags=["test-standard", "python-3.12", "pr"],
    )
    with pytest.raises(AssertionError, match=r"3\.12"):
        generate_config.validate_pr_and_coverage([task], [])


def test_rejects_coverage_without_cov_tag():
    task = EvgTask(
        name="test-standard-latest-python3.15-sync-noauth-nossl-standalone",
        tags=["test-standard", "python-3.15", "pr"],
        commands=[FunctionCall(func="run tests", vars={"COVERAGE": "1"})],
    )
    with pytest.raises(AssertionError, match="cov"):
        generate_config.validate_pr_and_coverage([task], [])


def test_rejects_distro_coverage_excluded_by_selector():
    task = EvgTask(
        name="test-standard-latest-python3.15-sync-noauth-nossl-standalone",
        tags=["test-standard", "python-3.15", "pr", "cov"],
        commands=[FunctionCall(func="run tests", vars={"COVERAGE": "1"})],
    )
    variant = BuildVariant(
        name="v", tasks=[EvgTaskRef(name=".test-standard !.cov")], run_on=["rhel8.10-small"]
    )
    with pytest.raises(AssertionError, match=r"rhel8\.10-small"):
        generate_config.validate_pr_and_coverage([task], [variant])


def test_standard_auth_coverage_does_not_satisfy_default_suite():
    task = EvgTask(
        name="test-standard-auth-latest-python3.15-auth-ssl-sharded-cluster",
        tags=["test-standard-auth", "python-3.15", "pr", "cov"],
        commands=[FunctionCall(func="run tests", vars={"COVERAGE": "1"})],
    )
    with pytest.raises(AssertionError, match="default suite"):
        generate_config.validate_pr_and_coverage([task], [])

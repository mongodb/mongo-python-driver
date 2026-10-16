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
    """Pr tasks that pin a server version run with COVERAGE=1."""
    offenders = [
        task.name
        for task in pr_tasks()
        if task_vars(task).get("VERSION") is not None and task_vars(task).get("COVERAGE") != "1"
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
            any(
                t in tags
                for t in ("test-encryption", "test-encryption-pyopenssl", "test-load-balancer")
            )
            and "server-latest" in tags
            and "sharded_cluster-auth-ssl" in tags
            and "macos" not in tags
            and "win64" not in tags
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


def test_sync_option_env_override(monkeypatch):
    """The SYNC env var selects the --sync option (PYTHON-6167)."""
    import utils

    monkeypatch.setattr(sys, "argv", ["setup_tests.py", "encryption"])
    monkeypatch.delenv("SYNC", raising=False)
    opts, _ = utils.get_test_options("Set up the test environment.")
    assert opts.sync is None
    monkeypatch.setenv("SYNC", "async")
    opts, _ = utils.get_test_options("Set up the test environment.")
    assert opts.sync == "async"


SUITE_FAMILIES = {
    "test-encryption": "encryption",
    "test-encryption-pyopenssl": "encryption",
    "test-load-balancer": "load_balancer",
    "test-doctest": "doctest",
}


def test_no_test_non_standard_tasks_remain():
    offenders = [
        task.name
        for task in all_tasks()
        if any(tag.startswith("test-non-standard") for tag in (task.tags or []))
    ]
    assert not offenders, f"test-non-standard tasks remain: {offenders}"


def test_suite_tasks_identify_their_suite():
    """Every per-suite task's name and tags identify the suite it runs."""
    for task in all_tasks():
        tags = task.tags or []
        for family, suite in SUITE_FAMILIES.items():
            if family not in tags:
                continue
            assert task.name.startswith(family), f"{task.name} does not start with {family}"
            vars = task_vars(task)
            assert vars.get("TEST_NAME") == suite, f"{task.name} does not set TEST_NAME={suite}"
            if family == "test-encryption-pyopenssl":
                assert vars.get("SUB_TEST_NAME") == "pyopenssl"


def test_encryption_and_load_balancer_split_sync_async():
    """Each matrix cell of the split suites runs as one sync and one async task."""
    for family in ("test-encryption", "test-encryption-pyopenssl", "test-load-balancer"):
        modes = {}
        for task in all_tasks():
            tags = task.tags or []
            if family not in tags:
                continue
            mode_tags = {"sync", "async"} & set(tags)
            assert len(mode_tags) == 1, f"{task.name} carries tags {sorted(mode_tags)}"
            mode = mode_tags.pop()
            assert task_vars(task).get("SYNC") == mode, f"{task.name} does not set SYNC={mode}"
            key = frozenset(tag for tag in tags if tag not in ("sync", "async"))
            modes.setdefault(key, set()).add(mode)
        assert modes, f"{family} has no tasks"
        incomplete = [sorted(k) for k, v in modes.items() if v != {"sync", "async"}]
        assert not incomplete, f"{family} cells without both sync modes: {incomplete}"


def test_suite_family_sizes():
    """The deliberate matrix sizes of the per-suite families (PYTHON-6167)."""
    sizes = {
        # 22 non-3.15t cells x 2 sync modes, doubled for the crypt_shared twins.
        "test-encryption": {"rhel8": 88, "macos": 88, "win64": 88, "ubuntu22": 4},
        # RHEL 8 runs every cell including 3.15t (setup_tests.py builds OpenSSL 3).
        "test-encryption-pyopenssl": {"rhel8": 50},
        "test-load-balancer": 14,
        "test-doctest": 8,
    }
    distros = ("rhel8", "macos", "win64", "ubuntu22")
    counts = {}
    for task in all_tasks():
        for family in sizes:
            if family in (task.tags or []):
                counts.setdefault(family, []).append(task)
    for family, expected in sizes.items():
        tasks = counts.get(family, [])
        if isinstance(expected, int):
            assert len(tasks) == expected, f"{family} has {len(tasks)} tasks, want {expected}"
        else:
            per_distro = dict.fromkeys(distros, 0)
            for task in tasks:
                distro = [d for d in distros if d in (task.tags or [])]
                assert len(distro) == 1, f"{task.name} does not carry exactly one distro tag"
                per_distro[distro[0]] += 1
            assert {d: n for d, n in per_distro.items() if n} == expected, (
                f"{family} sizes {per_distro}, want {expected}"
            )


def test_encryption_ubuntu22_groups_are_3_15t_only():
    """The ubuntu22 group runs only the 3.15t cells on 6.0+, keeping the
    pr-tagged variant's patch-build budget (PYTHON-6167)."""
    for task in all_tasks():
        tags = task.tags or []
        if "test-encryption" not in tags or "ubuntu22" not in tags:
            continue
        assert "python-3.15t" in tags, f"ubuntu22 task not on 3.15t: {task.name}"
        assert {"server-4.4", "server-5.0"} & set(tags) == set(), (
            f"ubuntu22 task on a server < 6.0: {task.name}"
        )


def test_crypt_shared_tasks_mirror_the_encryption_cells():
    """The crypt_shared tasks twin the non-ubuntu22 encryption cells and set
    TEST_CRYPT_SHARED on the run server call (PYTHON-6167)."""
    regular = [
        task
        for task in all_tasks()
        if "test-encryption" in (task.tags or [])
        and "crypt-shared" not in (task.tags or [])
        and "ubuntu22" not in (task.tags or [])
    ]
    twins = [task for task in all_tasks() if "crypt-shared" in (task.tags or [])]
    assert twins, "no crypt_shared tasks"
    assert {task.name.replace("-crypt-shared", "") for task in twins} == {
        task.name for task in regular
    }, "crypt_shared tasks do not mirror the regular cells"
    for task in twins:
        assert "ubuntu22" not in (task.tags or []), f"crypt_shared task on ubuntu22: {task.name}"
        servers = [c for c in task.commands or [] if getattr(c, "func", None) == "run server"]
        assert len(servers) == 1, f"{task.name} has no single run server call"
        assert servers[0].vars.get("TEST_CRYPT_SHARED") == "true", (
            f"{task.name} does not set TEST_CRYPT_SHARED"
        )


def test_pyopenssl_free_threaded_runs_on_rhel8():
    """The free-threaded PyOpenSSL cells run on RHEL 8, which setup_tests.py
    equips with OpenSSL 3 when the system library is older (PYTHON-6167)."""
    tasks = [
        task
        for task in all_tasks()
        if "test-encryption-pyopenssl" in (task.tags or []) and "free-threaded" in (task.tags or [])
    ]
    assert tasks, "no free-threaded pyopenssl tasks"
    assert all("rhel8" in (task.tags or []) for task in tasks), (
        f"free-threaded pyopenssl tasks outside rhel8: {[t.name for t in tasks]}"
    )


def test_suite_tasks_never_mix_min_deps_and_coverage():
    """A per-suite task tests the minimum dependencies or collects coverage, never both."""
    offenders = [
        task.name
        for task in all_tasks()
        if SUITE_FAMILIES.keys() & set(task.tags or [])
        and task_vars(task).get("TEST_MIN_DEPS") == "1"
        and task_vars(task).get("COVERAGE") == "1"
    ]
    assert not offenders, f"tasks with both TEST_MIN_DEPS and COVERAGE: {offenders}"


def test_suite_pr_pins():
    """Each split suite keeps its pinned PR task with coverage, in both sync modes."""
    for family in ("test-encryption", "test-encryption-pyopenssl", "test-load-balancer"):
        pins = [task for task in pr_tasks() if family in (task.tags or [])]
        modes = {"sync" if "sync" in (task.tags or []) else "async" for task in pins}
        assert modes == {"sync", "async"}, f"{family} pr pins: {[t.name for t in pins]}"
        for task in pins:
            vars = task_vars(task)
            assert vars.get("VERSION") == generate_config_utils.MAX_VERSION
            assert vars.get("UV_PYTHON") == generate_config_utils.MAX_PYTHON
            assert vars.get("COVERAGE") == "1"


def test_encryption_cov_and_pr_tasks_run_on_rhel8_only():
    """Coverage and the PR pin ride the rhel8 groups only (the suites exceed
    the 60-min timeout with coverage overhead on macOS/Win64)."""
    for task in all_tasks():
        tags = task.tags or []
        if "test-encryption" in tags or "test-encryption-pyopenssl" in tags:
            if "cov" in tags or "pr" in tags:
                assert "rhel8" in tags, f"cov/pr task outside rhel8: {task.name}"


def test_variants_select_only_their_suite():
    """No variant runs another suite's tasks (PYTHON-6167)."""
    for variant in all_variants():
        selected = [task for task in all_tasks() if variant_selects(variant, task)]
        families = {
            family for task in selected for family in SUITE_FAMILIES if family in (task.tags or [])
        }
        if variant.name == "encryption-rhel8":
            expected = {"test-encryption", "test-encryption-pyopenssl"}
        elif variant.name.startswith("encryption"):
            expected = {"test-encryption"}
        elif variant.name == "load-balancer":
            expected = {"test-load-balancer"}
        elif variant.name.startswith("doctests"):
            expected = {"test-doctest"}
        else:
            expected = set()
        assert families == expected, (
            f"{variant.name} selects {sorted(families)}, want {sorted(expected)}"
        )

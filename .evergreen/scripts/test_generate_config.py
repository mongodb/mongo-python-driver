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


def family_tasks(family, crypt_shared=None):
    """The tasks of one per-suite family, optionally filtered by the
    crypt-shared tag."""
    return [
        task
        for task in all_tasks()
        if family in (task.tags or [])
        and (crypt_shared is None or ("crypt-shared" in (task.tags or [])) == crypt_shared)
    ]


def group_by_version(tasks):
    """Group the tasks by their VERSION var, preserving order."""
    by_version = {}
    for task in tasks:
        by_version.setdefault(task_vars(task).get("VERSION"), []).append(task)
    return by_version


def assert_covers_versions(label, tasks, expected):
    """The tasks cover exactly the expected set of server versions."""
    versions = {task_vars(task).get("VERSION") for task in tasks}
    assert versions == set(expected), f"{label} versions: {sorted(versions)}"


def assert_modes(label, tasks, expected):
    """Each task carries the sync or async tag matching its SYNC var, and the
    tasks cover the expected set of modes."""
    modes = set()
    for task in tasks:
        tags = task.tags or []
        mode = "sync" if "sync" in tags else "async"
        modes.add(mode)
        assert task_vars(task).get("SYNC") == mode, f"{label} {task.name}"
    assert modes == set(expected), f"{label} modes: {sorted(modes)}, want {sorted(expected)}"


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
    pre-pin baseline), at most 3 on Python 3.14+."""
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


def test_ensure_openssl3_passes_path_without_interpolation(monkeypatch, tmp_path):
    """ensure_openssl3() hands the install-openssl3.sh path to bash as a
    positional argument, never interpolated into the bash -c script, so shell
    metacharacters in DRIVERS_TOOLS cannot execute (PYTHON-6167)."""
    import setup_tests

    monkeypatch.setattr(setup_tests, "PLATFORM", "linux")
    monkeypatch.setattr(setup_tests, "write_env", lambda *args, **kwargs: None)

    # A system openssl older than 3.0 forces the source path.
    fake_openssl = tmp_path / "openssl"
    fake_openssl.write_text('#!/bin/sh\necho "OpenSSL 1.1.1w  11 Sep 2023"\n')
    fake_openssl.chmod(0o755)
    monkeypatch.setattr(setup_tests.shutil, "which", lambda _name: str(fake_openssl))

    # The drivers-evergreen-tools path carries a command substitution payload.
    payload = tmp_path / "dr$(touch pwned.txt)vers"
    script_dir = payload / ".evergreen"
    script_dir.mkdir(parents=True)
    script = script_dir / "install-openssl3.sh"
    script.write_text('export OPENSSL_DIR="/openssl3"\n')
    monkeypatch.setenv("DRIVERS_TOOLS", str(payload))
    monkeypatch.setattr(setup_tests, "DRIVERS_TOOLS", str(payload))

    setup_tests.ensure_openssl3()

    assert not (tmp_path / "pwned.txt").exists(), "the DRIVERS_TOOLS payload executed"
    import os

    try:
        assert os.environ.get("OPENSSL_DIR") == "/openssl3", (
            "the sourced script did not export OPENSSL_DIR"
        )
    finally:
        os.environ.pop("OPENSSL_DIR", None)
        os.environ.pop("LD_LIBRARY_PATH", None)


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
    "test-compression": "default",
    "test-pyopenssl": "default",
    "test-gevent": "default",
    "test-disable-test-commands": "default",
    "test-no-c-ext": "default",
    "test-stable-api": "default",
    "test-standard": "default",
    "test-storage-inmemory": "default",
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


def test_suite_family_sizes():
    """The deliberate matrix sizes of the per-suite families (PYTHON-6167)."""
    sizes = {
        # The three standard corners plus the max-CPython sharded task across
        # the remaining MongoDB versions (8 per group), with rhel8-only
        # coverage twins of the max-stable and latest cells (10 per group),
        # doubled for the crypt_shared twins.
        "test-encryption": 20,
        # PyOpenSSL encryption runs on RHEL 8 only: one 8-task group.
        "test-encryption-pyopenssl": 8,
        "test-load-balancer": 12,
        "test-doctest": 8,
        # One task per compressor and version (every compressor on every
        # server version): sync on the latest replica set, async on the
        # sharded-cluster rotation.
        "test-compression": 24,
        # One task per server version per distro: sync on the latest replica
        # set, async on the sharded-cluster rotation.
        "test-pyopenssl": 8,
        "test-gevent": 8,
        # One task per server version: sync on the standalone and replica-set
        # cells, async on the sharded-cluster cells.
        "test-disable-test-commands": 8,
        "test-no-c-ext": 8,
        # One accept-v2 standalone task and one require-v1 sharded-cluster
        # task per server version 5.0+.
        "test-stable-api": 14,
        # One task per server version: the encryption corners spread across
        # the four non-linux platforms (arm64 6.0+, no free-threaded win32).
        "test-standard": 8,
        # One sync standalone task per server version on the InMemory engine.
        "test-storage-inmemory": 8,
    }
    counts = {}
    for task in all_tasks():
        for family in sizes:
            if family in (task.tags or []):
                counts.setdefault(family, []).append(task)
    for family, expected in sizes.items():
        tasks = counts.get(family, [])
        assert len(tasks) == expected, f"{family} has {len(tasks)} tasks, want {expected}"


def corner_rows(split_coverage):
    """The encryption-corner cells shared by the encryption and standard
    families: version -> (python, topology tag, sync mode, min deps, task
    count). The three-distro encryption family splits the max-stable and
    latest cells into a plain task plus a rhel8-only coverage twin; a family
    without the split folds coverage into the single task."""
    corners = {
        # Sync, min Python, min deps, min MongoDB, standalone.
        generate_config_utils.MIN_VERSION: (
            generate_config_utils.MIN_PYTHON,
            "standalone-noauth-nossl",
            "sync",
            True,
            1,
        ),
        # Async, max CPython, max stable MongoDB, sharded cluster.
        generate_config.MAX_VERSION: (
            generate_config_utils.MAX_PYTHON,
            "sharded_cluster-auth-ssl",
            "async",
            False,
            2 if split_coverage else 1,
        ),
        # Sync, max free-threaded CPython, middle MongoDB, replica set.
        generate_config.MIDDLE_VERSION: (
            generate_config_utils.FREE_THREADED_PYTHON,
            "replica_set-noauth-ssl",
            "sync",
            False,
            1,
        ),
    }
    for version in set(generate_config_utils.ALL_VERSIONS) - set(corners):
        corners[version] = (
            generate_config_utils.MAX_PYTHON,
            "sharded_cluster-auth-ssl",
            "async",
            False,
            2 if version == "latest" and split_coverage else 1,
        )
    return corners


def assert_corner_cells(label, cells, row):
    """The corner cell(s) of one version run the corner's Python, topology,
    sync mode, and min-deps."""
    python, topology, mode, min_deps, count = row
    assert len(cells) == count, f"{label}: {len(cells)} tasks, want {count}"
    for task in cells:
        where = f"{label} {task.name}"
        assert f"python-{python}" in (task.tags or []), where
        assert topology in (task.tags or []), where
        assert mode in (task.tags or []), where
        assert task_vars(task).get("UV_PYTHON") == python, where
        assert (task_vars(task).get("TEST_MIN_DEPS") == "1") == min_deps, where


def test_encryption_shape():
    """Per test type: the three standard corners — sync min-Python min-deps on
    the min MongoDB standalone, async max-CPython on the max stable MongoDB
    sharded cluster, and sync max free-threaded CPython on the middle MongoDB
    replica set — plus the max-CPython sharded task across the remaining
    MongoDB versions, including latest and rapid. The names carry no distro;
    every encryption variant runs the shared tasks, with the max-stable and
    latest coverage twins riding rhel8 only (PYTHON-6167)."""
    for family, crypt_shared in (
        ("test-encryption", False),
        ("test-encryption", True),
        ("test-encryption-pyopenssl", False),
    ):
        if crypt_shared and family != "test-encryption":
            continue
        group = family_tasks(family, crypt_shared=crypt_shared)
        label = f"{family}/crypt_shared={crypt_shared}"
        assert group, f"{label} has no tasks"
        by_version = group_by_version(group)
        assert_covers_versions(label, group, generate_config_utils.ALL_VERSIONS)

        # The three-distro family splits the max-stable and latest cells into
        # a plain task plus a rhel8-only coverage twin; the rhel8-only
        # pyopenssl family folds coverage into the single task.
        corners = corner_rows(family == "test-encryption")
        for version, row in corners.items():
            assert_corner_cells(f"{label} {version}", by_version[version], row)
        # The shared tasks carry every distro tag; the twins only rhel8.
        for task in group:
            tags = task.tags or []
            assert "ubuntu22" not in tags, task.name
            assert {"rhel8", "macos", "win64"} & set(tags), f"{task.name} carries no distro tag"
            if "cov" in tags or "pr" in tags:
                assert "rhel8" in tags, task.name
                assert not {"macos", "win64"} & set(tags), task.name
        # Every encryption variant lists the same coverage of all versions.
        for variant in all_variants():
            if not variant.name.startswith("encryption") or not variant.name.endswith(
                ("rhel8", "macos", "win64")
            ):
                continue
            if family == "test-encryption-pyopenssl" and variant.name != "encryption-rhel8":
                # The pyopenssl encryption tasks ride the RHEL 8 variant only.
                assert not [t for t in group if variant_selects(variant, t)], variant.name
                continue
            selected = [task for task in group if variant_selects(variant, task)]
            selected_versions = {task_vars(task).get("VERSION") for task in selected}
            assert selected_versions == set(generate_config_utils.ALL_VERSIONS), (
                f"{variant.name} versions: {sorted(selected_versions)}"
            )


def test_compression_shape():
    """Compression: one sync task per compressor on the latest replica set plus
    one async sharded-cluster task per compressor and MongoDB version for every
    other version, so each compressor is tested against every MongoDB version
    without duplicating cells; every task enables its compressor
    (PYTHON-6167)."""
    tasks = family_tasks("test-compression")
    assert tasks, "test-compression has no tasks"
    for compressor in ("snappy", "zlib", "zstd"):
        group = [t for t in tasks if f"test-compression-{compressor}" in t.name]
        assert_covers_versions(compressor, group, generate_config_utils.ALL_VERSIONS)
        by_version = group_by_version(group)
        primary = by_version["latest"][0]
        assert "replica_set-noauth-ssl" in (primary.tags or []), primary.name
        assert_modes(f"{compressor} latest", [primary], {"sync"})
        for version, cell_tasks in by_version.items():
            if version == "latest":
                continue
            assert len(cell_tasks) == 1, (
                f"{compressor} {version} cells: {[t.name for t in cell_tasks]}"
            )
            cell = cell_tasks[0]
            assert "sharded_cluster-auth-ssl" in (cell.tags or []), cell.name
            assert_modes(f"{compressor} {version}", [cell], {"async"})
    for task in tasks:
        assert task_vars(task).get("COMPRESSORS"), f"{task.name} does not set COMPRESSORS"


def test_stable_api_shape():
    """Stable API: one consolidated family — a sync accept-v2 standalone task
    and an async require-v1 sharded-cluster task per MongoDB version 5.0+,
    with the CPython rotated across the versions. The require-v1 tasks start
    the server with requireApiVersion=1 and pin apiVersion=1 on the suite; the
    accept-v2 tasks use the versioned-api orchestration without pinning an
    apiVersion; every stable-api variant lists all versions (PYTHON-6167)."""
    tasks = family_tasks("test-stable-api")
    assert tasks, "test-stable-api has no tasks"
    by_version = group_by_version(tasks)
    assert_covers_versions("stable api", tasks, generate_config_utils.get_versions_from("5.0"))
    for version, cells in by_version.items():
        assert len(cells) == 2, f"{version} cells: {[t.name for t in cells]}"
        require_v1 = [t for t in cells if "-require-v1-" in t.name]
        accept_v2 = [t for t in cells if "-accept-v2-" in t.name]
        assert len(require_v1) == 1, f"{version} require-v1 cells: {[t.name for t in cells]}"
        assert len(accept_v2) == 1, f"{version} accept-v2 cells: {[t.name for t in cells]}"
        # Async, auth, SSL, sharded cluster with requireApiVersion=1.
        task = require_v1[0]
        where = f"stable api {task.name}"
        tags = task.tags or []
        assert "sharded_cluster-auth-ssl" in tags, where
        assert "async" in tags, where
        vars = task_vars(task)
        assert vars.get("REQUIRE_API_VERSION") == "1", where
        assert vars.get("MONGODB_API_VERSION") == "1", where
        servers = [c for c in task.commands or [] if getattr(c, "func", None) == "run server"]
        assert servers[0].vars.get("REQUIRE_API_VERSION") == "1", where
        # Sync, noauth, no SSL, standalone from the versioned-api orchestration.
        task = accept_v2[0]
        where = f"stable api {task.name}"
        tags = task.tags or []
        assert "standalone-noauth-nossl" in tags, where
        assert "sync" in tags, where
        vars = task_vars(task)
        assert vars.get("ORCHESTRATION_FILE") == "versioned-api-testing.json", where
        assert not vars.get("MONGODB_API_VERSION"), where
        servers = [c for c in task.commands or [] if getattr(c, "func", None) == "run server"]
        assert servers[0].vars.get("ORCHESTRATION_FILE") == "versioned-api-testing.json", where


def test_standard_shape():
    """The standard family: one task per MongoDB version following the
    encryption corners — sync min-Python min-deps on the min MongoDB
    standalone, sync max free-threaded CPython on the middle MongoDB replica
    set, async max-CPython on the max stable MongoDB sharded cluster and the
    remaining versions, with coverage on latest. The names carry no distro;
    the platform tags restrict the cells: macos-arm64 starts at 6.0 and win32
    gets no free-threaded CPython; every platform variant lists its full
    version coverage (PYTHON-6167)."""
    tasks = family_tasks("test-standard")
    assert tasks, "test-standard has no tasks"
    by_version = group_by_version(tasks)
    assert_covers_versions("standard", tasks, generate_config_utils.ALL_VERSIONS)
    corners = corner_rows(split_coverage=False)
    # The platform tags restrict the cells: macOS arm64 only works on server
    # versions 6.0+, and win32 gets no free-threaded CPython.
    platforms = {
        generate_config_utils.MIN_VERSION: {"macos", "win32", "win64"},
        generate_config.MIDDLE_VERSION: {"macos", "macos-arm64", "win64"},
        "5.0": {"macos", "win32", "win64"},
    }
    for version, (python, topology, mode, min_deps, count) in corners.items():
        cells = by_version[version]
        assert len(cells) == count, f"{version} cells: {[t.name for t in cells]}"
        cell_platforms = platforms.get(version) or {
            "macos",
            "macos-arm64",
            "win32",
            "win64",
        }
        task = cells[0]
        where = f"standard {task.name}"
        assert_corner_cells(f"standard {version}", cells, (python, topology, mode, min_deps, 1))
        assert set(cell_platforms) <= set(task.tags or []), where
        assert not ("free-threaded" in (task.tags or []) and "win32" in cell_platforms), where
    latest = by_version["latest"][0]
    assert "cov" in (latest.tags or []), latest.name
    assert task_vars(latest).get("COVERAGE") == "1", latest.name
    # Every platform variant lists its full version coverage: macOS arm64
    # starts at 6.0, and win32 skips the free-threaded middle corner.
    for variant in all_variants():
        if not variant.name.startswith("test-macos") and not variant.name.startswith("test-win"):
            continue
        selected = [task for task in tasks if variant_selects(variant, task)]
        selected_versions = {task_vars(task).get("VERSION") for task in selected}
        if variant.name == "test-macos-arm64":
            expected = set(generate_config_utils.get_versions_from("6.0"))
        elif variant.name == "test-win32":
            expected = set(generate_config_utils.ALL_VERSIONS) - {generate_config.MIDDLE_VERSION}
        else:
            expected = set(generate_config_utils.ALL_VERSIONS)
        assert selected_versions == expected, (
            f"{variant.name} versions: {sorted(selected_versions)}"
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


def test_per_version_families_cover_every_server_version():
    """The per-version families cover every server version without duplicating
    cells: PyOpenSSL (SUB_TEST_NAME=pyopenssl), disable-test-commands
    (DISABLE_TEST_COMMANDS=1), no-c-ext (NO_EXT=1), and gevent
    (GREEN_FRAMEWORK=gevent) assign the sync mode like the encryption corners —
    sync on the standalone and replica-set cells, async on the sharded-cluster
    cells, except gevent which only runs the sync half — load balancer runs a
    sync and an async all-sharded task per version 6.0+ covering the full
    suite, and storage-inmemory runs one sync standalone task per version
    (PYTHON-6167)."""
    for family, distros, extra_var, split_modes, per_version in (
        ("test-pyopenssl", ("rhel8", "macos", "win64"), "SUB_TEST_NAME", True, 1),
        ("test-gevent", ("",), "GREEN_FRAMEWORK", False, 1),
        ("test-disable-test-commands", ("",), "DISABLE_TEST_COMMANDS", True, 1),
        ("test-no-c-ext", ("",), "NO_EXT", True, 1),
        ("test-load-balancer", ("",), None, True, 2),
        ("test-storage-inmemory", ("",), "STORAGE_ENGINE", True, 1),
    ):
        for distro in distros:
            group = [
                task for task in family_tasks(family) if not distro or distro in (task.tags or [])
            ]
            label = f"{family}/{distro or 'any'}"
            assert group, f"{label} has no tasks"
            by_version = group_by_version(group)
            # Load balancer support starts with MongoDB 6.0.
            expected_versions = set(
                generate_config_utils.get_versions_from("6.0")
                if family == "test-load-balancer"
                else generate_config_utils.ALL_VERSIONS
            )
            assert_covers_versions(label, group, expected_versions)
            for version, cell_tasks in by_version.items():
                assert len(cell_tasks) == per_version, (
                    f"{label} {version} cells: {[t.name for t in cell_tasks]}"
                )
                if per_version == 2:
                    assert_modes(label, cell_tasks, {"sync", "async"})
            for task in group:
                tags = task.tags or []
                where = f"{label} {task.name}"
                if extra_var:
                    assert task_vars(task).get(extra_var), f"{where} does not set {extra_var}"
                topology = next(
                    tag
                    for tag in tags
                    if tag.startswith(("standalone", "replica_set", "sharded_cluster"))
                )
                if split_modes and per_version == 2:
                    # One sync and one async all-sharded task per version.
                    assert task_vars(task).get("SYNC") == ("sync" if "sync" in tags else "async"), (
                        where
                    )
                elif split_modes:
                    # Sync on the standalone and replica-set cells, async on
                    # the sharded clusters, like the encryption corners.
                    expected_mode = "async" if topology.startswith("sharded") else "sync"
                    assert expected_mode in tags, where
                    assert task_vars(task).get("SYNC") == expected_mode, where
                elif family == "test-gevent":
                    # Gevent only runs the sync half; the async tests are
                    # excluded under the framework.
                    assert "sync" in tags, where
                    assert "async" not in tags, where
                    assert task_vars(task).get("SYNC") == "sync", where
                else:
                    # Load balancer runs both halves in its single task.
                    assert not {"sync", "async"} & set(tags), where
            if family in ("test-gevent", "test-disable-test-commands", "test-no-c-ext"):
                # One topology per version, cycling through all three.
                topologies = {
                    next(
                        tag
                        for tag in (task.tags or [])
                        if tag.startswith(("standalone", "replica_set", "sharded_cluster"))
                    )
                    for task in group
                }
                assert topologies == {
                    "standalone-noauth-nossl",
                    "replica_set-noauth-ssl",
                    "sharded_cluster-auth-ssl",
                }, f"{label} topologies: {sorted(topologies)}"
            if family == "test-load-balancer":
                # Load balancer support requires an authenticated SSL sharded cluster.
                assert all(
                    "sharded_cluster-auth-ssl" in (task.tags or []) and "auth" in (task.tags or [])
                    for task in group
                ), label
            if family == "test-storage-inmemory":
                # The InMemory engine runs on standalone servers.
                assert all("standalone-noauth-nossl" in (task.tags or []) for task in group), label
            # Free-threaded CPython only rotates on RHEL 8, the distro
            # setup_tests.py equips with OpenSSL 3; gevent does not support it.
            if family in ("test-gevent", "test-pyopenssl"):
                assert not [t for t in group if "free-threaded" in (t.tags or [])], label


def test_pyopenssl_free_threaded_runs_on_rhel8():
    """The free-threaded PyOpenSSL tasks ride the RHEL 8 encryption variant
    only: setup_tests.py equips RHEL 8 with OpenSSL 3 when the system library
    is older, and the pyopenssl-* variants select no free-threaded cells
    (PYTHON-6167)."""
    assert not [v for v in all_variants() if v.name == "pyopenssl-ubuntu-22"]
    tasks = [
        task
        for task in all_tasks()
        if "test-pyopenssl" in (task.tags or []) and "free-threaded" in (task.tags or [])
    ]
    assert not tasks, f"free-threaded pyopenssl tasks: {[t.name for t in tasks]}"
    encryption_tasks = [
        task
        for task in all_tasks()
        if "test-encryption-pyopenssl" in (task.tags or []) and "free-threaded" in (task.tags or [])
    ]
    assert encryption_tasks, "no free-threaded encryption-pyopenssl tasks"
    assert all("rhel8" in (task.tags or []) for task in encryption_tasks), (
        f"free-threaded encryption-pyopenssl tasks outside rhel8: "
        f"{[t.name for t in encryption_tasks]}"
    )
    for variant in all_variants():
        if not variant.name.startswith("pyopenssl"):
            continue
        selected = [task for task in encryption_tasks if variant_selects(variant, task)]
        assert not selected, (
            f"{variant.name} runs free-threaded pyopenssl cells: {[t.name for t in selected]}"
        )


def test_numpy_win32_tasks_run_on_windows_only():
    """The win32 numpy tasks use the 32-bit toolchain (IS_WIN32) and are
    selected only by the windows variant (PYTHON-6167)."""
    win32_tasks = [task for task in all_tasks() if "test-numpy-win32" in (task.tags or [])]
    assert win32_tasks, "no win32 numpy tasks"
    for task in win32_tasks:
        assert "test-numpy" not in (task.tags or []), task.name
        assert task_vars(task).get("IS_WIN32") == "1", task.name
    for variant in all_variants():
        if not variant.name.startswith("test-numpy"):
            continue
        selected = [task for task in win32_tasks if variant_selects(variant, task)]
        if variant.name == "test-numpy-windows":
            assert selected, "the windows numpy variant does not run the win32 tasks"
        else:
            assert not selected, (
                f"{variant.name} runs the win32 tasks: {[t.name for t in selected]}"
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
    """The per-suite pr pins: the encryption suites pin the RHEL 8
    sharded-cluster rotation task on MAX_VERSION with coverage; the
    compression, load balancer, pyopenssl, gevent, disable-test-commands, and
    no-c-ext families have no pr pin."""
    for family, expected_pins in (
        ("test-encryption", 2),  # The regular task and its crypt_shared twin.
        ("test-encryption-pyopenssl", 1),
    ):
        pins = [task for task in pr_tasks() if family in (task.tags or [])]
        assert len(pins) == expected_pins, f"{family} pr pins: {[t.name for t in pins]}"
        for pin in pins:
            tags = pin.tags or []
            assert "rhel8" in tags, pin.name
            assert "sharded_cluster-auth-ssl" in tags, pin.name
            vars = task_vars(pin)
            assert vars.get("VERSION") == generate_config_utils.MAX_VERSION
            assert vars.get("UV_PYTHON") == generate_config_utils.MAX_PYTHON
            assert vars.get("COVERAGE") == "1"
    for family in (
        "test-compression",
        "test-pyopenssl",
        "test-gevent",
        "test-disable-test-commands",
        "test-no-c-ext",
        "test-load-balancer",
    ):
        pins = [task for task in pr_tasks() if family in (task.tags or [])]
        assert not pins, f"{family} pr pins: {[t.name for t in pins]}"


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
        elif variant.name.startswith("compression"):
            expected = {"test-compression"}
        elif variant.name.startswith("pyopenssl"):
            expected = {"test-pyopenssl"}
        elif variant.name.startswith("green-gevent"):
            expected = {"test-gevent"}
        elif variant.name.startswith("disable-test-commands"):
            expected = {"test-disable-test-commands"}
        elif variant.name.startswith("no-c-ext"):
            expected = {"test-no-c-ext"}
        elif variant.name.startswith("stable-api"):
            expected = {"test-stable-api"}
        elif variant.name.startswith("test-macos") or variant.name.startswith("test-win"):
            expected = {"test-standard"}
        elif variant.name.startswith("storage-inmemory"):
            expected = {"test-storage-inmemory"}
        else:
            expected = set()
        assert families == expected, (
            f"{variant.name} selects {sorted(families)}, want {sorted(expected)}"
        )

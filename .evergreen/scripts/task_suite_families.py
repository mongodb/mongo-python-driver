"""The per-suite task families of the Evergreen config (PYTHON-6167).

Each creator builds the tasks of one suite family. generate_config.py
re-exports them so the config writer and the tests keep discovering them
alongside the other task creators.
"""

from __future__ import annotations

from itertools import product

from generate_config_utils import (
    ALL_PYTHONS,
    ALL_VERSIONS,
    CPYTHONS,
    CPYTHONS_NO_FREE_THREADED,
    FREE_THREADED_PYTHON,
    MAX_PYTHON,
    MAX_VERSION,
    MIN_PYTHON,
    MIN_VERSION,
    SYNCS,
    TOPOLOGIES,
    get_standard_auth_ssl,
    get_suite_mode,
    get_task_name,
    get_versions_from,
    zip_cycle,
)
from shrub.v3.evg_command import FunctionCall
from shrub.v3.evg_task import EvgTask

# The wire-protocol compressors exercised by the compression tasks.
COMPRESSORS = ["snappy", "zlib", "zstd"]

# The distros sharing the per-suite task definitions.
ALL_DISTROS = ["rhel8", "macos", "win64"]

# The middle stable MongoDB version, anchoring the free-threaded cells.
STABLE_VERSIONS = [v for v in ALL_VERSIONS if v not in ("rapid", "latest")]
MIDDLE_VERSION = STABLE_VERSIONS[len(STABLE_VERSIONS) // 2]


def _create_suite_task(
    family,
    version,
    topology,
    python,
    sync,
    suite,
    sub_test=None,
    crypt_shared=False,
    suffix=None,
    extra_test_vars=None,
    extra_expansions=None,
    distros=None,
    pr=False,
    cov=None,
):
    """One task of a per-suite family: the name carries no distro, so every
    distro variant lists the same definition, tagged with its distros
    (PYTHON-6167)."""
    auth, ssl = get_standard_auth_ssl(topology)
    tags = [
        family,
        f"server-{version}",
        f"python-{python}",
        f"{topology}-{auth}-{ssl}",
        auth,
    ]
    tags.extend(distros or [])
    if sync:
        tags.append(sync)
    if "t" in python:
        tags.append("free-threaded")
    if pr:
        tags.append("pr")
    expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology, VERSION=version)
    if extra_expansions:
        # Shared by the run server and run tests calls, for vars the server
        # start needs (REQUIRE_API_VERSION, ORCHESTRATION_FILE, ...).
        expansions.update(extra_expansions)
    if crypt_shared:
        tags.append("crypt-shared")
        expansions["TEST_CRYPT_SHARED"] = "true"
    collects_cov = (
        cov if cov is not None else (version == "latest" and topology == "sharded_cluster")
    )
    if python == ALL_PYTHONS[0]:
        expansions["TEST_MIN_DEPS"] = "1"
    elif collects_cov:
        tags.append("cov")
        expansions["COVERAGE"] = "1"
    base = family
    if suffix:
        base += f"-{suffix}"
    if crypt_shared:
        base += "-crypt-shared"
    name = get_task_name(base, python=python, sync=sync, **expansions)
    server_vars = dict(TEST_NAME=suite, **expansions)
    test_vars = dict(TEST_NAME=suite, UV_PYTHON=python, **expansions)
    if sub_test:
        test_vars["SUB_TEST_NAME"] = sub_test
    if sync:
        test_vars["SYNC"] = sync
    if extra_test_vars:
        test_vars.update(extra_test_vars)
    server_func = FunctionCall(func="run server", vars=server_vars)
    test_func = FunctionCall(func="run tests", vars=test_vars)
    return EvgTask(name=name, tags=tags, commands=[server_func, test_func])


def _create_encryption_tasks(family, sub_test=None, crypt_shared=False, distros=None):
    """Encryption tasks for one test type (PYTHON-6167).

    Coverage and the pr pin ride rhel8-only twins: the max-stable and
    latest cells exceed the 60-minute timeout with coverage on
    macOS/Win64."""
    all_distros = ALL_DISTROS
    distros = distros or all_distros
    split_coverage = distros == all_distros
    tasks = []

    def add(version, topology, python, sync, **kwargs):
        tasks.append(
            _create_suite_task(
                family,
                version,
                topology,
                python,
                sync,
                "encryption",
                sub_test=sub_test,
                crypt_shared=crypt_shared,
                **kwargs,
            )
        )

    # Sync, min Python, min deps, min MongoDB, no auth, no SSL, standalone.
    add(MIN_VERSION, "standalone", MIN_PYTHON, "sync", distros=distros)
    # Sync, max free-threaded CPython, middle MongoDB, noauth, SSL, replica set.
    add(MIDDLE_VERSION, "replica_set", FREE_THREADED_PYTHON, "sync", distros=distros)
    # Async, max CPython, max stable MongoDB, auth, SSL, sharded cluster.
    if split_coverage:
        # The macos/win64 cell carries the pr pin without coverage: the suites
        # exceed the 60-minute timeout with coverage on macOS/Win64.
        add(
            MAX_VERSION,
            "sharded_cluster",
            MAX_PYTHON,
            "async",
            distros=["macos", "win64"],
            pr=True,
        )
        add(
            MAX_VERSION,
            "sharded_cluster",
            MAX_PYTHON,
            "async",
            distros=["rhel8"],
            pr=True,
            cov=True,
        )
    else:
        add(MAX_VERSION, "sharded_cluster", MAX_PYTHON, "async", distros=distros, pr=True, cov=True)
    # The max-CPython sharded task across the remaining MongoDB versions; the
    # latest cell keeps a rhel8-only coverage twin.
    for version in ALL_VERSIONS:
        if version in (MIN_VERSION, MIDDLE_VERSION, MAX_VERSION):
            continue
        if version == "latest" and split_coverage:
            add(
                version,
                "sharded_cluster",
                MAX_PYTHON,
                "async",
                distros=["macos", "win64"],
                cov=False,
            )
            add(version, "sharded_cluster", MAX_PYTHON, "async", distros=["rhel8"], cov=True)
        else:
            add(version, "sharded_cluster", MAX_PYTHON, "async", distros=distros)
    return tasks


def _create_cov_pr_twin(family, sync, sub_test=None, extra_test_vars=None):
    """Rhel8-only max-stable coverage twin carrying the pr pin: the rotated
    max-stable cell can land on the min-deps Python, and coverage stays off
    macOS/Win64 (PYTHON-6167)."""
    return _create_suite_task(
        family,
        MAX_VERSION,
        "sharded_cluster",
        MAX_PYTHON,
        sync,
        "default",
        sub_test=sub_test,
        distros=["rhel8"],
        pr=True,
        cov=True,
        extra_test_vars=extra_test_vars,
    )


def create_test_encryption_tasks():
    """Encryption tasks, plus crypt_shared variants of every task."""
    return _create_encryption_tasks("test-encryption") + _create_encryption_tasks(
        "test-encryption", crypt_shared=True
    )


def create_test_encryption_pyopenssl_tasks():
    """PyOpenSSL tasks on RHEL 8 only (setup_tests.py builds OpenSSL 3 there)."""
    return _create_encryption_tasks(
        "test-encryption-pyopenssl", sub_test="pyopenssl", distros=["rhel8"]
    )


def create_test_load_balancer_tasks():
    """Load balancer tasks: one sync and one async task per MongoDB version
    6.0+ on a load-balanced auth SSL sharded cluster, running the full
    default suite (PYTHON-6167). The v9.0 pair carries the pr pin and
    coverage."""
    tasks = []
    for sync in SYNCS:
        # One CPython per version (6.0→3.11 … latest→3.15t); the trailing 3.15 is unused.
        for version, python in zip(get_versions_from("6.0"), CPYTHONS, strict=False):
            is_pin = version == MAX_VERSION
            tasks.append(
                _create_suite_task(
                    "test-load-balancer",
                    version,
                    "sharded_cluster",
                    python,
                    sync,
                    "load_balancer",
                    pr=is_pin,
                    cov=True if is_pin else None,
                )
            )
    return tasks


def create_test_doctest_tasks():
    """Doctest tasks: one task per MongoDB version on a standalone server."""
    tasks = []
    for version, python in zip_cycle(ALL_VERSIONS, CPYTHONS):
        is_pin = version == MAX_VERSION
        tasks.append(
            _create_suite_task(
                "test-doctest",
                version,
                "standalone",
                python,
                "",
                "doctest",
                pr=is_pin,
                cov=True if is_pin else None,
            )
        )
    return tasks


def create_test_compression_tasks():
    """Compression tasks: one sync task per compressor on the latest replica
    set plus async sharded-cluster cells on every other MongoDB version, so
    each compressor meets every server version (PYTHON-6167)."""
    tasks = []
    for compressor in COMPRESSORS:
        # Sync, max CPython, noauth, SSL, replica set.
        tasks.append(
            _create_suite_task(
                "test-compression",
                "latest",
                "replica_set",
                MAX_PYTHON,
                "sync",
                "default",
                suffix=compressor,
                extra_test_vars=dict(COMPRESSORS=compressor),
            )
        )
    cells = list(product(COMPRESSORS, [v for v in ALL_VERSIONS if v != "latest"]))
    for (compressor, version), python in zip_cycle(cells, CPYTHONS):
        # Async, auth, SSL, sharded cluster. The first compressor's max-stable
        # cell carries the pr pin and coverage; the primaries ride "latest",
        # which PRs skip.
        is_pin = compressor == COMPRESSORS[0] and version == MAX_VERSION
        tasks.append(
            _create_suite_task(
                "test-compression",
                version,
                "sharded_cluster",
                python,
                "async",
                "default",
                suffix=compressor,
                pr=is_pin,
                cov=True if is_pin else None,
                extra_test_vars=dict(COMPRESSORS=compressor),
            )
        )
    return tasks


def _per_version_cells():
    """One cell per MongoDB version: a sharded cluster for every stable and
    rapid version, a replica set for latest, all with SSL."""
    return [(version, "sharded_cluster") for version in ALL_VERSIONS if version != "latest"] + [
        ("latest", "replica_set")
    ]


def create_test_pyopenssl_tasks():
    """PyOpenSSL tasks: one task per MongoDB version, running the default
    suite with the PyOpenSSL extra. Free-threaded CPython is covered by the
    encryption-pyopenssl family, and the rhel8 max-stable twin carries the
    pr pin and coverage (PYTHON-6167)."""
    return [
        _create_suite_task(
            "test-pyopenssl",
            version,
            topology,
            python,
            get_suite_mode(topology),
            "default",
            sub_test="pyopenssl",
            distros=ALL_DISTROS,
        )
        for (version, topology), python in zip_cycle(
            _per_version_cells(), CPYTHONS_NO_FREE_THREADED
        )
    ] + [
        _create_cov_pr_twin(
            "test-pyopenssl", get_suite_mode("sharded_cluster"), sub_test="pyopenssl"
        )
    ]


def _create_rotation_tasks(
    family, extra_test_vars=None, extra_expansions=None, pythons=CPYTHONS, sync=None, pin=None
):
    """One task per MongoDB version, the topology cycling through standalone,
    replica set, and sharded cluster (PYTHON-6167). The max-stable cell
    carries the pr pin when pin is set, with coverage for "pr+cov"."""
    tasks = []
    for version, topology, python in zip_cycle(ALL_VERSIONS, TOPOLOGIES, pythons):
        is_pin = bool(pin) and version == MAX_VERSION
        kwargs = dict(pr=True) if is_pin else {}
        if is_pin and pin == "pr+cov":
            kwargs["cov"] = True
        tasks.append(
            _create_suite_task(
                family,
                version,
                topology,
                python,
                sync if sync else get_suite_mode(topology),
                "default",
                extra_test_vars=extra_test_vars,
                extra_expansions=extra_expansions,
                **kwargs,
            )
        )
    return tasks


def create_test_gevent_tasks():
    """Gevent tasks: one task per MongoDB version running the sync default
    suite (async tests are excluded under the framework; free-threaded
    CPython is unsupported), plus the rhel8 max-stable pr twin with coverage
    (PYTHON-6167)."""
    return [
        *_create_rotation_tasks(
            "test-gevent",
            extra_test_vars=dict(GREEN_FRAMEWORK="gevent"),
            pythons=CPYTHONS_NO_FREE_THREADED,
            sync="sync",
        ),
        _create_cov_pr_twin("test-gevent", "sync", extra_test_vars=dict(GREEN_FRAMEWORK="gevent")),
    ]


def create_test_disable_test_commands_tasks():
    """Disable-test-commands tasks: one task per MongoDB version, the
    topology cycling through all three (PYTHON-6167)."""
    return _create_rotation_tasks(
        "test-disable-test-commands",
        # Shared with the run server call, whose orchestration picks the
        # disableTestCommands config from it.
        extra_expansions=dict(DISABLE_TEST_COMMANDS="1"),
        pin="pr+cov",
    )


def create_test_no_c_ext_tasks():
    """No-c-ext tasks: one task per MongoDB version, the topology cycling
    through all three, without the C extensions (NO_EXT feeds run_tests.py)
    (PYTHON-6167)."""
    return _create_rotation_tasks("test-no-c-ext", extra_test_vars=dict(NO_EXT="1"), pin="pr+cov")


def create_test_stable_api_tasks():
    """Stable API tasks: one require-v1 task per MongoDB version 5.0+ on a
    sharded cluster, and one accept-v2 standalone task per version
    (PYTHON-6167)."""
    cells = [
        # Async, auth, SSL, sharded cluster started with requireApiVersion=1.
        (
            "require-v1",
            "sharded_cluster",
            "async",
            dict(REQUIRE_API_VERSION="1", MONGODB_API_VERSION="1"),
        ),
        # Sync, noauth, no SSL, standalone from the versioned-api orchestration.
        ("accept-v2", "standalone", "sync", dict(ORCHESTRATION_FILE="versioned-api-testing.json")),
    ]
    tasks = []
    for version, python in zip_cycle(get_versions_from("5.0"), CPYTHONS):
        is_pin = version == MAX_VERSION
        for suffix, topology, sync, extra_expansions in cells:
            tasks.append(
                _create_suite_task(
                    "test-stable-api",
                    version,
                    topology,
                    python,
                    sync,
                    "default",
                    suffix=suffix,
                    extra_expansions=extra_expansions,
                    pr=is_pin,
                    cov=True if is_pin else None,
                )
            )
    return tasks


def create_test_storage_inmemory_tasks():
    """InMemory storage engine tasks: one sync standalone task per MongoDB
    version (PYTHON-6167)."""
    tasks = []
    for version, python in zip_cycle(ALL_VERSIONS, CPYTHONS):
        is_pin = version == MAX_VERSION
        tasks.append(
            _create_suite_task(
                "test-storage-inmemory",
                version,
                "standalone",
                python,
                "sync",
                "default",
                extra_expansions=dict(STORAGE_ENGINE="inmemory"),
                pr=is_pin,
                cov=True if is_pin else None,
            )
        )
    return tasks


def create_standard_tasks():
    """The standard suite on the non-linux platforms, one task per MongoDB
    version following the encryption corners; the platform tags restrict the
    cells (macOS arm64 starts at 6.0, win32 has no free-threaded CPython)
    (PYTHON-6167)."""
    all_platforms = ["macos", "macos-arm64", "win32", "win64"]
    corners = [
        # Sync, min Python, min deps, min MongoDB, no auth, no SSL, standalone.
        (
            MIN_VERSION,
            "standalone",
            MIN_PYTHON,
            "sync",
            ["macos", "win32", "win64"],
            {},
        ),
        # Sync, max free-threaded CPython, middle MongoDB, noauth, SSL, replica set.
        (
            MIDDLE_VERSION,
            "replica_set",
            FREE_THREADED_PYTHON,
            "sync",
            ["macos", "macos-arm64", "win64"],
            {},
        ),
        # Async, max CPython, max stable MongoDB, auth, SSL, sharded cluster;
        # carries the pr pin and coverage.
        (
            MAX_VERSION,
            "sharded_cluster",
            MAX_PYTHON,
            "async",
            all_platforms,
            {"pr": True, "cov": True},
        ),
        # The max-CPython sharded task across the remaining MongoDB versions;
        # the latest cell collects coverage.
        *[
            (
                version,
                "sharded_cluster",
                MAX_PYTHON,
                "async",
                # macOS arm64 only works on server versions 6.0+.
                ["macos", "win32", "win64"] if version == "5.0" else all_platforms,
                {},
            )
            for version in ALL_VERSIONS
            if version not in (MIN_VERSION, MIDDLE_VERSION, MAX_VERSION)
        ],
    ]
    return [
        _create_suite_task(
            "test-standard",
            version,
            topology,
            python,
            sync,
            "default",
            distros=platforms,
            **kwargs,
        )
        for version, topology, python, sync, platforms, kwargs in corners
    ]

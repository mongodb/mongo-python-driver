# Note: See CONTRIBUTING.md for how to update/run this file.
from __future__ import annotations

import sys
from itertools import product

from generate_config_utils import (
    ALL_PYTHONS,
    ALL_VERSIONS,
    BATCHTIME_DAY,
    BATCHTIME_WEEK,
    C_EXTS,
    CPYTHONS,
    CPYTHONS_NO_FREE_THREADED,
    DEFAULT_HOST,
    FREE_THREADED_PYTHON,
    HOSTS,
    MAX_PYTHON,
    MAX_VERSION,
    MIN_MAX_PYTHON,
    MIN_PYTHON,
    MIN_VERSION,
    OTHER_HOSTS,
    SYNCS,
    TOPOLOGIES,
    create_variant,
    get_assume_role,
    get_s3_put,
    get_standard_auth_ssl,
    get_subprocess_exec,
    get_suite_mode,
    get_task_name,
    get_variant_name,
    get_versions_from,
    handle_c_ext,
    write_functions_to_file,
    write_tasks_to_file,
    write_variants_to_file,
    zip_cycle,
)
from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import (
    FunctionCall,
    archive_targz_pack,
    attach_results,
    attach_xunit_results,
    ec2_assume_role,
    expansions_update,
    git_get_project,
)
from shrub.v3.evg_task import EvgTask, EvgTaskDependency, EvgTaskRef
from utils import PERF_PYTHON_VERSION

# The wire-protocol compressors exercised by the compression tasks.
COMPRESSORS = ["snappy", "zlib", "zstd"]

# The distros sharing the per-suite task definitions.
ALL_DISTROS = ["rhel8", "macos", "win64"]

# The middle stable MongoDB version, anchoring the free-threaded standard task.
STABLE_VERSIONS = [v for v in ALL_VERSIONS if v not in ("rapid", "latest")]
MIDDLE_VERSION = STABLE_VERSIONS[len(STABLE_VERSIONS) // 2]

##############
# Variants
##############


def create_ocsp_variants() -> list[BuildVariant]:
    variants = []
    # OCSP tests on default host with all servers v4.4+.
    # MongoDB servers on Windows and MacOS do not staple OCSP responses and only support RSA.
    # Only test with MongoDB 4.4 and latest.
    for host_name in ["rhel8", "win64", "macos"]:
        host = HOSTS[host_name]
        if host == DEFAULT_HOST:
            tasks = [".ocsp"]
        else:
            tasks = [".ocsp-rsa !.ocsp-staple .latest", ".ocsp-rsa !.ocsp-staple .4.4"]
        variant = create_variant(
            tasks,
            get_variant_name("OCSP", host),
            host=host,
            batchtime=BATCHTIME_WEEK,
        )
        variants.append(variant)
        # Run the stapled OCSP tasks (tagged "pr") on every PR so cryptography-backend
        # regressions, like the min-deps ML-KEM breakage in PYTHON-6032, are caught before merge
        # instead of on the next weekly batch run.
        if host == DEFAULT_HOST:
            variants.append(
                create_variant(
                    [".ocsp-staple .pr"],
                    get_variant_name("OCSP Staples", host),
                    tags=["pr"],
                    host=host,
                )
            )
    return variants


def create_server_version_variants() -> list[BuildVariant]:
    variants = []
    for version in ALL_VERSIONS:
        display_name = get_variant_name("* MongoDB", version=version)
        # Each version-boundary variant selects only the pr tasks pinned to its version.
        if version in SERVER_VERSION_PR_PYTHONS:
            tasks = [".server-version !.pr", f".server-version .{version}"]
        else:
            tasks = [".server-version !.pr"]
        variant = create_variant(
            tasks,
            display_name,
            version=version,
            host=DEFAULT_HOST,
            tags=["coverage_tag"],
        )
        variants.append(variant)
    return variants


def create_standard_nonlinux_variants() -> list[BuildVariant]:
    variants = []
    base_display_name = "* Test"

    # Test the standard family on each of the other platforms; the platform
    # tags restrict the cells (macOS arm64 starts at 6.0, win32 has no
    # free-threaded CPython).
    for host_name in ("macos", "macos-arm64", "win64", "win32"):
        host = HOSTS[host_name]
        display_name = get_variant_name(base_display_name, host)
        tasks = [f".test-standard .{host_name}"]
        expansions = dict()
        if host_name == "win32":
            # IS_WIN32 makes setup-uv-python.sh use the 32-bit toolchain.
            expansions["IS_WIN32"] = "1"
        elif host_name == "win64":
            tasks.append(".test-no-orchestration")
        tags = ["standard-non-linux"]
        variants.append(
            create_variant(tasks, display_name, host=host, tags=tags, expansions=expansions)
        )

    return variants


def create_encryption_variants() -> list[BuildVariant]:
    variants = []
    tags = ["encryption_tag"]
    batchtime = BATCHTIME_DAY

    # The tasks identify the suite, distro, sync mode, and crypt_shared; each
    # variant picks its distro group(s).
    for host in ["rhel8", "macos", "win64"]:
        tasks = [f".test-encryption .{host}", ".test-string-query-preview"]
        if host == "rhel8":
            tasks.append(".test-encryption-pyopenssl .rhel8")
        display_name = get_variant_name("Encryption", host)
        variants.append(
            create_variant(tasks, display_name, host=host, batchtime=batchtime, tags=tags)
        )
    return variants


def create_load_balancer_variants():
    return [
        create_variant(
            [".test-load-balancer"],
            "Load Balancer",
            host=DEFAULT_HOST,
            batchtime=BATCHTIME_DAY,
        )
    ]


def create_compression_variants():
    # Compression tests - the tasks carry their compressor.
    return [
        create_variant(
            [".test-compression"],
            get_variant_name("Compression", DEFAULT_HOST),
        )
    ]


def create_enterprise_auth_variants():
    variants = []
    for host in ["rhel8", "macos", "win64"]:
        expansions = dict(TEST_NAME="enterprise_auth", AUTH="auth")
        display_name = get_variant_name("Auth Enterprise", host)
        tasks = [".test-standard-auth .auth !.free-threaded"]
        # https://jira.mongodb.org/browse/PYTHON-5586
        if host == "macos":
            tasks = [".test-standard-auth .auth !.free-threaded"]
        if host == "win64":
            # https://jira.mongodb.org/browse/PYTHON-5704
            tasks = [".test-standard-auth .auth !.free-threaded"]
        variant = create_variant(tasks, display_name, host=host, expansions=expansions)
        variants.append(variant)
    return variants


def create_pyopenssl_variants():
    variants = []
    for host in ["rhel8", "macos", "win64"]:
        display_name = get_variant_name("PyOpenSSL", host)
        variants.append(
            create_variant(
                [f".test-pyopenssl .{host}"],
                display_name,
                host=host,
                batchtime=BATCHTIME_DAY,
            )
        )

    return variants


def create_storage_engine_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-storage-inmemory"],
            get_variant_name("Storage InMemory", host),
            host=host,
        )
    ]


def create_stable_api_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-stable-api"],
            get_variant_name("Stable API", host),
            host=host,
            tags=["versionedApi_tag"],
        )
    ]


def create_green_framework_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-gevent"],
            get_variant_name("Green Gevent", host),
            host=host,
        )
    ]


def create_no_c_ext_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-no-c-ext"],
            get_variant_name("No C Ext", host),
            host=host,
        )
    ]


def create_disable_test_commands_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-disable-test-commands"],
            get_variant_name("Disable test commands", host),
            host=host,
        )
    ]


def create_test_numpy_tasks():
    tasks = []
    for python in MIN_MAX_PYTHON:
        for win32 in (False, True):
            tags = ["binary", "vector", f"python-{python}"]
            vars = dict(UV_PYTHON=python)
            name_base = "test-numpy"
            if win32:
                # Only the windows variant selects these: IS_WIN32 makes
                # setup-uv-python.sh use the 32-bit toolchain (C:/python/32).
                tags.append("test-numpy-win32")
                vars["IS_WIN32"] = "1"
                name_base += "-win32"
            else:
                tags.append("test-numpy")
                if python == MIN_MAX_PYTHON[-1]:
                    tags.append("pr")
                    vars["COVERAGE"] = "1"
            task_name = get_task_name(name_base, python=python, **vars)
            test_func = FunctionCall(func="test numpy", vars=vars)
            tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))
    return tasks


def create_test_numpy_variants() -> list[BuildVariant]:
    variants = []
    base_display_name = "Test Numpy"

    # Test a subset on each of the other platforms: Linux, Windows (the 32-bit
    # and 64-bit tasks in one variant on their shared distro, named to avoid
    # create_variant's "Win64" display-name redirect), and macOS Apple
    # silicon. The Intel macOS host is not tested.
    for host_name, display_name in (
        ("rhel8", None),
        ("win64", f"{base_display_name} Windows"),
        ("macos-arm64", None),
    ):
        tasks = [".test-numpy"]
        if host_name == "win64":
            tasks.append(".test-numpy-win32")
        host = HOSTS[host_name]
        tags = ["binary", "vector"]
        if host_name == "rhel8":
            tags.append("pr")
        if display_name is None:
            display_name = get_variant_name(base_display_name, host)  # noqa: PLW2901
        variant = create_variant(tasks, display_name, host=host, tags=tags)
        variants.append(variant)

    return variants


def create_oidc_auth_variants():
    variants = []
    for host_name in ["ubuntu22", "macos", "win64"]:
        if host_name == "ubuntu22":
            tasks = [".auth_oidc_remote"]
        else:
            tasks = ["!.auth_oidc_remote .auth_oidc"]
        host = HOSTS[host_name]
        variants.append(
            create_variant(
                tasks,
                get_variant_name("Auth OIDC", host),
                host=host,
                batchtime=BATCHTIME_DAY,
            )
        )
        # Add a specific local test to run on PRs.
        if host_name == "ubuntu22":
            tasks = ["!.auth_oidc_remote .auth_oidc"]
            variants.append(
                create_variant(
                    tasks,
                    get_variant_name("Auth OIDC Local", host),
                    tags=["pr"],
                    host=host,
                    batchtime=BATCHTIME_DAY,
                    expansions=dict(COVERAGE="1"),
                )
            )
    return variants


def create_search_index_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".search_index"],
            get_variant_name("Search Index Helpers", host),
            host=host,
        )
    ]


def create_mockupdb_variants():
    host = DEFAULT_HOST
    expansions = dict(TEST_NAME="mockupdb")
    return [
        create_variant(
            [".test-no-orchestration"],
            get_variant_name("MockupDB", host),
            host=host,
            tags=["pr"],
            expansions=expansions,
        )
    ]


def create_doctests_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-doctest"],
            get_variant_name("Doctests", host),
            host=host,
        )
    ]


def create_atlas_connect_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-no-orchestration"],
            get_variant_name("Atlas connect", host),
            tags=["pr"],
            host=DEFAULT_HOST,
            expansions=dict(TEST_NAME="atlas_connect"),
        )
    ]


def create_sfp_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-no-orchestration"],
            get_variant_name("SFP", host),
            tags=["pr"],
            host=host,
            expansions=dict(TEST_NAME="sfp"),
        )
    ]


def create_coverage_report_variants():
    return [create_variant(["coverage-report"], "Coverage Report", host=DEFAULT_HOST)]


def create_kms_variants():
    tasks = []
    tasks.append(EvgTaskRef(name="test-gcpkms", batchtime=BATCHTIME_DAY))
    tasks.append("test-gcpkms-fail")
    tasks.append(EvgTaskRef(name="test-azurekms", batchtime=BATCHTIME_DAY))
    tasks.append("test-azurekms-fail")
    return [create_variant(tasks, "KMS", host=HOSTS["debian11"])]


def create_import_time_variants():
    return [create_variant(["check-import-time"], "Import Time", host=DEFAULT_HOST)]


def create_backport_pr_variants():
    return [create_variant(["backport-pr"], "Backport PR", host=DEFAULT_HOST)]


def create_perf_variants():
    host = HOSTS["perf"]
    return [create_variant([".perf"], "Performance Benchmarks", host=host, batchtime=BATCHTIME_DAY)]


def create_aws_auth_variants():
    variants = []

    for host_name in ["rhel8", "win64", "macos"]:
        expansions = dict()
        tasks = [".auth-aws"]
        tags = []
        if host_name == "macos":
            tasks = [".auth-aws !.auth-aws-web-identity !.auth-aws-ec2"]
            tags = ["pr"]
        elif host_name == "win64":
            tasks = [".auth-aws"]
        host = HOSTS[host_name]
        variant = create_variant(
            tasks,
            get_variant_name("Auth AWS", host),
            host=host,
            tags=tags,
            expansions=expansions,
        )
        variants.append(variant)

    # The ECS test must be run on Ubuntu 24 to match the Fargate Config.
    # Fresh tags and expansions: the loop's macOS values must not leak here.
    variant = create_variant(
        [".auth-aws-ecs"],
        get_variant_name("Auth AWS ECS", HOSTS["ubuntu24"]),
        host=HOSTS["ubuntu24"],
        tags=[],
        expansions=dict(),
    )
    variants.append(variant)
    return variants


def create_min_support_variants():
    host = HOSTS["rhel8"]
    name = get_variant_name("Min Support", host=host)
    return [create_variant([".test-min-support"], name, host=host)]


def create_no_server_variants():
    host = HOSTS["rhel8"]
    name = get_variant_name("No server", host=host)
    return [create_variant([".test-no-orchestration"], name, host=host, tags=["pr"])]


def create_alternative_hosts_variants():
    batchtime = BATCHTIME_DAY
    variants = []

    version = "latest"
    for host_name in OTHER_HOSTS:
        expansions = dict(VERSION="latest")
        handle_c_ext(C_EXTS[0], expansions)
        host = HOSTS[host_name]
        tags = []
        if "fips" in host_name.lower():
            expansions["REQUIRE_FIPS"] = "1"
            # Use explicit Python 3.11 binary on the host since the default python3 is 3.9.
            expansions["UV_PYTHON"] = "/usr/bin/python3.11"
        if "amazon" in host_name.lower():
            tags.append("pr")
        variants.append(
            create_variant(
                [".test-no-toolchain"],
                display_name=get_variant_name("Other hosts", host, version=version),
                batchtime=batchtime,
                host=host,
                tags=tags,
                expansions=expansions,
            )
        )
    return variants


def create_aws_lambda_variants():
    host = HOSTS["rhel8"]
    return [create_variant([".aws_lambda"], display_name="FaaS Lambda", host=host)]


##############
# Tasks
##############


# The server-version pr tasks: the Python boundaries pinned per server version.
SERVER_VERSION_PR_PYTHONS = {
    MIN_VERSION: {("standalone", "noauth", "nossl"): MIN_PYTHON},
    MAX_VERSION: {
        ("standalone", "noauth", "nossl"): MIN_PYTHON,
        ("replica_set", "noauth", "nossl"): MAX_PYTHON,
        ("sharded_cluster", "auth", "ssl"): MAX_PYTHON,
    },
}


def create_server_version_tasks():
    def make_task(topology, auth, ssl, sync, python, extra_tags=(), **extra_vars):
        combo = f"{topology}-{auth}-{ssl}"
        tags = ["server-version", f"python-{python}", combo, sync, *extra_tags]
        expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology)
        if python == ALL_PYTHONS[0]:
            expansions["TEST_MIN_DEPS"] = "1"
        expansions.update(extra_vars)
        if "t" in python:
            tags.append("free-threaded")
        name = get_task_name(
            "test-server-version",
            python=python,
            sync=sync,
            **expansions,
        )
        server_func = FunctionCall(func="run server", vars=expansions)
        test_vars = expansions.copy()
        test_vars["UV_PYTHON"] = python
        test_vars["TEST_NAME"] = f"default_{sync}"
        test_func = FunctionCall(func="run tests", vars=test_vars)
        return EvgTask(name=name, tags=tags, commands=[server_func, test_func])

    tasks = []
    task_combos = set()
    # All combinations of topology, auth, ssl, and sync should be tested.
    for (topology, auth, ssl, sync), rotated in zip_cycle(
        list(product(TOPOLOGIES, ["auth", "noauth"], ["ssl", "nossl"], SYNCS)), ALL_PYTHONS
    ):
        task_combos.add((topology, auth, ssl, sync, rotated))

    # Every python should be tested with sharded cluster, auth, ssl, with sync and async.
    for python, sync in product(ALL_PYTHONS, SYNCS):
        task_combos.add(("sharded_cluster", "auth", "ssl", sync, python))

    # Rotated tasks inherit the variant's VERSION; boundary-Python tasks keep
    # COVERAGE without pr so per-commit coverage is not pr-only.
    pr_pythons = {
        combo: python
        for combos in SERVER_VERSION_PR_PYTHONS.values()
        for combo, python in combos.items()
    }
    for topology, auth, ssl, sync, python in sorted(task_combos):
        extra_vars = {}
        if python == pr_pythons.get((topology, auth, ssl)):
            extra_vars["COVERAGE"] = "1"
        tasks.append(make_task(topology, auth, ssl, sync, python, **extra_vars))

    # The pinned pr tasks: the boundary Pythons per server version, stable as CPYTHONS changes.
    for version, combos in sorted(SERVER_VERSION_PR_PYTHONS.items()):
        for (topology, auth, ssl), python in sorted(combos.items()):
            for sync in SYNCS:
                tasks.append(
                    make_task(
                        topology,
                        auth,
                        ssl,
                        sync,
                        python,
                        extra_tags=("pr", version),
                        VERSION=version,
                        COVERAGE="1",
                    )
                )
    return tasks


def create_no_toolchain_tasks():
    tasks = []

    for topology, sync in zip_cycle(TOPOLOGIES, SYNCS):
        auth, ssl = get_standard_auth_ssl(topology)
        tags = [
            "test-no-toolchain",
            f"{topology}-{auth}-{ssl}",
        ]
        expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology)
        name = get_task_name("test-no-toolchain", sync=sync, **expansions)
        server_func = FunctionCall(func="run server", vars=expansions)
        test_vars = expansions.copy()
        test_vars["TEST_NAME"] = f"default_{sync}"
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
    return tasks


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
    """One task of a per-suite family, identified by its name and tags. The
    name carries no distro, so the same definitions are listed for every
    distro variant; the distros param tags which variants run the task
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
    """Encryption tasks for one test type: the three standard corners — sync
    min-Python min-deps on the min MongoDB standalone, async max-CPython on
    the max stable MongoDB sharded cluster, and sync max free-threaded CPython
    on the middle MongoDB replica set — plus the max-CPython sharded task
    across the remaining MongoDB versions, including latest and rapid. The
    names carry no distro, so the same tasks are listed for every distro;
    coverage and the pr pin ride rhel8-only twins of the max-stable and
    latest cells, which exceed the 60-min timeout with coverage overhead on
    macOS/Win64 (PYTHON-6167)."""
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
        add(MAX_VERSION, "sharded_cluster", MAX_PYTHON, "async", distros=["macos", "win64"])
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
    # latest cell keeps a rhel8-only coverage twin when the family spans the
    # distros that cannot run coverage.
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
    6.0+ on an authenticated SSL sharded cluster fronted by a load balancer,
    running the full default suite — the load-balancer-marked tests are part
    of it — with the CPython paired one-to-one across the versions
    (PYTHON-6167)."""
    return [
        _create_suite_task(
            "test-load-balancer", version, "sharded_cluster", python, sync, "load_balancer"
        )
        for sync in SYNCS
        for version, python in zip(get_versions_from("6.0"), CPYTHONS)
    ]


def create_test_doctest_tasks():
    """Doctest tasks: one task per MongoDB version on a standalone server."""
    return [
        _create_suite_task("test-doctest", version, "standalone", python, "", "doctest")
        for version, python in zip_cycle(ALL_VERSIONS, CPYTHONS)
    ]


def create_test_compression_tasks():
    """Compression tasks: one sync task per compressor on the latest replica
    set plus one async sharded-cluster task per compressor and MongoDB version
    for every other version, with the CPython rotated across the cells, so
    each compressor is tested against every MongoDB version without
    duplicating cells (the compressors differ in client-side support, the
    versions in server-side wire support). Each task runs the default suite
    with its compressor enabled on every client (PYTHON-6167)."""
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
        # Async, auth, SSL, sharded cluster.
        tasks.append(
            _create_suite_task(
                "test-compression",
                version,
                "sharded_cluster",
                python,
                "async",
                "default",
                suffix=compressor,
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
    """PyOpenSSL tasks: one task per MongoDB version — sync on the latest
    replica set, async on the sharded-cluster rotation — running the default
    suite with the PyOpenSSL extra installed (SUB_TEST_NAME feeds
    setup_tests.py), so the PyOpenSSL SSL backend is exercised against every
    server version. The tasks run on every distro with the CPython rotated
    across the cells; free-threaded CPython is covered by the
    encryption-pyopenssl family on RHEL 8, the distro setup_tests.py equips
    with OpenSSL 3 (PYTHON-6167)."""
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
    ]


def _create_rotation_tasks(
    family, extra_test_vars=None, extra_expansions=None, pythons=CPYTHONS, sync=None
):
    """One task per MongoDB version, the topology cycling through standalone,
    replica set, and sharded cluster (PYTHON-6167)."""
    return [
        _create_suite_task(
            family,
            version,
            topology,
            python,
            sync if sync else get_suite_mode(topology),
            "default",
            extra_test_vars=extra_test_vars,
            extra_expansions=extra_expansions,
        )
        for version, topology, python in zip_cycle(ALL_VERSIONS, TOPOLOGIES, pythons)
    ]


def create_test_gevent_tasks():
    """Gevent tasks: one task per MongoDB version, the topology cycling
    through standalone, replica set, and sharded cluster, running the sync
    default suite under gevent; run_tests.py monkey-patches with gevent and
    skips the async tests. Free-threaded CPython is not supported by gevent
    (PYTHON-6167)."""
    return _create_rotation_tasks(
        "test-gevent",
        extra_test_vars=dict(GREEN_FRAMEWORK="gevent"),
        pythons=CPYTHONS_NO_FREE_THREADED,
        sync="sync",
    )


def create_test_disable_test_commands_tasks():
    """Disable-test-commands tasks: one task per MongoDB version, the topology
    cycling through standalone, replica set, and sharded cluster (each uses
    its own disableTestCommands orchestration config), running the default
    suite against a server started without enableTestCommands. The mode
    follows the encryption corners: sync on the standalone and replica-set
    cells, async on the sharded clusters (PYTHON-6167)."""
    return _create_rotation_tasks(
        "test-disable-test-commands",
        # Shared with the run server call, whose orchestration picks the
        # disableTestCommands config from it.
        extra_expansions=dict(DISABLE_TEST_COMMANDS="1"),
    )


def create_test_no_c_ext_tasks():
    """No-c-ext tasks: one task per MongoDB version, the topology cycling
    through standalone, replica set, and sharded cluster, running the default
    suite without the C extensions (NO_EXT feeds run_tests.py). The mode
    follows the encryption corners: sync on the standalone and replica-set
    cells, async on the sharded clusters (PYTHON-6167)."""
    return _create_rotation_tasks("test-no-c-ext", extra_test_vars=dict(NO_EXT="1"))


def create_test_stable_api_tasks():
    """Stable API tasks: one require-v1 task per MongoDB version 5.0+ on an
    authenticated SSL sharded cluster started with requireApiVersion=1, and
    one accept-v2 task per version on a standalone server from the
    versioned-api-testing orchestration, with the CPython rotated across the
    versions. The require-v1 tasks pin apiVersion=1 on the suite; the
    accept-v2 tasks do not pin an apiVersion (PYTHON-6167)."""
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
    return [
        _create_suite_task(
            "test-stable-api",
            version,
            topology,
            python,
            sync,
            "default",
            suffix=suffix,
            extra_expansions=extra_expansions,
        )
        for version, python in zip_cycle(get_versions_from("5.0"), CPYTHONS)
        for suffix, topology, sync, extra_expansions in cells
    ]


def create_test_storage_inmemory_tasks():
    """InMemory storage engine tasks: one sync standalone task per MongoDB
    version, running the default suite against a server started with
    STORAGE_ENGINE=inmemory (PYTHON-6167)."""
    return [
        _create_suite_task(
            "test-storage-inmemory",
            version,
            "standalone",
            python,
            "sync",
            "default",
            extra_expansions=dict(STORAGE_ENGINE="inmemory"),
        )
        for version, python in zip_cycle(ALL_VERSIONS, CPYTHONS)
    ]


def create_string_query_preview_tasks():
    """Tasks for the preview Queryable Encryption string query types.

    The preview types need a server >= 8.2 and < 9.0; ALL_VERSIONS jumps from
    8.0 to 9.0, so they need a dedicated task. setup_tests.py pins the
    released pymongocrypt for 8.x, which bundles a libmongocrypt still
    carrying the preview types.
    """
    python = CPYTHONS[-1]
    topology = "replica_set"
    auth, ssl = get_standard_auth_ssl(topology)
    expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology, VERSION="8.2")
    tags = [
        "test-string-query-preview",
        "server-8.2",
        f"python-{python}",
        f"{topology}-{auth}-{ssl}",
        auth,
    ]
    name = get_task_name("test-string-query-preview", python=python, **expansions)
    server_func = FunctionCall(func="run server", vars=expansions)
    test_vars = expansions.copy()
    test_vars["UV_PYTHON"] = python
    test_func = FunctionCall(func="run tests", vars=test_vars)
    return [EvgTask(name=name, tags=tags, commands=[server_func, test_func])]


def create_test_standard_auth_tasks():
    """We only use auth on sharded clusters"""
    tasks = []
    task_combos = set()
    # Rotate through the CPython and MongoDB versions
    for (version, topology), python in zip_cycle(
        list(product(ALL_VERSIONS, ["sharded_cluster"])), CPYTHONS
    ):
        task_combos.add((version, topology, python))
    # The pinned PR tasks: the min and max Python on MAX_VERSION.
    task_combos.add((MAX_VERSION, "sharded_cluster", MIN_PYTHON))
    task_combos.add((MAX_VERSION, "sharded_cluster", MAX_PYTHON))
    for version, topology, python in sorted(task_combos):
        pr = version == MAX_VERSION and python in (MIN_PYTHON, MAX_PYTHON)
        auth, ssl = get_standard_auth_ssl(topology)
        tags = [
            "test-standard-auth",
            f"server-{version}",
            f"python-{python}",
            f"{topology}-{auth}-{ssl}",
            auth,
        ]
        if "t" in python:
            tags.append("free-threaded")
        if pr:
            tags.append("pr")
        expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology, VERSION=version)
        if python == ALL_PYTHONS[0]:
            expansions["TEST_MIN_DEPS"] = "1"
        if pr:
            expansions["COVERAGE"] = "1"
        name = get_task_name("test-standard-auth", python=python, **expansions)
        server_func = FunctionCall(func="run server", vars=expansions)
        test_vars = expansions.copy()
        test_vars["UV_PYTHON"] = python
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
    return tasks


def create_standard_tasks():
    """The standard suite on the non-linux platforms, one task per MongoDB
    version following the encryption corners: sync min-Python min-deps on the
    min MongoDB standalone, sync max free-threaded CPython on the middle
    MongoDB replica set, and async max-CPython on the max stable MongoDB
    sharded cluster and the remaining versions, with coverage on latest. The
    names carry no distro; the platform tags restrict the cells (macOS arm64
    starts at 6.0, win32 has no free-threaded CPython) (PYTHON-6167)."""
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
        # Async, max CPython, max stable MongoDB, auth, SSL, sharded cluster.
        (MAX_VERSION, "sharded_cluster", MAX_PYTHON, "async", all_platforms, {}),
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


def create_no_orchestration_tasks():
    tasks = []
    for python in MIN_MAX_PYTHON:
        tags = [
            "test-no-orchestration",
            f"python-{python}",
        ]
        assume_func = FunctionCall(func="assume ec2 role")
        test_vars = dict(UV_PYTHON=python)
        if python == ALL_PYTHONS[0]:
            test_vars["TEST_MIN_DEPS"] = "1"
        name = get_task_name("test-no-orchestration", **test_vars)
        test_func = FunctionCall(func="run tests", vars=test_vars)
        commands = [assume_func, test_func]
        tasks.append(EvgTask(name=name, tags=tags, commands=commands))
    return tasks


def create_kms_tasks():
    tasks = []
    for kms_type in ["gcp", "azure"]:
        for success in [True, False]:
            name = f"test-{kms_type}kms"
            sub_test_name = kms_type
            tags = []
            if not success:
                name += "-fail"
                sub_test_name += "-fail"
                tags.append("pr")
            commands = []
            if not success:
                commands.append(FunctionCall(func="run server"))
            test_vars = dict(TEST_NAME="kms", SUB_TEST_NAME=sub_test_name)
            test_func = FunctionCall(func="run tests", vars=test_vars)
            commands.append(test_func)
            tasks.append(EvgTask(name=name, tags=tags, commands=commands))
    return tasks


def create_aws_tasks():
    tasks = []
    aws_test_types = [
        "regular",
        "assume-role",
        "ec2",
        "env-creds",
        "session-creds",
        "web-identity",
    ]
    assume_func = FunctionCall(func="assume ec2 role")
    task_combos = set(zip_cycle(get_versions_from("4.4"), aws_test_types, CPYTHONS))
    # The pinned PR tasks: the min and max Python on MAX_VERSION.
    task_combos |= {
        (MAX_VERSION, "regular", MIN_PYTHON),
        (MAX_VERSION, "regular", MAX_PYTHON),
    }
    for version, test_type, python in sorted(task_combos):
        base_name = f"test-auth-aws-{version}"
        base_tags = ["auth-aws"]
        server_vars = dict(AUTH_AWS="1", VERSION=version)
        server_func = FunctionCall(func="run server", vars=server_vars)
        tags = [*base_tags, f"auth-aws-{test_type}"]
        if "t" in python:
            tags.append("free-threaded")
        test_vars = dict(TEST_NAME="auth_aws", SUB_TEST_NAME=test_type, UV_PYTHON=python)
        # The rotation never produces (MAX_VERSION, regular).
        pr = (
            version == MAX_VERSION and test_type == "regular" and python in (MIN_PYTHON, MAX_PYTHON)
        )
        if python == MIN_MAX_PYTHON[0]:
            test_vars["TEST_MIN_DEPS"] = "1"
        if pr:
            tags.append("pr")
        if pr or python == MIN_MAX_PYTHON[-1]:
            test_vars["COVERAGE"] = "1"
        name = get_task_name(f"{base_name}-{test_type}", **test_vars)
        test_func = FunctionCall(func="run tests", vars=test_vars)
        funcs = [server_func, assume_func, test_func]
        tasks.append(EvgTask(name=name, tags=tags, commands=funcs))

        if test_type == "web-identity":
            tags = [*base_tags, "auth-aws-web-identity"]
            name = get_task_name(f"{base_name}-web-identity-session-name", python=python)
            test_vars = dict(
                TEST_NAME="auth_aws",
                SUB_TEST_NAME="web-identity",
                AWS_ROLE_SESSION_NAME="test",
                UV_PYTHON=python,
            )
            if "t" in python:
                tags.append("free-threaded")
            test_func = FunctionCall(func="run tests", vars=test_vars)
            funcs = [server_func, assume_func, test_func]
            tasks.append(EvgTask(name=name, tags=tags, commands=funcs))

    # Add the ECS task.  This will run on Ubuntu 24 to match the
    # Fargate environment.
    tags = ["auth-aws-ecs"]
    test_vars = dict(TEST_NAME="auth_aws", SUB_TEST_NAME="ecs")
    name = get_task_name("test-auth-aws-ecs", **test_vars)
    test_func = FunctionCall(func="run tests", vars=test_vars)
    server_func = FunctionCall(func="run server", vars=dict(VERSION="8.0"))
    funcs = [assume_func, server_func, test_func]
    tasks.append(EvgTask(name=name, tags=tags, commands=funcs))

    return tasks


def create_oidc_tasks():
    tasks = []
    for sub_test in ["default", "azure", "gcp", "eks", "aks", "gke"]:
        vars = dict(TEST_NAME="auth_oidc", SUB_TEST_NAME=sub_test)
        tags = ["auth_oidc"]
        if sub_test != "default":
            tags.append("auth_oidc_remote")
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = get_task_name(f"test-auth-oidc-{sub_test}", **vars)
        tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))

    return tasks


def _create_ocsp_tasks(algo, variant, server_type, base_task_name):
    tasks = []
    file_name = f"{algo}-basic-tls-ocsp-{variant}.json"

    for version in get_versions_from("4.4"):
        # Rotated nightlies: latest-CPython on "latest", min-deps Python elsewhere.
        pr = base_task_name == "valid-cert-server-staples" and algo == "ecdsa" and version == "4.4"
        # The "latest" ECDSA staples task keeps nightly coverage; the pinned
        # MAX_VERSION task below is the pr task.
        nightly_cov = (
            base_task_name == "valid-cert-server-staples"
            and algo == "ecdsa"
            and version == "latest"
        )
        if version == "latest":
            python = MIN_MAX_PYTHON[-1]
        else:
            python = MIN_MAX_PYTHON[0]

        vars = dict(
            ORCHESTRATION_FILE=file_name,
            OCSP_SERVER_TYPE=server_type,
            TEST_NAME="ocsp",
            UV_PYTHON=python,
            VERSION=version,
        )
        if python == ALL_PYTHONS[0]:
            vars["TEST_MIN_DEPS"] = "1"
        tags = ["ocsp", f"ocsp-{algo}", version]
        if "disableStapling" not in variant:
            tags.append("ocsp-staple")
        if pr or nightly_cov:
            vars["COVERAGE"] = "1"
        if pr:
            tags.append("pr")
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = get_task_name(f"test-ocsp-{algo}-{base_task_name}", **vars)
        tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))

    # One latest-CPython ECDSA staples task on every PR, pinned to MAX_VERSION
    # (the "latest" nightly is unstable): catches cryptography-backend
    # regressions at the dependency extremes before merge (PYTHON-6032).
    if base_task_name == "valid-cert-server-staples" and algo == "ecdsa":
        vars = dict(
            ORCHESTRATION_FILE=file_name,
            OCSP_SERVER_TYPE=server_type,
            TEST_NAME="ocsp",
            UV_PYTHON=MIN_MAX_PYTHON[-1],
            VERSION=MAX_VERSION,
            COVERAGE="1",
        )
        tags = ["ocsp", f"ocsp-{algo}", MAX_VERSION, "ocsp-staple", "pr"]
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = get_task_name(f"test-ocsp-{algo}-{base_task_name}", **vars)
        tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))

    return tasks


def create_min_support_tasks():
    server_func = FunctionCall(func="run server")
    from generate_config_utils import MIN_SUPPORT_VERSIONS

    tasks = []
    for python, topology in product(MIN_SUPPORT_VERSIONS, TOPOLOGIES):
        auth, ssl = get_standard_auth_ssl(topology)
        vars = dict(UV_PYTHON=python, AUTH=auth, SSL=ssl, TOPOLOGY=topology)
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = get_task_name(
            "test-min-support", python=python, topology=topology, auth=auth, ssl=ssl
        )
        tags = ["test-min-support"]
        commands = [server_func, test_func]
        tasks.append(EvgTask(name=task_name, tags=tags, commands=commands))
    return tasks


def create_aws_lambda_tasks():
    assume_func = FunctionCall(func="assume ec2 role")
    vars = dict(TEST_NAME="aws_lambda")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-aws-lambda-deployed"
    tags = ["aws_lambda"]
    commands = [assume_func, test_func]
    return [EvgTask(name=task_name, tags=tags, commands=commands)]


def create_search_index_tasks():
    assume_func = FunctionCall(func="assume ec2 role")
    server_func = FunctionCall(func="run server", vars=dict(TEST_NAME="search_index"))
    vars = dict(TEST_NAME="search_index", UV_PYTHON=CPYTHONS[0])
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-search-index-helpers"
    tags = ["search_index"]
    commands = [assume_func, server_func, test_func]
    return [EvgTask(name=task_name, tags=tags, commands=commands)]


def create_perf_tasks():
    tasks = []
    for version, ssl, sync in product(["8.0"], ["ssl", "nossl"], ["sync", "async"]):
        # The toolchain build may be this same patch version but is not
        # optimized, which regressed CPU-bound benchmarks (PYTHON-6135).
        perf_vars = dict(UV_PYTHON=PERF_PYTHON_VERSION, UV_PYTHON_PREFERENCE="only-managed")
        vars = dict(VERSION=f"v{version}-perf", SSL=ssl, **perf_vars)
        server_func = FunctionCall(func="run server", vars=vars)
        vars = dict(TEST_NAME="perf", SUB_TEST_NAME=sync, **perf_vars)
        test_func = FunctionCall(func="run tests", vars=vars)
        attach_func = FunctionCall(func="attach benchmark test results")
        send_func = FunctionCall(func="send dashboard data")
        task_name = f"perf-{version}-standalone"
        if ssl == "ssl":
            task_name += "-ssl"
        if sync == "async":
            task_name += "-async"
        tags = ["perf"]
        commands = [server_func, test_func, attach_func, send_func]
        tasks.append(EvgTask(name=task_name, tags=tags, commands=commands))
    return tasks


def create_getdata_tasks():
    # Wildcard task. Do you need to find out what tools are available and where?
    # Throw it here, and execute this task on all buildvariants
    cmd = get_subprocess_exec(args=[".evergreen/scripts/run-getdata.sh"])
    return [EvgTask(name="getdata", commands=[cmd])]


def create_coverage_report_tasks():
    tags = ["coverage", "pr"]
    task_name = "coverage-report"
    # BUILD-3165: We can't use "*" (all tasks) and specify "variant".
    # Instead list out all coverage tasks using tags.
    # Run the coverage task even if some tasks fail.
    # Run the coverage task even if some tasks are not scheduled in a patch build.
    task_deps = [
        EvgTaskDependency(
            name=".server-version", variant=".coverage_tag", status="*", patch_optional=True
        )
    ]
    cmd = FunctionCall(func="download and merge coverage")
    return [EvgTask(name=task_name, tags=tags, depends_on=task_deps, commands=[cmd])]


def create_import_time_tasks():
    name = "check-import-time"
    tags = ["pr"]
    args = [".evergreen/scripts/check-import-time.sh", "${revision}", "${github_commit}"]
    cmd = get_subprocess_exec(args=args)
    return [EvgTask(name=name, tags=tags, commands=[cmd])]


def create_backport_pr_tasks():
    name = "backport-pr"
    args = [
        "${DRIVERS_TOOLS}/.evergreen/github_app/backport-pr.sh",
        "mongodb",
        "mongo-python-driver",
        "${github_commit}",
    ]
    include_expansions = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
    cmd = get_subprocess_exec(args=args, include_expansions_in_env=include_expansions)
    assume_func = FunctionCall(func="assume ec2 role")
    return [EvgTask(name=name, commands=[assume_func, cmd], allowed_requesters=["commit"])]


def create_ocsp_tasks():
    tasks = []
    tests = [
        ("disableStapling", "valid", "valid-cert-server-does-not-staple"),
        ("disableStapling", "revoked", "invalid-cert-server-does-not-staple"),
        ("disableStapling", "valid-delegate", "delegate-valid-cert-server-does-not-staple"),
        ("disableStapling", "revoked-delegate", "delegate-invalid-cert-server-does-not-staple"),
        ("disableStapling", "no-responder", "soft-fail"),
        ("mustStaple", "valid", "valid-cert-server-staples"),
        ("mustStaple", "revoked", "invalid-cert-server-staples"),
        ("mustStaple", "valid-delegate", "delegate-valid-cert-server-staples"),
        ("mustStaple", "revoked-delegate", "delegate-invalid-cert-server-staples"),
        (
            "mustStaple-disableStapling",
            "revoked",
            "malicious-invalid-cert-mustStaple-server-does-not-staple",
        ),
        (
            "mustStaple-disableStapling",
            "revoked-delegate",
            "delegate-malicious-invalid-cert-mustStaple-server-does-not-staple",
        ),
        (
            "mustStaple-disableStapling",
            "no-responder",
            "malicious-no-responder-mustStaple-server-does-not-staple",
        ),
    ]
    for algo in ["ecdsa", "rsa"]:
        for variant, server_type, base_task_name in tests:
            new_tasks = _create_ocsp_tasks(algo, variant, server_type, base_task_name)
            tasks.extend(new_tasks)

    return tasks


##############
# Functions
##############


def create_upload_coverage_func():
    # Upload the coverage report for all tasks in a single build to the same directory.
    remote_file = (
        "coverage/${revision}/${version_id}/coverage/coverage.${build_variant}.${task_name}"
    )
    display_name = "Raw Coverage Report"
    cmd = get_s3_put(
        local_file="src/.coverage",
        remote_file=remote_file,
        display_name=display_name,
        content_type="text/html",
    )
    return "upload coverage", [get_assume_role(), cmd]


def create_upload_coverage_codecov_func():
    # Upload the coverage xml report to codecov.
    include_expansions = [
        "CODECOV_TOKEN",
        "build_variant",
        "task_name",
        "github_commit",
        "github_pr_number",
        "github_pr_head_branch",
        "github_author",
        "requester",
        "branch_name",
    ]
    args = [
        ".evergreen/scripts/upload-codecov.sh",
    ]
    upload_cmd = get_subprocess_exec(include_expansions_in_env=include_expansions, args=args)
    return "upload codecov", [upload_cmd]


def create_download_and_merge_coverage_func():
    include_expansions = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
    args = [
        ".evergreen/scripts/download-and-merge-coverage.sh",
        "${bucket_name}",
        "${revision}",
        "${version_id}",
    ]
    merge_cmd = get_subprocess_exec(
        silent=True, include_expansions_in_env=include_expansions, args=args
    )
    combine_cmd = get_subprocess_exec(args=[".evergreen/combine-coverage.sh"])
    # Upload the resulting html coverage report.
    args = [
        ".evergreen/scripts/upload-coverage-report.sh",
        "${bucket_name}",
        "${revision}",
        "${version_id}",
    ]
    upload_cmd = get_subprocess_exec(
        silent=True, include_expansions_in_env=include_expansions, args=args
    )
    display_name = "Coverage Report HTML"
    remote_file = "coverage/${revision}/${version_id}/htmlcov/index.html"
    put_cmd = get_s3_put(
        local_file="src/htmlcov/index.html",
        remote_file=remote_file,
        display_name=display_name,
        content_type="text/html",
    )
    cmds = [get_assume_role(), merge_cmd, combine_cmd, upload_cmd, put_cmd]
    return "download and merge coverage", cmds


def create_upload_mo_artifacts_func():
    include = ["./**.core", "./**.mdmp"]  # Windows: minidumps
    archive_cmd = archive_targz_pack(target="mongo-coredumps.tgz", source_dir="./", include=include)
    display_name = "Core Dumps - Execution"
    remote_file = "${build_variant}/${revision}/${version_id}/${build_id}/coredumps/${task_id}-${execution}-mongodb-coredumps.tar.gz"
    s3_dumps = get_s3_put(
        local_file="mongo-coredumps.tgz", remote_file=remote_file, display_name=display_name
    )
    display_name = "drivers-tools-logs.tar.gz"
    remote_file = "${build_variant}/${revision}/${version_id}/${build_id}/logs/${task_id}-${execution}-drivers-tools-logs.tar.gz"
    s3_logs = get_s3_put(
        local_file="${DRIVERS_TOOLS}/.evergreen/test_logs.tar.gz",
        remote_file=remote_file,
        display_name=display_name,
    )
    cmds = [get_assume_role(), archive_cmd, s3_dumps, s3_logs]
    return "upload mo artifacts", cmds


def create_fetch_source_func():
    # Executes clone and applies the submitted patch, if any.
    cmd = git_get_project(directory="src")
    return "fetch source", [cmd]


def create_setup_system_func():
    # Make an evergreen expansion file with dynamic values.
    includes = ["is_patch", "project", "version_id"]
    args = [".evergreen/scripts/setup-system.sh"]
    setup_cmd = get_subprocess_exec(include_expansions_in_env=includes, args=args)
    # Load the expansion file to make an evergreen variable with the current unique version.
    expansion_cmd = expansions_update(file="src/expansion.yml")
    return "setup system", [setup_cmd, expansion_cmd]


def create_upload_test_results_func():
    results_cmd = attach_results(file_location="${DRIVERS_TOOLS}/results.json")
    xresults_cmd = attach_xunit_results(file="src/xunit-results/TEST-*.xml")
    return "upload test results", [results_cmd, xresults_cmd]


def create_run_server_func():
    includes = [
        "VERSION",
        "TOPOLOGY",
        "AUTH",
        "SSL",
        "ORCHESTRATION_FILE",
        "UV_PYTHON",
        "UV_PYTHON_PREFERENCE",
        "STORAGE_ENGINE",
        "REQUIRE_API_VERSION",
        "DRIVERS_TOOLS",
        "TEST_CRYPT_SHARED",
        "AUTH_AWS",
        "LOAD_BALANCER",
        "LOCAL_ATLAS",
        "NO_EXT",
        # The orchestration picks the disableTestCommands config from it.
        "DISABLE_TEST_COMMANDS",
        # The AWS credentials are needed to download the "latest" nightly
        # build from the private S3 bucket, see DRIVERS-3628.
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ]
    args = [".evergreen/just.sh", "run-server", "${TEST_NAME}"]
    sub_cmd = get_subprocess_exec(include_expansions_in_env=includes, args=args)
    expansion_cmd = expansions_update(file="${DRIVERS_TOOLS}/mo-expansion.yml")
    return "run server", [sub_cmd, expansion_cmd]


def create_run_tests_func():
    includes = [
        "AUTH",
        "SSL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "COVERAGE",
        "UV_PYTHON",
        "UV_PYTHON_PREFERENCE",
        "LIBMONGOCRYPT_URL",
        "MONGODB_URI",
        "DISABLE_TEST_COMMANDS",
        "GREEN_FRAMEWORK",
        "NO_EXT",
        "COMPRESSORS",
        "MONGODB_API_VERSION",
        "REQUIRE_API_VERSION",
        "DEBUG_LOG",
        "DISABLE_FLAKY",
        "ORCHESTRATION_FILE",
        "OCSP_SERVER_TYPE",
        "VERSION",
        "IS_WIN32",
        "REQUIRE_FIPS",
        "TEST_MIN_DEPS",
        "SYNC",
    ]
    args = [".evergreen/just.sh", "setup-tests", "${TEST_NAME}", "${SUB_TEST_NAME}"]
    setup_cmd = get_subprocess_exec(include_expansions_in_env=includes, args=args)
    test_cmd = get_subprocess_exec(args=[".evergreen/just.sh", "run-tests"])
    return "run tests", [setup_cmd, test_cmd]


def create_test_numpy_func():
    includes = ["UV_PYTHON", "COVERAGE", "IS_WIN32"]
    test_cmd = get_subprocess_exec(
        include_expansions_in_env=includes, args=[".evergreen/just.sh", "test-numpy"]
    )
    return "test numpy", [test_cmd]


def create_cleanup_func():
    cmd = get_subprocess_exec(args=[".evergreen/scripts/cleanup.sh"])
    return "cleanup", [cmd]


def create_teardown_system_func():
    tests_cmd = get_subprocess_exec(args=[".evergreen/just.sh", "teardown-tests"])
    drivers_cmd = get_subprocess_exec(args=["${DRIVERS_TOOLS}/.evergreen/teardown.sh"])
    return "teardown system", [tests_cmd, drivers_cmd]


def create_assume_ec2_role_func():
    cmd = ec2_assume_role(role_arn="${aws_test_secrets_role}", duration_seconds=3600)
    return "assume ec2 role", [cmd]


def create_attach_benchmark_test_results_func():
    cmd = attach_results(file_location="src/report.json")
    return "attach benchmark test results", [cmd]


def create_send_dashboard_data_func():
    includes = [
        "requester",
        "revision_order_id",
        "project_id",
        "version_id",
        "build_variant",
        "parsed_order_id",
        "task_name",
        "task_id",
        "execution",
        "is_mainline",
    ]
    cmds = [
        get_subprocess_exec(
            include_expansions_in_env=includes, args=[".evergreen/scripts/perf-submission-setup.sh"]
        ),
        expansions_update(file="src/expansion.yml"),
        get_subprocess_exec(
            include_expansions_in_env=includes, args=[".evergreen/scripts/perf-submission.sh"]
        ),
    ]
    return "send dashboard data", cmds


mod = sys.modules[__name__]
write_variants_to_file(mod)
write_tasks_to_file(mod)
write_functions_to_file(mod)

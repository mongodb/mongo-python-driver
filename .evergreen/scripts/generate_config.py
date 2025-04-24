# Note: See CONTRIBUTING.md for how to update/run this file.
from __future__ import annotations

import sys
from dataclasses import dataclass
from inspect import getmembers, isfunction
from itertools import cycle, product, zip_longest
from pathlib import Path
from typing import Any

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import (
    EvgCommandType,
    FunctionCall,
    archive_targz_pack,
    ec2_assume_role,
    s3_put,
    subprocess_exec,
)
from shrub.v3.evg_project import EvgProject
from shrub.v3.evg_task import EvgTask, EvgTaskDependency, EvgTaskRef
from shrub.v3.shrub_service import ShrubService

##############
# Globals
##############

ALL_VERSIONS = ["4.0", "4.2", "4.4", "5.0", "6.0", "7.0", "8.0", "rapid", "latest"]
CPYTHONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]
PYPYS = ["pypy3.10"]
ALL_PYTHONS = CPYTHONS + PYPYS
MIN_MAX_PYTHON = [CPYTHONS[0], CPYTHONS[-1]]
BATCHTIME_WEEK = 10080
AUTH_SSLS = [("auth", "ssl"), ("noauth", "ssl"), ("noauth", "nossl")]
TOPOLOGIES = ["standalone", "replica_set", "sharded_cluster"]
C_EXTS = ["without_ext", "with_ext"]
# By default test each of the topologies with a subset of auth/ssl.
SUB_TASKS = [
    ".sharded_cluster .auth .ssl",
    ".replica_set .noauth .ssl",
    ".standalone .noauth .nossl",
]
SYNCS = ["sync", "async", "sync_async"]
DISPLAY_LOOKUP = dict(
    ssl=dict(ssl="SSL", nossl="NoSSL"),
    auth=dict(auth="Auth", noauth="NoAuth"),
    test_suites=dict(default="Sync", default_async="Async"),
    coverage=dict(coverage="cov"),
    no_ext={"1": "No C"},
)
HOSTS = dict()


@dataclass
class Host:
    name: str
    run_on: str
    display_name: str
    variables: dict[str, str] | None


# Hosts with toolchains.
HOSTS["rhel8"] = Host("rhel8", "rhel87-small", "RHEL8", dict())
HOSTS["win64"] = Host("win64", "windows-64-vsMulti-small", "Win64", dict())
HOSTS["win32"] = Host("win32", "windows-64-vsMulti-small", "Win32", dict())
HOSTS["macos"] = Host("macos", "macos-14", "macOS", dict())
HOSTS["macos-arm64"] = Host("macos-arm64", "macos-14-arm64", "macOS Arm64", dict())
HOSTS["ubuntu20"] = Host("ubuntu20", "ubuntu2004-small", "Ubuntu-20", dict())
HOSTS["ubuntu22"] = Host("ubuntu22", "ubuntu2204-small", "Ubuntu-22", dict())
HOSTS["rhel7"] = Host("rhel7", "rhel79-small", "RHEL7", dict())
HOSTS["perf"] = Host("perf", "rhel90-dbx-perf-large", "", dict())
HOSTS["debian11"] = Host("debian11", "debian11-small", "Debian11", dict())
DEFAULT_HOST = HOSTS["rhel8"]

# Other hosts
OTHER_HOSTS = ["RHEL9-FIPS", "RHEL8-zseries", "RHEL8-POWER8", "RHEL8-arm64", "Amazon2023"]
for name, run_on in zip(
    OTHER_HOSTS,
    [
        "rhel92-fips",
        "rhel8-zseries-small",
        "rhel8-power-small",
        "rhel82-arm64-small",
        "amazon2023-arm64-latest-large-m8g",
    ],
):
    HOSTS[name] = Host(name, run_on, name, dict())


##############
# Helpers
##############


def create_variant_generic(
    tasks: list[str | EvgTaskRef],
    display_name: str,
    *,
    host: Host | None = None,
    default_run_on="rhel87-small",
    expansions: dict | None = None,
    **kwargs: Any,
) -> BuildVariant:
    """Create a build variant for the given inputs."""
    task_refs = []
    for t in tasks:
        if isinstance(t, EvgTaskRef):
            task_refs.append(t)
        else:
            task_refs.append(EvgTaskRef(name=t))
    expansions = expansions and expansions.copy() or dict()
    if "run_on" in kwargs:
        run_on = kwargs.pop("run_on")
    elif host:
        run_on = [host.run_on]
        if host.variables:
            expansions.update(host.variables)
    else:
        run_on = [default_run_on]
    if isinstance(run_on, str):
        run_on = [run_on]
    name = display_name.replace(" ", "-").replace("*-", "").lower()
    return BuildVariant(
        name=name,
        display_name=display_name,
        tasks=task_refs,
        expansions=expansions or None,
        run_on=run_on,
        **kwargs,
    )


def create_variant(
    tasks: list[str | EvgTaskRef],
    display_name: str,
    *,
    version: str | None = None,
    host: Host | None = None,
    python: str | None = None,
    expansions: dict | None = None,
    **kwargs: Any,
) -> BuildVariant:
    expansions = expansions and expansions.copy() or dict()
    if version:
        expansions["VERSION"] = version
    if python:
        expansions["PYTHON_BINARY"] = get_python_binary(python, host)
    return create_variant_generic(
        tasks, display_name, version=version, host=host, expansions=expansions, **kwargs
    )


def get_python_binary(python: str, host: Host) -> str:
    """Get the appropriate python binary given a python version and host."""
    name = host.name
    if name in ["win64", "win32"]:
        if name == "win32":
            base = "C:/python/32"
        else:
            base = "C:/python"
        python = python.replace(".", "")
        if python == "313t":
            return f"{base}/Python313/python3.13t.exe"
        return f"{base}/Python{python}/python.exe"

    if name in ["rhel8", "ubuntu22", "ubuntu20", "rhel7"]:
        return f"/opt/python/{python}/bin/python3"

    if name in ["macos", "macos-arm64"]:
        if python == "3.13t":
            return "/Library/Frameworks/PythonT.Framework/Versions/3.13/bin/python3t"
        return f"/Library/Frameworks/Python.Framework/Versions/{python}/bin/python3"

    raise ValueError(f"no match found for python {python} on {name}")


def get_versions_from(min_version: str) -> list[str]:
    """Get all server versions starting from a minimum version."""
    min_version_float = float(min_version)
    rapid_latest = ["rapid", "latest"]
    versions = [v for v in ALL_VERSIONS if v not in rapid_latest]
    return [v for v in versions if float(v) >= min_version_float] + rapid_latest


def get_versions_until(max_version: str) -> list[str]:
    """Get all server version up to a max version."""
    max_version_float = float(max_version)
    versions = [v for v in ALL_VERSIONS if v not in ["rapid", "latest"]]
    versions = [v for v in versions if float(v) <= max_version_float]
    if not len(versions):
        raise ValueError(f"No server versions found less <= {max_version}")
    return versions


def get_common_name(base: str, sep: str, **kwargs) -> str:
    display_name = base
    version = kwargs.pop("VERSION", None)
    version = version or kwargs.pop("version", None)
    if version:
        if version not in ["rapid", "latest"]:
            version = f"v{version}"
        display_name = f"{display_name}{sep}{version}"
    for key, value in kwargs.items():
        name = value
        if key.lower() == "python":
            if not value.startswith("pypy"):
                name = f"Python{value}"
            else:
                name = f"PyPy{value.replace('pypy', '')}"
        elif key.lower() in DISPLAY_LOOKUP:
            name = DISPLAY_LOOKUP[key.lower()][value]
        else:
            continue
        display_name = f"{display_name}{sep}{name}"
    return display_name


def get_variant_name(base: str, host: Host | None = None, **kwargs) -> str:
    """Get the display name of a variant."""
    display_name = base
    if host is not None:
        display_name += f" {host.display_name}"
    return get_common_name(display_name, " ", **kwargs)


def get_task_name(base: str, **kwargs):
    return get_common_name(base, "-", **kwargs).replace(" ", "-").lower()


def zip_cycle(*iterables, empty_default=None):
    """Get all combinations of the inputs, cycling over the shorter list(s)."""
    cycles = [cycle(i) for i in iterables]
    for _ in zip_longest(*iterables):
        yield tuple(next(i, empty_default) for i in cycles)


def handle_c_ext(c_ext, expansions) -> None:
    """Handle c extension option."""
    if c_ext == C_EXTS[0]:
        expansions["NO_EXT"] = "1"


def get_assume_role(**kwargs):
    kwargs.setdefault("command_type", EvgCommandType.SETUP)
    kwargs.setdefault("role_arn", "${assume_role_arn}")
    return ec2_assume_role(**kwargs)


def get_subprocess_exec(**kwargs):
    kwargs.setdefault("binary", "bash")
    kwargs.setdefault("working_dir", "src")
    kwargs.setdefault("command_type", EvgCommandType.TEST)
    return subprocess_exec(**kwargs)


def get_s3_put(**kwargs):
    kwargs["aws_key"] = "${AWS_ACCESS_KEY_ID}"
    kwargs["aws_secret"] = "${AWS_SECRET_ACCESS_KEY}"  # noqa:S105
    kwargs["aws_session_token"] = "${AWS_SESSION_TOKEN}"  # noqa:S105
    kwargs["bucket"] = "${bucket_name}"
    kwargs.setdefault("optional", "true")
    kwargs.setdefault("permissions", "public-read")
    kwargs.setdefault("content_type", "${content_type|application/x-gzip}")
    kwargs.setdefault("command_type", EvgCommandType.SETUP)
    return s3_put(**kwargs)


def generate_yaml(tasks=None, variants=None):
    """Generate the yaml for a given set of tasks and variants."""
    project = EvgProject(tasks=tasks, buildvariants=variants)
    out = ShrubService.generate_yaml(project)
    # Dedent by two spaces to match what we use in config.yml
    lines = [line[2:] for line in out.splitlines()]
    print("\n".join(lines))  # noqa: T201


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
    return variants


def create_server_variants() -> list[BuildVariant]:
    variants = []

    # Run the full matrix on linux with min and max CPython, and latest pypy.
    host = DEFAULT_HOST
    # Prefix the display name with an asterisk so it is sorted first.
    base_display_name = "* Test"
    for python, c_ext in product([*MIN_MAX_PYTHON, PYPYS[-1]], C_EXTS):
        expansions = dict(COVERAGE="coverage")
        handle_c_ext(c_ext, expansions)
        display_name = get_variant_name(base_display_name, host, python=python, **expansions)
        variant = create_variant(
            [f".{t} .sync_async" for t in TOPOLOGIES],
            display_name,
            python=python,
            host=host,
            tags=["coverage_tag"],
            expansions=expansions,
        )
        variants.append(variant)

    # Test the rest of the pythons.
    for python in CPYTHONS[1:-1] + PYPYS[:-1]:
        display_name = f"Test {host}"
        display_name = get_variant_name(base_display_name, host, python=python)
        variant = create_variant(
            [f"{t} .sync_async" for t in SUB_TASKS],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    # Test a subset on each of the other platforms.
    for host_name in ("macos", "macos-arm64", "win64", "win32"):
        for python in MIN_MAX_PYTHON:
            tasks = [f"{t} !.sync_async" for t in SUB_TASKS]
            # MacOS arm64 only works on server versions 6.0+
            if host_name == "macos-arm64":
                tasks = []
                for version in get_versions_from("6.0"):
                    tasks.extend(f"{t} .{version} !.sync_async" for t in SUB_TASKS)
            host = HOSTS[host_name]
            display_name = get_variant_name(base_display_name, host, python=python)
            variant = create_variant(tasks, display_name, python=python, host=host)
            variants.append(variant)

    return variants


def create_free_threaded_variants() -> list[BuildVariant]:
    variants = []
    for host_name in ("rhel8", "macos", "macos-arm64", "win64"):
        if host_name == "win64":
            # TODO: PYTHON-5027
            continue
        tasks = [".free-threading"]
        host = HOSTS[host_name]
        python = "3.13t"
        display_name = get_variant_name("Free-threaded", host, python=python)
        variant = create_variant(tasks, display_name, python=python, host=host)
        variants.append(variant)
    return variants


def create_encryption_variants() -> list[BuildVariant]:
    variants = []
    tags = ["encryption_tag"]
    batchtime = BATCHTIME_WEEK

    def get_encryption_expansions(encryption):
        expansions = dict(TEST_NAME="encryption")
        if "crypt_shared" in encryption:
            expansions["TEST_CRYPT_SHARED"] = "true"
        if "PyOpenSSL" in encryption:
            expansions["SUB_TEST_NAME"] = "pyopenssl"
        return expansions

    host = DEFAULT_HOST

    # Test against all server versions for the three main python versions.
    encryptions = ["Encryption", "Encryption crypt_shared"]
    for encryption, python in product(encryptions, [*MIN_MAX_PYTHON, PYPYS[-1]]):
        expansions = get_encryption_expansions(encryption)
        display_name = get_variant_name(encryption, host, python=python, **expansions)
        variant = create_variant(
            [f"{t} .sync_async" for t in SUB_TASKS],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
            tags=tags,
        )
        variants.append(variant)

    # Test PyOpenSSL against on all server versions for all python versions.
    for encryption, python in product(["Encryption PyOpenSSL"], [*MIN_MAX_PYTHON, PYPYS[-1]]):
        expansions = get_encryption_expansions(encryption)
        display_name = get_variant_name(encryption, host, python=python, **expansions)
        variant = create_variant(
            [f"{t} .sync" for t in SUB_TASKS],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
            tags=tags,
        )
        variants.append(variant)

    # Test the rest of the pythons on linux for all server versions.
    for encryption, python, task in zip_cycle(encryptions, CPYTHONS[1:-1] + PYPYS[:-1], SUB_TASKS):
        expansions = get_encryption_expansions(encryption)
        display_name = get_variant_name(encryption, host, python=python, **expansions)
        variant = create_variant(
            [f"{task} .sync_async"],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    # Test on macos and linux on one server version and topology for min and max python.
    encryptions = ["Encryption", "Encryption crypt_shared"]
    task_names = [".latest .replica_set .sync_async"]
    for host_name, encryption, python in product(["macos", "win64"], encryptions, MIN_MAX_PYTHON):
        host = HOSTS[host_name]
        expansions = get_encryption_expansions(encryption)
        display_name = get_variant_name(encryption, host, python=python, **expansions)
        variant = create_variant(
            task_names,
            display_name,
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
            tags=tags,
        )
        variants.append(variant)
    return variants


def create_load_balancer_variants():
    # Load balancer tests - run all supported server versions using the lowest supported python.
    return [
        create_variant(
            [".load-balancer"], "Load Balancer", host=DEFAULT_HOST, batchtime=BATCHTIME_WEEK
        )
    ]


def create_compression_variants():
    # Compression tests - standalone versions of each server, across python versions.
    host = DEFAULT_HOST
    base_task = ".compression"
    variants = []
    for compressor in "snappy", "zlib", "zstd":
        expansions = dict(COMPRESSOR=compressor)
        tasks = [base_task] if compressor != "zstd" else [f"{base_task} !.4.0"]
        display_name = get_variant_name(f"Compression {compressor}", host)
        variants.append(
            create_variant(
                tasks,
                display_name,
                host=host,
                expansions=expansions,
            )
        )
    return variants


def create_enterprise_auth_variants():
    variants = []
    for host in [HOSTS["macos"], HOSTS["win64"], DEFAULT_HOST]:
        display_name = get_variant_name("Auth Enterprise", host)
        if host == DEFAULT_HOST:
            tags = [".enterprise_auth"]
        else:
            tags = [".enterprise_auth !.pypy"]
        variant = create_variant(tags, display_name, host=host)
        variants.append(variant)

    return variants


def create_pyopenssl_variants():
    base_name = "PyOpenSSL"
    batchtime = BATCHTIME_WEEK
    expansions = dict(TEST_NAME="default", SUB_TEST_NAME="pyopenssl")
    variants = []

    for python in ALL_PYTHONS:
        # Only test "noauth" with min python.
        auth = "noauth" if python == CPYTHONS[0] else "auth"
        ssl = "nossl" if auth == "noauth" else "ssl"
        if python == CPYTHONS[0]:
            host = HOSTS["macos"]
        elif python == CPYTHONS[-1]:
            host = HOSTS["win64"]
        else:
            host = DEFAULT_HOST

        display_name = get_variant_name(base_name, host, python=python)
        # only need to run some on async
        if python in (CPYTHONS[1], CPYTHONS[-1]):
            variant = create_variant(
                [f".replica_set .{auth} .{ssl} .sync_async", f".7.0 .{auth} .{ssl} .sync_async"],
                display_name,
                python=python,
                host=host,
                expansions=expansions,
                batchtime=batchtime,
            )
        else:
            variant = create_variant(
                [f".replica_set .{auth} .{ssl} .sync", f".7.0 .{auth} .{ssl} .sync"],
                display_name,
                python=python,
                host=host,
                expansions=expansions,
                batchtime=batchtime,
            )
        variants.append(variant)

    return variants


def create_storage_engine_variants():
    host = DEFAULT_HOST
    engines = ["InMemory", "MMAPv1"]
    variants = []
    for engine in engines:
        python = CPYTHONS[0]
        expansions = dict(STORAGE_ENGINE=engine.lower())
        if engine == engines[0]:
            tasks = [f".standalone .noauth .nossl .{v} .sync_async" for v in ALL_VERSIONS]
        else:
            # MongoDB 4.2 drops support for MMAPv1
            versions = get_versions_until("4.0")
            tasks = [f".standalone .{v} .noauth .nossl .sync_async" for v in versions] + [
                f".replica_set .{v} .noauth .nossl .sync_async" for v in versions
            ]
        display_name = get_variant_name(f"Storage {engine}", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_stable_api_variants():
    host = DEFAULT_HOST
    tags = ["versionedApi_tag"]
    variants = []
    types = ["require v1", "accept v2"]

    # All python versions across platforms.
    for python, test_type in product(MIN_MAX_PYTHON, types):
        expansions = dict(AUTH="auth")
        # Test against a cluster with requireApiVersion=1.
        if test_type == types[0]:
            # REQUIRE_API_VERSION is set to make drivers-evergreen-tools
            # start a cluster with the requireApiVersion parameter.
            expansions["REQUIRE_API_VERSION"] = "1"
            # MONGODB_API_VERSION is the apiVersion to use in the test suite.
            expansions["MONGODB_API_VERSION"] = "1"
            tasks = [
                f"!.replica_set .{v} .noauth .nossl .sync_async" for v in get_versions_from("5.0")
            ]
        else:
            # Test against a cluster with acceptApiVersion2 but without
            # requireApiVersion, and don't automatically add apiVersion to
            # clients created in the test suite.
            expansions["ORCHESTRATION_FILE"] = "versioned-api-testing.json"
            tasks = [
                f".standalone .{v} .noauth .nossl .sync_async" for v in get_versions_from("5.0")
            ]
        base_display_name = f"Stable API {test_type}"
        display_name = get_variant_name(base_display_name, host, python=python, **expansions)
        variant = create_variant(
            tasks, display_name, host=host, python=python, tags=tags, expansions=expansions
        )
        variants.append(variant)

    return variants


def create_green_framework_variants():
    variants = []
    tasks = [".standalone .noauth .nossl .sync_async"]
    host = DEFAULT_HOST
    for python, framework in product([CPYTHONS[0], CPYTHONS[-1]], ["eventlet", "gevent"]):
        expansions = dict(GREEN_FRAMEWORK=framework, AUTH="auth", SSL="ssl")
        display_name = get_variant_name(f"Green {framework.capitalize()}", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_no_c_ext_variants():
    variants = []
    host = DEFAULT_HOST
    for python, topology in zip_cycle(CPYTHONS, TOPOLOGIES):
        tasks = [f".{topology} .noauth .nossl !.sync_async"]
        expansions = dict()
        handle_c_ext(C_EXTS[0], expansions)
        display_name = get_variant_name("No C Ext", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_atlas_data_lake_variants():
    variants = []
    host = HOSTS["ubuntu22"]
    for python in MIN_MAX_PYTHON:
        tasks = [".atlas_data_lake"]
        display_name = get_variant_name("Atlas Data Lake", host, python=python)
        variant = create_variant(tasks, display_name, host=host, python=python)
        variants.append(variant)
    return variants


def create_mod_wsgi_variants():
    variants = []
    host = HOSTS["ubuntu22"]
    tasks = [".mod_wsgi"]
    expansions = dict(MOD_WSGI_VERSION="4")
    for python in MIN_MAX_PYTHON:
        display_name = get_variant_name("mod_wsgi", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_disable_test_commands_variants():
    host = DEFAULT_HOST
    expansions = dict(AUTH="auth", SSL="ssl", DISABLE_TEST_COMMANDS="1")
    python = CPYTHONS[0]
    display_name = get_variant_name("Disable test commands", host, python=python)
    tasks = [".latest .sync_async"]
    return [create_variant(tasks, display_name, host=host, python=python, expansions=expansions)]


def create_serverless_variants():
    host = DEFAULT_HOST
    batchtime = BATCHTIME_WEEK
    tasks = [".serverless"]
    base_name = "Serverless"
    return [
        create_variant(
            tasks,
            get_variant_name(base_name, host, python=python),
            host=host,
            python=python,
            batchtime=batchtime,
        )
        for python in MIN_MAX_PYTHON
    ]


def create_oidc_auth_variants():
    variants = []
    for host_name in ["ubuntu22", "macos", "win64"]:
        if host_name == "ubuntu22":
            tasks = [".auth_oidc"]
        else:
            tasks = [".auth_oidc !.auth_oidc_remote"]
        host = HOSTS[host_name]
        variants.append(
            create_variant(
                tasks,
                get_variant_name("Auth OIDC", host),
                host=host,
                batchtime=BATCHTIME_WEEK,
            )
        )
    return variants


def create_search_index_variants():
    host = DEFAULT_HOST
    python = CPYTHONS[0]
    return [
        create_variant(
            [".search_index"],
            get_variant_name("Search Index Helpers", host, python=python),
            python=python,
            host=host,
        )
    ]


def create_mockupdb_variants():
    host = DEFAULT_HOST
    python = CPYTHONS[0]
    return [
        create_variant(
            [".mockupdb"],
            get_variant_name("MockupDB", host, python=python),
            python=python,
            host=host,
        )
    ]


def create_doctests_variants():
    host = DEFAULT_HOST
    python = CPYTHONS[0]
    return [
        create_variant(
            [".doctests"],
            get_variant_name("Doctests", host, python=python),
            python=python,
            host=host,
        )
    ]


def create_atlas_connect_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".atlas_connect"],
            get_variant_name("Atlas connect", host, python=python),
            python=python,
            host=host,
        )
        for python in MIN_MAX_PYTHON
    ]


def create_coverage_report_variants():
    return [create_variant(["coverage-report"], "Coverage Report", host=DEFAULT_HOST)]


def create_kms_variants():
    tasks = []
    tasks.append(EvgTaskRef(name="test-gcpkms", batchtime=BATCHTIME_WEEK))
    tasks.append("test-gcpkms-fail")
    tasks.append(EvgTaskRef(name="test-azurekms", batchtime=BATCHTIME_WEEK))
    tasks.append("test-azurekms-fail")
    return [create_variant(tasks, "KMS", host=HOSTS["debian11"])]


def create_import_time_variants():
    return [create_variant(["check-import-time"], "Import Time", host=DEFAULT_HOST)]


def create_backport_pr_variants():
    return [create_variant(["backport-pr"], "Backport PR", host=DEFAULT_HOST)]


def create_perf_variants():
    host = HOSTS["perf"]
    return [
        create_variant([".perf"], "Performance Benchmarks", host=host, batchtime=BATCHTIME_WEEK)
    ]


def create_aws_auth_variants():
    variants = []

    for host_name, python in product(["ubuntu20", "win64", "macos"], MIN_MAX_PYTHON):
        expansions = dict()
        tasks = [".auth-aws"]
        if host_name == "macos":
            tasks = [".auth-aws !.auth-aws-web-identity !.auth-aws-ecs !.auth-aws-ec2"]
        elif host_name == "win64":
            tasks = [".auth-aws !.auth-aws-ecs"]
        host = HOSTS[host_name]
        variant = create_variant(
            tasks,
            get_variant_name("Auth AWS", host, python=python),
            host=host,
            python=python,
            expansions=expansions,
        )
        variants.append(variant)
    return variants


def create_no_server_variants():
    host = HOSTS["rhel8"]
    return [create_variant([".no-server"], "No server", host=host)]


def create_alternative_hosts_variants():
    batchtime = BATCHTIME_WEEK
    variants = []

    host = HOSTS["rhel7"]
    variants.append(
        create_variant(
            [".5.0 .standalone !.sync_async"],
            get_variant_name("OpenSSL 1.0.2", host, python=CPYTHONS[0]),
            host=host,
            python=CPYTHONS[0],
            batchtime=batchtime,
        )
    )

    expansions = dict()
    handle_c_ext(C_EXTS[0], expansions)
    for host_name in OTHER_HOSTS:
        host = HOSTS[host_name]
        tags = [".6.0 .standalone !.sync_async"]
        if host_name == "Amazon2023":
            tags = [f".latest !.sync_async {t}" for t in SUB_TASKS]
        variants.append(
            create_variant(
                tags,
                display_name=get_variant_name("Other hosts", host),
                batchtime=batchtime,
                host=host,
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


def create_server_tasks():
    tasks = []
    for topo, version, (auth, ssl), sync in product(TOPOLOGIES, ALL_VERSIONS, AUTH_SSLS, SYNCS):
        name = f"test-{version}-{topo}-{auth}-{ssl}-{sync}".lower()
        tags = [version, topo, auth, ssl, sync]
        server_vars = dict(
            VERSION=version,
            TOPOLOGY=topo if topo != "standalone" else "server",
            AUTH=auth,
            SSL=ssl,
        )
        server_func = FunctionCall(func="run server", vars=server_vars)
        test_vars = dict(AUTH=auth, SSL=ssl, SYNC=sync)
        if sync == "sync":
            test_vars["TEST_NAME"] = "default_sync"
        elif sync == "async":
            test_vars["TEST_NAME"] = "default_async"
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
    return tasks


def create_load_balancer_tasks():
    tasks = []
    for (auth, ssl), version in product(AUTH_SSLS, get_versions_from("6.0")):
        name = get_task_name(f"test-load-balancer-{auth}-{ssl}", version=version)
        tags = ["load-balancer", auth, ssl]
        server_vars = dict(
            TOPOLOGY="sharded_cluster",
            AUTH=auth,
            SSL=ssl,
            TEST_NAME="load_balancer",
            VERSION=version,
        )
        server_func = FunctionCall(func="run server", vars=server_vars)
        test_vars = dict(AUTH=auth, SSL=ssl, TEST_NAME="load_balancer")
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))

    return tasks


def create_compression_tasks():
    tasks = []
    versions = get_versions_from("4.0")
    # Test all server versions with min python.
    for version in versions:
        python = CPYTHONS[0]
        tags = ["compression", version]
        name = get_task_name("test-compression", python=python, version=version)
        server_func = FunctionCall(func="run server", vars=dict(VERSION=version))
        test_func = FunctionCall(func="run tests")
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))

    # Test latest with max python, with and without c exts.
    version = "latest"
    tags = ["compression", "latest"]
    for c_ext in C_EXTS:
        python = CPYTHONS[-1]
        expansions = dict()
        handle_c_ext(c_ext, expansions)
        name = get_task_name("test-compression", python=python, version=version, **expansions)
        server_func = FunctionCall(func="run server", vars=dict(VERSION=version))
        test_func = FunctionCall(func="run tests", vars=expansions)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))

    # Test on latest with pypy.
    python = PYPYS[-1]
    name = get_task_name("test-compression", python=python, version=version)
    server_func = FunctionCall(func="run server", vars=dict(VERSION=version))
    test_func = FunctionCall(func="run tests")
    tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
    return tasks


def create_kms_tasks():
    tasks = []
    for kms_type in ["gcp", "azure"]:
        for success in [True, False]:
            name = f"test-{kms_type}kms"
            sub_test_name = kms_type
            if not success:
                name += "-fail"
                sub_test_name += "-fail"
            commands = []
            if not success:
                commands.append(FunctionCall(func="run server"))
            test_vars = dict(TEST_NAME="kms", SUB_TEST_NAME=sub_test_name)
            test_func = FunctionCall(func="run tests", vars=test_vars)
            commands.append(test_func)
            tasks.append(EvgTask(name=name, commands=commands))
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
        "ecs",
    ]
    for version in get_versions_from("4.4"):
        base_name = f"test-auth-aws-{version}"
        base_tags = ["auth-aws"]
        server_vars = dict(AUTH_AWS="1", VERSION=version)
        server_func = FunctionCall(func="run server", vars=server_vars)
        assume_func = FunctionCall(func="assume ec2 role")
        for test_type in aws_test_types:
            tags = [*base_tags, f"auth-aws-{test_type}"]
            name = f"{base_name}-{test_type}"
            test_vars = dict(TEST_NAME="auth_aws", SUB_TEST_NAME=test_type)
            test_func = FunctionCall(func="run tests", vars=test_vars)
            funcs = [server_func, assume_func, test_func]
            tasks.append(EvgTask(name=name, tags=tags, commands=funcs))

        tags = [*base_tags, "auth-aws-web-identity"]
        name = f"{base_name}-web-identity-session-name"
        test_vars = dict(
            TEST_NAME="auth_aws", SUB_TEST_NAME="web-identity", AWS_ROLE_SESSION_NAME="test"
        )
        test_func = FunctionCall(func="run tests", vars=test_vars)
        funcs = [server_func, assume_func, test_func]
        tasks.append(EvgTask(name=name, tags=tags, commands=funcs))

    return tasks


def create_oidc_tasks():
    tasks = []
    for sub_test in ["default", "azure", "gcp", "eks", "aks", "gke"]:
        vars = dict(TEST_NAME="auth_oidc", SUB_TEST_NAME=sub_test)
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = f"test-auth-oidc-{sub_test}"
        tags = ["auth_oidc"]
        if sub_test != "default":
            tags.append("auth_oidc_remote")
        tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))
    return tasks


def create_mod_wsgi_tasks():
    tasks = []
    for test, topology in product(["standalone", "embedded-mode"], ["standalone", "replica_set"]):
        if test == "standalone":
            task_name = "mod-wsgi-"
        else:
            task_name = "mod-wsgi-embedded-mode-"
        task_name += topology.replace("_", "-")
        server_vars = dict(TOPOLOGY=topology)
        server_func = FunctionCall(func="run server", vars=server_vars)
        vars = dict(TEST_NAME="mod_wsgi", SUB_TEST_NAME=test.split("-")[0])
        test_func = FunctionCall(func="run tests", vars=vars)
        tags = ["mod_wsgi"]
        commands = [server_func, test_func]
        tasks.append(EvgTask(name=task_name, tags=tags, commands=commands))
    return tasks


def _create_ocsp_tasks(algo, variant, server_type, base_task_name):
    tasks = []
    file_name = f"{algo}-basic-tls-ocsp-{variant}.json"

    for version in get_versions_from("4.4"):
        if version == "latest":
            python = MIN_MAX_PYTHON[-1]
        else:
            python = MIN_MAX_PYTHON[0]

        vars = dict(
            ORCHESTRATION_FILE=file_name,
            OCSP_SERVER_TYPE=server_type,
            TEST_NAME="ocsp",
            PYTHON_VERSION=python,
            VERSION=version,
        )
        test_func = FunctionCall(func="run tests", vars=vars)

        tags = ["ocsp", f"ocsp-{algo}", version]
        if "disableStapling" not in variant:
            tags.append("ocsp-staple")

        task_name = get_task_name(
            f"test-ocsp-{algo}-{base_task_name}", python=python, version=version
        )
        tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))
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
    vars = dict(TEST_NAME="search_index")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-search-index-helpers"
    tags = ["search_index"]
    commands = [assume_func, server_func, test_func]
    return [EvgTask(name=task_name, tags=tags, commands=commands)]


def create_atlas_connect_tasks():
    vars = dict(TEST_NAME="atlas_connect")
    assume_func = FunctionCall(func="assume ec2 role")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-atlas-connect"
    tags = ["atlas_connect"]
    return [EvgTask(name=task_name, tags=tags, commands=[assume_func, test_func])]


def create_enterprise_auth_tasks():
    tasks = []
    for python in [*MIN_MAX_PYTHON, PYPYS[-1]]:
        vars = dict(TEST_NAME="enterprise_auth", AUTH="auth", PYTHON_VERSION=python)
        server_func = FunctionCall(func="run server", vars=vars)
        assume_func = FunctionCall(func="assume ec2 role")
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = get_task_name("test-enterprise-auth", python=python)
        tags = ["enterprise_auth"]
        if python in PYPYS:
            tags += ["pypy"]
        tasks.append(
            EvgTask(name=task_name, tags=tags, commands=[server_func, assume_func, test_func])
        )
    return tasks


def create_perf_tasks():
    tasks = []
    for version, ssl, sync in product(["8.0"], ["ssl", "nossl"], ["sync", "async"]):
        vars = dict(VERSION=f"v{version}-perf", SSL=ssl)
        server_func = FunctionCall(func="run server", vars=vars)
        vars = dict(TEST_NAME="perf", SUB_TEST_NAME=sync)
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


def create_atlas_data_lake_tasks():
    tags = ["atlas_data_lake"]
    tasks = []
    for c_ext in C_EXTS:
        vars = dict(TEST_NAME="data_lake")
        handle_c_ext(c_ext, vars)
        test_func = FunctionCall(func="run tests", vars=vars)
        task_name = f"test-atlas-data-lake-{c_ext}"
        tasks.append(EvgTask(name=task_name, tags=tags, commands=[test_func]))
    return tasks


def create_getdata_tasks():
    # Wildcard task. Do you need to find out what tools are available and where?
    # Throw it here, and execute this task on all buildvariants
    cmd = get_subprocess_exec(args=[".evergreen/scripts/run-getdata.sh"])
    return [EvgTask(name="getdata", commands=[cmd])]


def create_coverage_report_tasks():
    tags = ["coverage"]
    task_name = "coverage-report"
    # BUILD-3165: We can't use "*" (all tasks) and specify "variant".
    # Instead list out all coverage tasks using tags.
    # Run the coverage task even if some tasks fail.
    # Run the coverage task even if some tasks are not scheduled in a patch build.
    task_deps = []
    for name in [".standalone", ".replica_set", ".sharded_cluster"]:
        task_deps.append(
            EvgTaskDependency(name=name, variant=".coverage_tag", status="*", patch_optional=True)
        )
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
    cmd = get_subprocess_exec(args=args)
    return [EvgTask(name=name, commands=[cmd], allowed_requesters=["commit"])]


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


def create_mockupdb_tasks():
    test_func = FunctionCall(func="run tests", vars=dict(TEST_NAME="mockupdb"))
    task_name = "test-mockupdb"
    tags = ["mockupdb"]
    return [EvgTask(name=task_name, tags=tags, commands=[test_func])]


def create_doctest_tasks():
    server_func = FunctionCall(func="run server")
    test_func = FunctionCall(func="run just script", vars=dict(JUSTFILE_TARGET="docs-test"))
    task_name = "test-doctests"
    tags = ["doctests"]
    return [EvgTask(name=task_name, tags=tags, commands=[server_func, test_func])]


def create_no_server_tasks():
    test_func = FunctionCall(func="run tests")
    task_name = "test-no-server"
    tags = ["no-server"]
    return [EvgTask(name=task_name, tags=tags, commands=[test_func])]


def create_free_threading_tasks():
    vars = dict(VERSION="8.0", TOPOLOGY="replica_set")
    server_func = FunctionCall(func="run server", vars=vars)
    test_func = FunctionCall(func="run tests")
    task_name = "test-free-threading"
    tags = ["free-threading"]
    return [EvgTask(name=task_name, tags=tags, commands=[server_func, test_func])]


def create_serverless_tasks():
    vars = dict(TEST_NAME="serverless", AUTH="auth", SSL="ssl")
    test_func = FunctionCall(func="run tests", vars=vars)
    tags = ["serverless"]
    task_name = "test-serverless"
    return [EvgTask(name=task_name, tags=tags, commands=[test_func])]


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


##################
# Generate Config
##################


def write_variants_to_file():
    mod = sys.modules[__name__]
    here = Path(__file__).absolute().parent
    target = here.parent / "generated_configs" / "variants.yml"
    if target.exists():
        target.unlink()
    with target.open("w") as fid:
        fid.write("buildvariants:\n")

    for name, func in sorted(getmembers(mod, isfunction)):
        if not name.endswith("_variants"):
            continue
        if not name.startswith("create_"):
            raise ValueError("Variant creators must start with create_")
        title = name.replace("create_", "").replace("_variants", "").replace("_", " ").capitalize()
        project = EvgProject(tasks=None, buildvariants=func())
        out = ShrubService.generate_yaml(project).splitlines()
        with target.open("a") as fid:
            fid.write(f"  # {title} tests\n")
            for line in out[1:]:
                fid.write(f"{line}\n")
            fid.write("\n")

    # Remove extra trailing newline:
    data = target.read_text().splitlines()
    with target.open("w") as fid:
        for line in data[:-1]:
            fid.write(f"{line}\n")


def write_tasks_to_file():
    mod = sys.modules[__name__]
    here = Path(__file__).absolute().parent
    target = here.parent / "generated_configs" / "tasks.yml"
    if target.exists():
        target.unlink()
    with target.open("w") as fid:
        fid.write("tasks:\n")

    for name, func in sorted(getmembers(mod, isfunction)):
        if name.startswith("_") or not name.endswith("_tasks"):
            continue
        if not name.startswith("create_"):
            raise ValueError("Task creators must start with create_")
        title = name.replace("create_", "").replace("_tasks", "").replace("_", " ").capitalize()
        project = EvgProject(tasks=func(), buildvariants=None)
        out = ShrubService.generate_yaml(project).splitlines()
        with target.open("a") as fid:
            fid.write(f"  # {title} tests\n")
            for line in out[1:]:
                fid.write(f"{line}\n")
            fid.write("\n")

    # Remove extra trailing newline:
    data = target.read_text().splitlines()
    with target.open("w") as fid:
        for line in data[:-1]:
            fid.write(f"{line}\n")


def write_functions_to_file():
    mod = sys.modules[__name__]
    here = Path(__file__).absolute().parent
    target = here.parent / "generated_configs" / "functions.yml"
    if target.exists():
        target.unlink()
    with target.open("w") as fid:
        fid.write("functions:\n")

    functions = dict()
    for name, func in sorted(getmembers(mod, isfunction)):
        if name.startswith("_") or not name.endswith("_func"):
            continue
        if not name.startswith("create_"):
            raise ValueError("Function creators must start with create_")
        title = name.replace("create_", "").replace("_func", "").replace("_", " ").capitalize()
        func_name, cmds = func()
        functions = dict()
        functions[func_name] = cmds
        project = EvgProject(functions=functions, tasks=None, buildvariants=None)
        out = ShrubService.generate_yaml(project).splitlines()
        with target.open("a") as fid:
            fid.write(f"  # {title}\n")
            for line in out[1:]:
                fid.write(f"{line}\n")
            fid.write("\n")

    # Remove extra trailing newline:
    data = target.read_text().splitlines()
    with target.open("w") as fid:
        for line in data[:-1]:
            fid.write(f"{line}\n")


write_variants_to_file()
write_tasks_to_file()
write_functions_to_file()

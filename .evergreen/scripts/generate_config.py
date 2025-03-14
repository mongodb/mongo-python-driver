# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "shrub.py>=3.2.0",
#   "pyyaml>=6.0.2"
# ]
# ///

# Note: Run this file with `pipx run`, or `uv run`.
from __future__ import annotations

import sys
from dataclasses import dataclass
from inspect import getmembers, isfunction
from itertools import cycle, product, zip_longest
from pathlib import Path
from typing import Any

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import FunctionCall
from shrub.v3.evg_project import EvgProject
from shrub.v3.evg_task import EvgTask, EvgTaskRef
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
C_EXTS = ["with_ext", "without_ext"]
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
DEFAULT_HOST = HOSTS["rhel8"]

# Other hosts
OTHER_HOSTS = ["RHEL9-FIPS", "RHEL8-zseries", "RHEL8-POWER8", "RHEL8-arm64"]
for name, run_on in zip(
    OTHER_HOSTS, ["rhel92-fips", "rhel8-zseries-small", "rhel8-power-small", "rhel82-arm64-small"]
):
    HOSTS[name] = Host(name, run_on, name, dict())


##############
# Helpers
##############


def create_variant_generic(
    task_names: list[str],
    display_name: str,
    *,
    host: Host | None = None,
    default_run_on="rhel87-small",
    expansions: dict | None = None,
    **kwargs: Any,
) -> BuildVariant:
    """Create a build variant for the given inputs."""
    task_refs = [EvgTaskRef(name=n) for n in task_names]
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
    task_names: list[str],
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
        task_names, display_name, version=version, host=host, expansions=expansions, **kwargs
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


def get_display_name(base: str, host: Host | None = None, **kwargs) -> str:
    """Get the display name of a variant."""
    display_name = base
    if host is not None:
        display_name += f" {host.display_name}"
    version = kwargs.pop("VERSION", None)
    version = version or kwargs.pop("version", None)
    if version:
        if version not in ["rapid", "latest"]:
            version = f"v{version}"
        display_name = f"{display_name} {version}"
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
        display_name = f"{display_name} {name}"
    return display_name


def zip_cycle(*iterables, empty_default=None):
    """Get all combinations of the inputs, cycling over the shorter list(s)."""
    cycles = [cycle(i) for i in iterables]
    for _ in zip_longest(*iterables):
        yield tuple(next(i, empty_default) for i in cycles)


def handle_c_ext(c_ext, expansions):
    """Handle c extension option."""
    if c_ext == C_EXTS[0]:
        expansions["NO_EXT"] = "1"


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
    batchtime = BATCHTIME_WEEK
    expansions = dict(AUTH="noauth", SSL="ssl", TOPOLOGY="server")
    base_display = "OCSP"

    # OCSP tests on default host with all servers v4.4+ and all python versions.
    versions = get_versions_from("4.4")
    for version, python in zip_cycle(versions, ALL_PYTHONS):
        host = DEFAULT_HOST
        variant = create_variant(
            [".ocsp"],
            get_display_name(base_display, host, version=version, python=python),
            python=python,
            version=version,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
        )
        variants.append(variant)

    # OCSP tests on Windows and MacOS.
    # MongoDB servers on these hosts do not staple OCSP responses and only support RSA.
    for host_name, version in product(["win64", "macos"], ["4.4", "8.0"]):
        host = HOSTS[host_name]
        python = CPYTHONS[0] if version == "4.4" else CPYTHONS[-1]
        variant = create_variant(
            [".ocsp-rsa !.ocsp-staple"],
            get_display_name(base_display, host, version=version, python=python),
            python=python,
            version=version,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
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
        display_name = get_display_name(base_display_name, host, python=python, **expansions)
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
        display_name = get_display_name(base_display_name, host, python=python)
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
            display_name = get_display_name(base_display_name, host, python=python)
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
        display_name = get_display_name("Free-threaded", host, python=python)
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
    encryptions = ["Encryption", "Encryption crypt_shared", "Encryption PyOpenSSL"]
    for encryption, python in product(encryptions, [*MIN_MAX_PYTHON, PYPYS[-1]]):
        expansions = get_encryption_expansions(encryption)
        display_name = get_display_name(encryption, host, python=python, **expansions)
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

    # Test the rest of the pythons on linux for all server versions.
    for encryption, python, task in zip_cycle(encryptions, CPYTHONS[1:-1] + PYPYS[:-1], SUB_TASKS):
        expansions = get_encryption_expansions(encryption)
        display_name = get_display_name(encryption, host, python=python, **expansions)
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
        display_name = get_display_name(encryption, host, python=python, **expansions)
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
    host = DEFAULT_HOST
    batchtime = BATCHTIME_WEEK
    versions = get_versions_from("6.0")
    variants = []
    for version in versions:
        python = CPYTHONS[0]
        display_name = get_display_name("Load Balancer", host, python=python, version=version)
        variant = create_variant(
            [".load-balancer"],
            display_name,
            python=python,
            host=host,
            version=version,
            batchtime=batchtime,
        )
        variants.append(variant)
    return variants


def create_compression_variants():
    # Compression tests - standalone versions of each server, across python versions, with and without c extensions.
    # PyPy interpreters are always tested without extensions.
    host = DEFAULT_HOST
    base_task = ".standalone .noauth .nossl .sync_async"
    task_names = dict(snappy=[base_task], zlib=[base_task], zstd=[f"{base_task} !.4.0"])
    variants = []
    for ind, (compressor, c_ext) in enumerate(product(["snappy", "zlib", "zstd"], C_EXTS)):
        expansions = dict(COMPRESSORS=compressor)
        handle_c_ext(c_ext, expansions)
        base_name = f"Compression {compressor}"
        python = CPYTHONS[ind % len(CPYTHONS)]
        display_name = get_display_name(base_name, host, python=python, **expansions)
        variant = create_variant(
            task_names[compressor],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    other_pythons = PYPYS + CPYTHONS[ind:]
    for compressor, python in zip_cycle(["snappy", "zlib", "zstd"], other_pythons):
        expansions = dict(COMPRESSORS=compressor)
        handle_c_ext(c_ext, expansions)
        base_name = f"Compression {compressor}"
        display_name = get_display_name(base_name, host, python=python, **expansions)
        variant = create_variant(
            task_names[compressor],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    return variants


def create_enterprise_auth_variants():
    variants = []

    # All python versions across platforms.
    for python in ALL_PYTHONS:
        if python == CPYTHONS[0]:
            host = HOSTS["macos"]
        elif python == CPYTHONS[-1]:
            host = HOSTS["win64"]
        else:
            host = DEFAULT_HOST
        display_name = get_display_name("Auth Enterprise", host, python=python)
        variant = create_variant([".enterprise_auth"], display_name, host=host, python=python)
        variants.append(variant)

    return variants


def create_pyopenssl_variants():
    base_name = "PyOpenSSL"
    batchtime = BATCHTIME_WEEK
    expansions = dict(TEST_NAME="pyopenssl")
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

        display_name = get_display_name(base_name, host, python=python)
        variant = create_variant(
            [f".replica_set .{auth} .{ssl} .sync_async", f".7.0 .{auth} .{ssl} .sync_async"],
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
        display_name = get_display_name(f"Storage {engine}", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_stable_api_variants():
    host = DEFAULT_HOST
    tags = ["versionedApi_tag"]
    tasks = [f".standalone .{v} .noauth .nossl .sync_async" for v in get_versions_from("5.0")]
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
        else:
            # Test against a cluster with acceptApiVersion2 but without
            # requireApiVersion, and don't automatically add apiVersion to
            # clients created in the test suite.
            expansions["ORCHESTRATION_FILE"] = "versioned-api-testing.json"
        base_display_name = f"Stable API {test_type}"
        display_name = get_display_name(base_display_name, host, python=python, **expansions)
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
        display_name = get_display_name(f"Green {framework.capitalize()}", host, python=python)
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
        display_name = get_display_name("No C Ext", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_atlas_data_lake_variants():
    variants = []
    host = HOSTS["ubuntu22"]
    for python, c_ext in product(MIN_MAX_PYTHON, C_EXTS):
        tasks = ["atlas-data-lake-tests"]
        expansions = dict(AUTH="auth")
        handle_c_ext(c_ext, expansions)
        display_name = get_display_name("Atlas Data Lake", host, python=python, **expansions)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_mod_wsgi_variants():
    variants = []
    host = HOSTS["ubuntu22"]
    tasks = [".mod_wsgi"]
    expansions = dict(MOD_WSGI_VERSION="4")
    for python in MIN_MAX_PYTHON:
        display_name = get_display_name("mod_wsgi", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_disable_test_commands_variants():
    host = DEFAULT_HOST
    expansions = dict(AUTH="auth", SSL="ssl", DISABLE_TEST_COMMANDS="1")
    python = CPYTHONS[0]
    display_name = get_display_name("Disable test commands", host, python=python)
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
            get_display_name(base_name, host, python=python),
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
                get_display_name("Auth OIDC", host),
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
            [".index_management"],
            get_display_name("Search Index Helpers", host, python=python),
            python=python,
            host=host,
        )
    ]


def create_mockupdb_variants():
    host = DEFAULT_HOST
    python = CPYTHONS[0]
    return [
        create_variant(
            ["mockupdb"],
            get_display_name("MockupDB", host, python=python),
            python=python,
            host=host,
        )
    ]


def create_doctests_variants():
    host = DEFAULT_HOST
    python = CPYTHONS[0]
    return [
        create_variant(
            ["doctests"],
            get_display_name("Doctests", host, python=python),
            python=python,
            host=host,
        )
    ]


def create_atlas_connect_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".atlas_connect"],
            get_display_name("Atlas connect", host, python=python),
            python=python,
            host=host,
        )
        for python in MIN_MAX_PYTHON
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
            get_display_name("Auth AWS", host, python=python),
            host=host,
            python=python,
            expansions=expansions,
        )
        variants.append(variant)
    return variants


def create_alternative_hosts_variants():
    batchtime = BATCHTIME_WEEK
    variants = []

    host = HOSTS["rhel7"]
    variants.append(
        create_variant(
            [".5.0 .standalone !.sync_async"],
            get_display_name("OpenSSL 1.0.2", host, python=CPYTHONS[0]),
            host=host,
            python=CPYTHONS[0],
            batchtime=batchtime,
        )
    )

    expansions = dict()
    handle_c_ext(C_EXTS[0], expansions)
    for host_name in OTHER_HOSTS:
        host = HOSTS[host_name]
        variants.append(
            create_variant(
                [".6.0 .standalone !.sync_async"],
                display_name=get_display_name("Other hosts", host),
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
    for auth, ssl in AUTH_SSLS:
        name = f"test-load-balancer-{auth}-{ssl}".lower()
        tags = ["load-balancer", auth, ssl]
        server_vars = dict(
            TOPOLOGY="sharded_cluster", AUTH=auth, SSL=ssl, TEST_NAME="load_balancer"
        )
        server_func = FunctionCall(func="run server", vars=server_vars)
        test_vars = dict(AUTH=auth, SSL=ssl, TEST_NAME="load_balancer")
        test_func = FunctionCall(func="run tests", vars=test_vars)
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


def _create_ocsp_task(algo, variant, server_type, base_task_name):
    file_name = f"{algo}-basic-tls-ocsp-{variant}.json"

    vars = dict(TEST_NAME="ocsp", ORCHESTRATION_FILE=file_name)
    server_func = FunctionCall(func="run server", vars=vars)

    vars = dict(ORCHESTRATION_FILE=file_name, OCSP_SERVER_TYPE=server_type, TEST_NAME="ocsp")
    test_func = FunctionCall(func="run tests", vars=vars)

    tags = ["ocsp", f"ocsp-{algo}"]
    if "disableStapling" not in variant:
        tags.append("ocsp-staple")

    task_name = f"test-ocsp-{algo}-{base_task_name}"
    commands = [server_func, test_func]
    return EvgTask(name=task_name, tags=tags, commands=commands)


def create_aws_lambda_tasks():
    assume_func = FunctionCall(func="assume ec2 role")
    atlas_func = FunctionCall(func="setup atlas")
    vars = dict(TEST_NAME="aws_lambda")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-aws-lambda-deployed"
    tags = ["aws_lambda"]
    commands = [assume_func, atlas_func, test_func]
    return [EvgTask(name=task_name, tags=tags, commands=commands)]


def create_search_index_tasks():
    assume_func = FunctionCall(func="assume ec2 role")
    atlas_func = FunctionCall(func="setup atlas")
    server_func = FunctionCall(func="run server", vars=dict(TEST_NAME="index_management"))
    vars = dict(TEST_NAME="index_management")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-search-index-helpers"
    tags = ["index_managment"]
    commands = [assume_func, atlas_func, server_func, test_func]
    return [EvgTask(name=task_name, tags=tags, commands=commands)]


def create_atlas_connect_tasks():
    vars = dict(TEST_NAME="atlas_connect")
    assume_func = FunctionCall(func="assume ec2 role")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-atlas-connect"
    tags = ["atlas_connect"]
    return [EvgTask(name=task_name, tags=tags, commands=[assume_func, test_func])]


def create_enterprise_auth_tasks():
    vars = dict(TEST_NAME="enterprise_auth", AUTH="auth", FOO="${THIS THING}")
    server_func = FunctionCall(func="run server", vars=vars)
    assume_func = FunctionCall(func="assume ec2 role")
    test_func = FunctionCall(func="run tests", vars=vars)
    task_name = "test-enterprise-auth"
    tags = ["enterprise_auth"]
    return [EvgTask(name=task_name, tags=tags, commands=[server_func, assume_func, test_func])]


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
            task = _create_ocsp_task(algo, variant, server_type, base_task_name)
            tasks.append(task)

    return tasks


def create_serverless_tasks():
    vars = dict(TEST_NAME="serverless", AUTH="auth", SSL="ssl")
    test_func = FunctionCall(func="run tests", vars=vars)
    tags = ["serverless"]
    task_name = "test-serverless"
    return [EvgTask(name=task_name, tags=tags, commands=[test_func])]


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

    for name, func in getmembers(mod, isfunction):
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

    for name, func in getmembers(mod, isfunction):
        if not name.endswith("_tasks"):
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


write_variants_to_file()
write_tasks_to_file()

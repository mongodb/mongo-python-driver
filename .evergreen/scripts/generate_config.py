# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "shrub.py>=3.2.0",
#   "pyyaml>=6.0.2"
# ]
# ///

# Note: Run this file with `hatch run`, `pipx run`, or `uv run`.
from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle, product, zip_longest
from typing import Any

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_project import EvgProject
from shrub.v3.evg_task import EvgTaskRef
from shrub.v3.shrub_service import ShrubService

##############
# Globals
##############

ALL_VERSIONS = ["4.0", "4.4", "5.0", "6.0", "7.0", "8.0", "rapid", "latest"]
CPYTHONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]
PYPYS = ["pypy3.9", "pypy3.10"]
ALL_PYTHONS = CPYTHONS + PYPYS
MIN_MAX_PYTHON = [CPYTHONS[0], CPYTHONS[-1]]
BATCHTIME_WEEK = 10080
AUTH_SSLS = [("auth", "ssl"), ("noauth", "ssl"), ("noauth", "nossl")]
TOPOLOGIES = ["standalone", "replica_set", "sharded_cluster"]
C_EXTS = ["with_ext", "without_ext"]
SYNCS = ["sync", "async"]
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


HOSTS["rhel8"] = Host("rhel8", "rhel87-small", "RHEL8")
HOSTS["win64"] = Host("win64", "windows-64-vsMulti-small", "Win64")
HOSTS["win32"] = Host("win32", "windows-64-vsMulti-small", "Win32")
HOSTS["macos"] = Host("macos", "macos-14", "macOS")
HOSTS["macos-arm64"] = Host("macos-arm64", "macos-14-arm64", "macOS Arm64")


##############
# Helpers
##############


def create_variant(
    task_names: list[str],
    display_name: str,
    *,
    python: str | None = None,
    version: str | None = None,
    host: str | None = None,
    **kwargs: Any,
) -> BuildVariant:
    """Create a build variant for the given inputs."""
    task_refs = [EvgTaskRef(name=n) for n in task_names]
    kwargs.setdefault("expansions", dict())
    expansions = kwargs.pop("expansions", dict()).copy()
    host = host or "rhel8"
    run_on = [HOSTS[host].run_on]
    name = display_name.replace(" ", "-").lower()
    if python:
        expansions["PYTHON_BINARY"] = get_python_binary(python, host)
    if version:
        expansions["VERSION"] = version
    expansions = expansions or None
    return BuildVariant(
        name=name,
        display_name=display_name,
        tasks=task_refs,
        expansions=expansions,
        run_on=run_on,
        **kwargs,
    )


def get_python_binary(python: str, host: str) -> str:
    """Get the appropriate python binary given a python version and host."""
    if host in ["win64", "win32"]:
        if host == "win32":
            base = "C:/python/32"
        else:
            base = "C:/python"
        python = python.replace(".", "")
        return f"{base}/Python{python}/python.exe"

    if host == "rhel8":
        return f"/opt/python/{python}/bin/python3"

    if host in ["macos", "macos-arm64"]:
        return f"/Library/Frameworks/Python.Framework/Versions/{python}/bin/python3"

    raise ValueError(f"no match found for python {python} on {host}")


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


def get_display_name(base: str, host: str, **kwargs) -> str:
    """Get the display name of a variant."""
    display_name = f"{base} {HOSTS[host].display_name}"
    version = kwargs.pop("VERSION", None)
    if version:
        if version not in ["rapid", "latest"]:
            version = f"v{version}"
        display_name = f"{display_name} {version}"
    for key, value in kwargs.items():
        name = value
        if key.lower() == "python":
            if not value.startswith("pypy"):
                name = f"py{value}"
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
    batchtime = BATCHTIME_WEEK * 2
    expansions = dict(AUTH="noauth", SSL="ssl", TOPOLOGY="server")
    base_display = "OCSP test"

    # OCSP tests on rhel8 with all servers v4.4+ and all python versions.
    versions = [v for v in ALL_VERSIONS if v != "4.0"]
    for version, python in zip_cycle(versions, ALL_PYTHONS):
        host = "rhel8"
        variant = create_variant(
            [".ocsp"],
            get_display_name(base_display, host, version, python),
            python=python,
            version=version,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
        )
        variants.append(variant)

    # OCSP tests on Windows and MacOS.
    # MongoDB servers on these hosts do not staple OCSP responses and only support RSA.
    for host, version in product(["win64", "macos"], ["4.4", "8.0"]):
        python = CPYTHONS[0] if version == "4.4" else CPYTHONS[-1]
        variant = create_variant(
            [".ocsp-rsa !.ocsp-staple"],
            get_display_name(base_display, host, version, python),
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
    host = "rhel8"
    for python, (auth, ssl) in product([*MIN_MAX_PYTHON, PYPYS[-1]], AUTH_SSLS):
        display_name = f"Test {host}"
        expansions = dict(AUTH=auth, SSL=ssl, COVERAGE="coverage")
        display_name = get_display_name("Test", host, python=python, **expansions)
        variant = create_variant(
            [f".{t}" for t in TOPOLOGIES],
            display_name,
            python=python,
            host=host,
            tags=["coverage_tag"],
            expansions=expansions,
        )
        variants.append(variant)

    # Test the rest of the pythons on linux.
    for python, (auth, ssl), topology in zip_cycle(
        CPYTHONS[1:-1] + PYPYS[:-1], AUTH_SSLS, TOPOLOGIES
    ):
        display_name = f"Test {host}"
        expansions = dict(AUTH=auth, SSL=ssl)
        display_name = get_display_name("Test", host, python=python, **expansions)
        variant = create_variant(
            [f".{topology}"],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    # Test a subset on each of the other platforms.
    for host in ("macos", "macos-arm64", "win64", "win32"):
        for (python, (auth, ssl), topology), sync in product(
            zip_cycle(MIN_MAX_PYTHON, AUTH_SSLS, TOPOLOGIES), SYNCS
        ):
            test_suite = "default" if sync == "sync" else "default_async"
            tasks = [f".{topology}"]
            # MacOS arm64 only works on server versions 6.0+
            if host == "macos-arm64":
                tasks = [f".{topology} .{version}" for version in get_versions_from("6.0")]
            expansions = dict(AUTH=auth, SSL=ssl, TEST_SUITES=test_suite, SKIP_CSOT_TESTS="true")
            display_name = get_display_name("Test", host, python=python, **expansions)
            variant = create_variant(
                tasks,
                display_name,
                python=python,
                host=host,
                expansions=expansions,
            )
            variants.append(variant)

    return variants


def create_encryption_variants() -> list[BuildVariant]:
    variants = []
    tags = ["encryption_tag"]
    batchtime = BATCHTIME_WEEK

    def get_encryption_expansions(encryption, ssl="ssl"):
        expansions = dict(AUTH="auth", SSL=ssl, test_encryption="true")
        if "crypt_shared" in encryption:
            expansions["test_crypt_shared"] = "true"
        if "PyOpenSSL" in encryption:
            expansions["test_encryption_pyopenssl"] = "true"
        return expansions

    host = "rhel8"

    # Test against all server versions and topolgies for the three main python versions.
    encryptions = ["Encryption", "Encryption crypt_shared", "Encryption PyOpenSSL"]
    for encryption, python in product(encryptions, [*MIN_MAX_PYTHON, PYPYS[-1]]):
        expansions = get_encryption_expansions(encryption)
        display_name = get_display_name(encryption, host, python=python, **expansions)
        variant = create_variant(
            [f".{t}" for t in TOPOLOGIES],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
            tags=tags,
        )
        variants.append(variant)

    # Test the rest of the pythons on linux for all server versions.
    for encryption, python, ssl in zip_cycle(
        encryptions, CPYTHONS[1:-1] + PYPYS[:-1], ["ssl", "nossl"]
    ):
        expansions = get_encryption_expansions(encryption, ssl)
        display_name = get_display_name(encryption, host, python=python, **expansions)
        variant = create_variant(
            [".replica_set"],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    # Test on macos and linux on one server version and topology for min and max python.
    encryptions = ["Encryption", "Encryption crypt_shared"]
    task_names = [".latest .replica_set"]
    for host, encryption, python in product(["macos", "win64"], encryptions, MIN_MAX_PYTHON):
        ssl = "ssl" if python == CPYTHONS[0] else "nossl"
        expansions = get_encryption_expansions(encryption, ssl)
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
    # Load balancer tests - run all supported versions for all combinations of auth and ssl and system python.
    host = "rhel8"
    task_names = ["load-balancer-test"]
    batchtime = BATCHTIME_WEEK
    expansions_base = dict(test_loadbalancer="true")
    versions = get_versions_from("6.0")
    variants = []
    pythons = CPYTHONS + PYPYS
    for ind, (version, (auth, ssl)) in enumerate(product(versions, AUTH_SSLS)):
        expansions = dict(VERSION=version, AUTH=auth, SSL=ssl)
        expansions.update(expansions_base)
        python = pythons[ind % len(pythons)]
        display_name = get_display_name("Load Balancer", host, python=python, **expansions)
        variant = create_variant(
            task_names,
            display_name,
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
        )
        variants.append(variant)
    return variants


def create_compression_variants():
    # Compression tests - standalone versions of each server, across python versions, with and without c extensions.
    # PyPy interpreters are always tested without extensions.
    host = "rhel8"
    task_names = dict(snappy=[".standalone"], zlib=[".standalone"], zstd=[".standalone !.4.0"])
    variants = []
    for ind, (compressor, c_ext) in enumerate(product(["snappy", "zlib", "zstd"], C_EXTS)):
        expansions = dict(COMPRESSORS=compressor)
        handle_c_ext(c_ext, expansions)
        base_name = f"{compressor} compression"
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
        base_name = f"{compressor} compression"
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
    expansions = dict(AUTH="auth")
    variants = []

    # All python versions across platforms.
    for python in ALL_PYTHONS:
        if python == CPYTHONS[0]:
            host = "macos"
        elif python == CPYTHONS[-1]:
            host = "win64"
        else:
            host = "rhel8"
        display_name = get_display_name("Enterprise Auth", host, python=python, **expansions)
        variant = create_variant(
            ["test-enterprise-auth"], display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)

    return variants


def create_pyopenssl_variants():
    base_name = "PyOpenSSL"
    batchtime = BATCHTIME_WEEK
    base_expansions = dict(test_pyopenssl="true", SSL="ssl")
    variants = []

    for python in ALL_PYTHONS:
        # Only test "noauth" with min python.
        auth = "noauth" if python == CPYTHONS[0] else "auth"
        if python == CPYTHONS[0]:
            host = "macos"
        elif python == CPYTHONS[-1]:
            host = "win64"
        else:
            host = "rhel8"
        expansions = dict(AUTH=auth)
        expansions.update(base_expansions)

        display_name = get_display_name(base_name, host, python=python)
        variant = create_variant(
            [".replica_set", ".7.0"],
            display_name,
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
        )
        variants.append(variant)

    return variants


def create_storage_engine_tests():
    host = "rhel8"
    engines = ["InMemory", "MMAPv1"]
    variants = []
    for engine in engines:
        python = CPYTHONS[0]
        expansions = dict(STORAGE_ENGINE=engine.lower())
        if engine == engines[0]:
            tasks = [f".standalone .{v}" for v in ALL_VERSIONS]
        else:
            # MongoDB 4.2 drops support for MMAPv1
            versions = get_versions_until("4.0")
            tasks = [f".standalone .{v}" for v in versions] + [
                f".replica_set .{v}" for v in versions
            ]
        display_name = get_display_name(f"Storage {engine}", host, python=python)
        variant = create_variant(
            tasks, display_name, host=host, python=python, expansions=expansions
        )
        variants.append(variant)
    return variants


def create_versioned_api_tests():
    host = "rhel8"
    tags = ["versionedApi_tag"]
    tasks = [f".standalone .{v}" for v in get_versions_from("5.0")]
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
        base_display_name = f"Versioned API {test_type}"
        display_name = get_display_name(base_display_name, host, python=python, **expansions)
        variant = create_variant(
            tasks, display_name, host=host, python=python, tags=tags, expansions=expansions
        )
        variants.append(variant)

    return variants


##################
# Generate Config
##################

variants = create_versioned_api_tests()
# print(len(variants))
generate_yaml(variants=variants)

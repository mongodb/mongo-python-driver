# Note: See CONTRIBUTING.md for how to update/run this file.
from __future__ import annotations

import sys
from itertools import product

from generate_config_utils import (
    ALL_PYTHONS,
    ALL_VERSIONS,
    BATCHTIME_WEEK,
    C_EXTS,
    CPYTHONS,
    DEFAULT_HOST,
    HOSTS,
    MIN_MAX_PYTHON,
    OTHER_HOSTS,
    PYPYS,
    SYNCS,
    TOPOLOGIES,
    create_variant,
    get_assume_role,
    get_s3_put,
    get_standard_auth_ssl,
    get_subprocess_exec,
    get_task_name,
    get_variant_name,
    get_versions_from,
    get_versions_until,
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
    perf_send,
)
from shrub.v3.evg_task import EvgTask, EvgTaskDependency, EvgTaskRef

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


def create_server_version_variants() -> list[BuildVariant]:
    variants = []
    for version in ALL_VERSIONS:
        display_name = get_variant_name("* MongoDB", version=version)
        variant = create_variant(
            [".server-version"], display_name, host=DEFAULT_HOST, tags=["coverage_tag"]
        )
        variants.append(variant)
    return variants


def create_standard_nonlinux_variants() -> list[BuildVariant]:
    variants = []
    base_display_name = "* Test"

    # Test a subset on each of the other platforms.
    for host_name in ("macos", "macos-arm64", "win64", "win32"):
        tasks = [".test-standard !.pypy"]
        # MacOS arm64 only works on server versions 6.0+
        if host_name == "macos-arm64":
            tasks = [
                f".test-standard !.pypy .server-{version}" for version in get_versions_from("6.0")
            ]
        host = HOSTS[host_name]
        tags = ["standard-non-linux"]
        expansions = dict()
        if host_name == "win32":
            expansions["IS_WIN32"] = "1"
        display_name = get_variant_name(base_display_name, host)
        variant = create_variant(tasks, display_name, host=host, tags=tags, expansions=expansions)
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

    # Test encryption on all hosts.
    for encryption, host in product(
        ["Encryption", "Encryption crypt_shared"], ["rhel8", "macos", "win64"]
    ):
        expansions = get_encryption_expansions(encryption)
        display_name = get_variant_name(encryption, host, **expansions)
        tasks = [".test-non-standard"]
        if host != "rhel8":
            tasks = [".test-non-standard !.pypy"]
        variant = create_variant(
            tasks,
            display_name,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
            tags=tags,
        )
        variants.append(variant)

    # Test PyOpenSSL on linux.
    host = DEFAULT_HOST
    encryption = "Encryption PyOpenSSL"
    expansions = get_encryption_expansions(encryption)
    display_name = get_variant_name(encryption, host, **expansions)
    variant = create_variant(
        [".test-non-standard"],
        display_name,
        host=host,
        expansions=expansions,
        batchtime=batchtime,
        tags=tags,
    )
    variants.append(variant)
    return variants


def create_load_balancer_variants():
    tasks = [
        f".test-non-standard .server-{v} .sharded_cluster-auth-ssl"
        for v in get_versions_from("6.0")
    ]
    expansions = dict(TEST_NAME="load_balancer")
    return [
        create_variant(
            tasks,
            "Load Balancer",
            host=DEFAULT_HOST,
            batchtime=BATCHTIME_WEEK,
            expansions=expansions,
        )
    ]


def create_compression_variants():
    # Compression tests - use the standard linux tests.
    host = DEFAULT_HOST
    variants = []
    for compressor in "snappy", "zlib", "zstd":
        expansions = dict(COMPRESSOR=compressor)
        if compressor == "zstd":
            tasks = [".test-standard !.server-4.0"]
        else:
            tasks = [".test-standard"]
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
    expansions = dict(SUB_TEST_NAME="pyopenssl")
    variants = []

    for host in ["rhel8", "macos", "win64"]:
        display_name = get_variant_name(base_name, host)
        base_task = ".test-standard" if host == "rhel8" else ".test-standard !.pypy"
        # We only need to run a subset on async.
        tasks = [f"{base_task} .sync", f"{base_task} .async .replica_set-noauth-ssl"]
        variants.append(
            create_variant(
                tasks,
                display_name,
                expansions=expansions,
                batchtime=batchtime,
            )
        )

    return variants


def create_storage_engine_variants():
    host = DEFAULT_HOST
    engines = ["InMemory", "MMAPv1"]
    variants = []
    for engine in engines:
        expansions = dict(STORAGE_ENGINE=engine.lower())
        if engine == engines[0]:
            tasks = [".test-standard .standalone-noauth-nossl"]
        else:
            # MongoDB 4.2 drops support for MMAPv1
            versions = get_versions_until("4.0")
            tasks = [f".test-standard !.sharded_cluster-auth-ssl .server-{v}" for v in versions]
        display_name = get_variant_name(f"Storage {engine}", host)
        variant = create_variant(tasks, display_name, host=host, expansions=expansions)
        variants.append(variant)
    return variants


def create_stable_api_variants():
    host = DEFAULT_HOST
    tags = ["versionedApi_tag"]
    variants = []
    types = ["require v1", "accept v2"]

    # All python versions across platforms.
    for test_type in types:
        expansions = dict(AUTH="auth")
        # Test against a cluster with requireApiVersion=1.
        if test_type == types[0]:
            # REQUIRE_API_VERSION is set to make drivers-evergreen-tools
            # start a cluster with the requireApiVersion parameter.
            expansions["REQUIRE_API_VERSION"] = "1"
            # MONGODB_API_VERSION is the apiVersion to use in the test suite.
            expansions["MONGODB_API_VERSION"] = "1"
            tasks = [
                f".test-standard !.replica_set-noauth-ssl .server-{v}"
                for v in get_versions_from("5.0")
            ]
        else:
            # Test against a cluster with acceptApiVersion2 but without
            # requireApiVersion, and don't automatically add apiVersion to
            # clients created in the test suite.
            expansions["ORCHESTRATION_FILE"] = "versioned-api-testing.json"
            tasks = [
                f".test-standard .server-{v} .standalone-noauth-nossl"
                for v in get_versions_from("5.0")
            ]
        base_display_name = f"Stable API {test_type}"
        display_name = get_variant_name(base_display_name, host, **expansions)
        variant = create_variant(tasks, display_name, host=host, tags=tags, expansions=expansions)
        variants.append(variant)

    return variants


def create_green_framework_variants():
    variants = []
    host = DEFAULT_HOST
    for framework in ["eventlet", "gevent"]:
        tasks = [".test-standard .standalone-noauth-nossl"]
        if framework == "eventlet":
            # Eventlet has issues with dnspython > 2.0 and newer versions of CPython
            # https://jira.mongodb.org/browse/PYTHON-5284
            tasks = [".test-standard .standalone-noauth-nossl .python-3.9"]
        expansions = dict(GREEN_FRAMEWORK=framework, AUTH="auth", SSL="ssl")
        display_name = get_variant_name(f"Green {framework.capitalize()}", host)
        variant = create_variant(tasks, display_name, host=host, expansions=expansions)
        variants.append(variant)
    return variants


def create_no_c_ext_variants():
    host = DEFAULT_HOST
    tasks = [".test-standard"]
    expansions = dict()
    handle_c_ext(C_EXTS[0], expansions)
    display_name = get_variant_name("No C Ext", host)
    return [create_variant(tasks, display_name, host=host)]


def create_atlas_data_lake_variants():
    host = HOSTS["ubuntu22"]
    tasks = [".test-no-orchestration"]
    expansions = dict(TEST_NAME="data_lake")
    display_name = get_variant_name("Atlas Data Lake", host)
    return [create_variant(tasks, display_name, host=host, expansions=expansions)]


def create_mod_wsgi_variants():
    host = HOSTS["ubuntu22"]
    tasks = [".mod_wsgi"]
    expansions = dict(MOD_WSGI_VERSION="4")
    display_name = get_variant_name("mod_wsgi", host)
    return [create_variant(tasks, display_name, host=host, expansions=expansions)]


def create_disable_test_commands_variants():
    host = DEFAULT_HOST
    expansions = dict(AUTH="auth", SSL="ssl", DISABLE_TEST_COMMANDS="1")
    python = CPYTHONS[0]
    display_name = get_variant_name("Disable test commands", host, python=python)
    tasks = [".test-standard .server-latest"]
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
    expansions = dict(TEST_NAME="mockupdb")
    return [
        create_variant(
            [".test-no-orchestration"],
            get_variant_name("MockupDB", host),
            host=host,
            expansions=expansions,
        )
    ]


def create_doctests_variants():
    host = DEFAULT_HOST
    expansions = dict(TEST_NAME="doctest")
    return [
        create_variant(
            [".test-non-standard .standalone-noauth-nossl"],
            get_variant_name("Doctests", host),
            host=host,
            expansions=expansions,
        )
    ]


def create_atlas_connect_variants():
    host = DEFAULT_HOST
    return [
        create_variant(
            [".test-no-orchestration"],
            get_variant_name("Atlas connect", host),
            host=DEFAULT_HOST,
        )
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

    for host_name in ["ubuntu20", "win64", "macos"]:
        expansions = dict()
        tasks = [".auth-aws"]
        if host_name == "macos":
            tasks = [".auth-aws !.auth-aws-web-identity !.auth-aws-ecs !.auth-aws-ec2"]
        elif host_name == "win64":
            tasks = [".auth-aws !.auth-aws-ecs"]
        host = HOSTS[host_name]
        variant = create_variant(
            tasks,
            get_variant_name("Auth AWS", host),
            host=host,
            expansions=expansions,
        )
        variants.append(variant)
    return variants


def create_no_server_variants():
    host = HOSTS["rhel8"]
    name = get_variant_name("No server", host=host)
    return [create_variant([".test-no-orchestration"], name, host=host)]


def create_alternative_hosts_variants():
    batchtime = BATCHTIME_WEEK
    variants = []

    host = HOSTS["rhel7"]
    version = "5.0"
    variants.append(
        create_variant(
            [".test-no-toolchain"],
            get_variant_name("OpenSSL 1.0.2", host, python=CPYTHONS[0], version=version),
            host=host,
            python=CPYTHONS[0],
            batchtime=batchtime,
            expansions=dict(VERSION=version, PYTHON_VERSION=CPYTHONS[0]),
        )
    )

    version = "latest"
    for host_name in OTHER_HOSTS:
        expansions = dict(VERSION="latest")
        handle_c_ext(C_EXTS[0], expansions)
        host = HOSTS[host_name]
        if "fips" in host_name.lower():
            expansions["REQUIRE_FIPS"] = "1"
        variants.append(
            create_variant(
                [".test-no-toolchain"],
                display_name=get_variant_name("Other hosts", host, version=version),
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


def create_server_version_tasks():
    tasks = []
    task_inputs = []
    # All combinations of topology, auth, ssl, and sync should be tested.
    for (topology, auth, ssl, sync), python in zip_cycle(
        list(product(TOPOLOGIES, ["auth", "noauth"], ["ssl", "nossl"], SYNCS)), ALL_PYTHONS
    ):
        task_inputs.append((topology, auth, ssl, sync, python))

    # Every python should be tested with sharded cluster, auth, ssl, with sync and async.
    for python, sync in product(ALL_PYTHONS, SYNCS):
        task_input = ("sharded_cluster", "auth", "ssl", sync, python)
        if task_input not in task_inputs:
            task_inputs.append(task_input)

    # Assemble the tasks.
    for topology, auth, ssl, sync, python in task_inputs:
        tags = ["server-version", f"python-{python}", f"{topology}-{auth}-{ssl}", sync]
        expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology)
        if python not in PYPYS:
            expansions["COVERAGE"] = "1"
        name = get_task_name("test-server-version", python=python, sync=sync, **expansions)
        server_func = FunctionCall(func="run server", vars=expansions)
        test_vars = expansions.copy()
        test_vars["PYTHON_VERSION"] = python
        test_vars["TEST_NAME"] = f"default_{sync}"
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
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


def create_test_non_standard_tasks():
    """For variants that set a TEST_NAME."""
    tasks = []
    task_combos = []
    # For each version and topology, rotate through the CPythons.
    for (version, topology), python in zip_cycle(list(product(ALL_VERSIONS, TOPOLOGIES)), CPYTHONS):
        task_combos.append((version, topology, python))
    # For each PyPy and topology, rotate through the the versions.
    for (python, topology), version in zip_cycle(list(product(PYPYS, TOPOLOGIES)), ALL_VERSIONS):
        task_combos.append((version, topology, python))
    for version, topology, python in task_combos:
        auth, ssl = get_standard_auth_ssl(topology)
        tags = [
            "test-non-standard",
            f"server-{version}",
            f"python-{python}",
            f"{topology}-{auth}-{ssl}",
        ]
        if python in PYPYS:
            tags.append("pypy")
        expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology, VERSION=version)
        name = get_task_name("test-non-standard", python=python, **expansions)
        server_func = FunctionCall(func="run server", vars=expansions)
        test_vars = expansions.copy()
        test_vars["PYTHON_VERSION"] = python
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
    return tasks


def create_standard_tasks():
    """For variants that do not set a TEST_NAME."""
    tasks = []
    task_combos = []
    # For each version and topology, rotate through the CPythons and sync/async.
    for (version, topology), python, sync in zip_cycle(
        list(product(ALL_VERSIONS, TOPOLOGIES)), CPYTHONS, SYNCS
    ):
        task_combos.append((version, topology, python, sync))
    # For each PyPy and topology, rotate through the the versions and sync/async.
    for (python, topology), version, sync in zip_cycle(
        list(product(PYPYS, TOPOLOGIES)), ALL_VERSIONS, SYNCS
    ):
        task_combos.append((version, topology, python, sync))

    for version, topology, python, sync in task_combos:
        auth, ssl = get_standard_auth_ssl(topology)
        tags = [
            "test-standard",
            f"server-{version}",
            f"python-{python}",
            f"{topology}-{auth}-{ssl}",
            sync,
        ]
        if python in PYPYS:
            tags.append("pypy")
        expansions = dict(AUTH=auth, SSL=ssl, TOPOLOGY=topology, VERSION=version)
        name = get_task_name("test-standard", python=python, sync=sync, **expansions)
        server_func = FunctionCall(func="run server", vars=expansions)
        test_vars = expansions.copy()
        test_vars["PYTHON_VERSION"] = python
        test_vars["TEST_NAME"] = f"default_{sync}"
        test_func = FunctionCall(func="run tests", vars=test_vars)
        tasks.append(EvgTask(name=name, tags=tags, commands=[server_func, test_func]))
    return tasks


def create_no_orchestration_tasks():
    tasks = []
    for python in [*MIN_MAX_PYTHON, PYPYS[-1]]:
        tags = [
            "test-no-orchestration",
            f"python-{python}",
        ]
        name = get_task_name("test-no-orchestration", python=python)
        assume_func = FunctionCall(func="assume ec2 role")
        test_vars = dict(PYTHON_VERSION=python)
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
    for version, test_type, python in zip_cycle(get_versions_from("4.4"), aws_test_types, CPYTHONS):
        base_name = f"test-auth-aws-{version}"
        base_tags = ["auth-aws"]
        server_vars = dict(AUTH_AWS="1", VERSION=version)
        server_func = FunctionCall(func="run server", vars=server_vars)
        assume_func = FunctionCall(func="assume ec2 role")
        tags = [*base_tags, f"auth-aws-{test_type}"]
        name = get_task_name(f"{base_name}-{test_type}", python=python)
        test_vars = dict(TEST_NAME="auth_aws", SUB_TEST_NAME=test_type, PYTHON_VERSION=python)
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
                PYTHON_VERSION=python,
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
    for (test, topology), python in zip_cycle(
        product(["standalone", "embedded-mode"], ["standalone", "replica_set"]), CPYTHONS
    ):
        if test == "standalone":
            task_name = "mod-wsgi-"
        else:
            task_name = "mod-wsgi-embedded-mode-"
        task_name += topology.replace("_", "-")
        task_name = get_task_name(task_name, python=python)
        server_vars = dict(TOPOLOGY=topology, PYTHON_VERSION=python)
        server_func = FunctionCall(func="run server", vars=server_vars)
        vars = dict(TEST_NAME="mod_wsgi", SUB_TEST_NAME=test.split("-")[0], PYTHON_VERSION=python)
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
        "PYTHON_BINARY",
        "PYTHON_VERSION",
        "STORAGE_ENGINE",
        "REQUIRE_API_VERSION",
        "DRIVERS_TOOLS",
        "TEST_CRYPT_SHARED",
        "AUTH_AWS",
        "LOAD_BALANCER",
        "LOCAL_ATLAS",
        "NO_EXT",
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
        "PYTHON_BINARY",
        "LIBMONGOCRYPT_URL",
        "MONGODB_URI",
        "PYTHON_VERSION",
        "DISABLE_TEST_COMMANDS",
        "GREEN_FRAMEWORK",
        "NO_EXT",
        "COMPRESSORS",
        "MONGODB_API_VERSION",
        "REQUIRE_API_VERSION",
        "DEBUG_LOG",
        "ORCHESTRATION_FILE",
        "OCSP_SERVER_TYPE",
        "VERSION",
        "IS_WIN32",
        "REQUIRE_FIPS",
    ]
    args = [".evergreen/just.sh", "setup-tests", "${TEST_NAME}", "${SUB_TEST_NAME}"]
    setup_cmd = get_subprocess_exec(include_expansions_in_env=includes, args=args)
    test_cmd = get_subprocess_exec(args=[".evergreen/just.sh", "run-tests"])
    return "run tests", [setup_cmd, test_cmd]


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
    cmd = perf_send(file="src/results.json")
    return "send dashboard data", [cmd]


mod = sys.modules[__name__]
write_variants_to_file(mod)
write_tasks_to_file(mod)
write_functions_to_file(mod)

from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import sys
from datetime import datetime
from pathlib import Path
from shutil import which

try:
    import importlib_metadata
except ImportError:
    from importlib import metadata as importlib_metadata


import pytest
from utils import DRIVERS_TOOLS, LOGGER, ROOT, run_command

AUTH = os.environ.get("AUTH", "noauth")
SSL = os.environ.get("SSL", "nossl")
UV_ARGS = os.environ.get("UV_ARGS", "")
TEST_PERF = os.environ.get("TEST_PERF")
GREEN_FRAMEWORK = os.environ.get("GREEN_FRAMEWORK")
TEST_ARGS = os.environ.get("TEST_ARGS", "").split()
TEST_NAME = os.environ.get("TEST_NAME")
SUB_TEST_NAME = os.environ.get("SUB_TEST_NAME")


def list_packages():
    packages = set()
    for distribution in importlib_metadata.distributions():
        if distribution.name:
            packages.add(distribution.name)
    print("Package             Version     URL")
    print("------------------- ----------- ----------------------------------------------------")
    for name in sorted(packages):
        distribution = importlib_metadata.distribution(name)
        url = ""
        if distribution.origin is not None:
            url = distribution.origin.url
        print(f"{name:20s}{distribution.version:12s}{url}")
    print("------------------- ----------- ----------------------------------------------------\n")


def handle_perf(start_time: datetime):
    end_time = datetime.now()
    elapsed_secs = (end_time - start_time).total_seconds()
    with open("results.json") as fid:
        results = json.load(fid)
    LOGGER.info("results.json:\n%s", json.dumps(results, indent=2))

    results = dict(
        status="PASS",
        exit_code=0,
        test_file="BenchMarkTests",
        start=int(start_time.timestamp()),
        end=int(end_time.timestamp()),
        elapsed=elapsed_secs,
    )
    report = dict(failures=0, results=[results])
    LOGGER.info("report.json\n%s", json.dumps(report, indent=2))

    with open("report.json", "w", newline="\n") as fid:
        json.dump(report, fid)


def handle_green_framework() -> None:
    if GREEN_FRAMEWORK == "gevent":
        from gevent import monkey

        monkey.patch_all()

    # Never run async tests with a framework.
    if len(TEST_ARGS) <= 1:
        TEST_ARGS.extend(["-m", "not default_async and default"])
    else:
        for i in range(len(TEST_ARGS) - 1):
            if "-m" in TEST_ARGS[i]:
                TEST_ARGS[i + 1] = f"not default_async and {TEST_ARGS[i + 1]}"

    LOGGER.info(f"Running tests with {GREEN_FRAMEWORK}...")


def handle_c_ext() -> None:
    if platform.python_implementation() != "CPython":
        return
    sys.path.insert(0, str(ROOT / "tools"))
    from fail_if_no_c import main as fail_if_no_c

    fail_if_no_c()


def handle_pymongocrypt() -> None:
    import pymongocrypt

    LOGGER.info(f"pymongocrypt version: {pymongocrypt.__version__})")
    LOGGER.info(f"libmongocrypt version: {pymongocrypt.libmongocrypt_version()})")


def handle_aws_lambda() -> None:
    env = os.environ.copy()
    target_dir = ROOT / "test/lambda"
    env["TEST_LAMBDA_DIRECTORY"] = str(target_dir)
    env.setdefault("AWS_REGION", "us-east-1")
    dirs = ["pymongo", "gridfs", "bson"]
    # Remove the original .so files.
    for dname in dirs:
        so_paths = [f"{f.parent.name}/{f.name}" for f in (ROOT / dname).glob("*.so")]
        for so_path in list(so_paths):
            Path(so_path).unlink()
    # Build the c extensions.
    docker = which("docker") or which("podman")
    if not docker:
        raise ValueError("Could not find docker!")
    image = "quay.io/pypa/manylinux2014_x86_64:latest"
    run_command(
        f'{docker} run --rm -v "{ROOT}:/src" --platform linux/amd64 {image} /src/test/lambda/build_internal.sh'
    )
    for dname in dirs:
        target = ROOT / "test/lambda/mongodb" / dname
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(ROOT / dname, target)
    # Remove the new so files from the ROOT directory.
    for dname in dirs:
        so_paths = [f"{f.parent.name}/{f.name}" for f in (ROOT / dname).glob("*.so")]
        for so_path in list(so_paths):
            Path(so_path).unlink()

    script_name = "run-deployed-lambda-aws-tests.sh"
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/aws_lambda/{script_name}", env=env)


def run() -> None:
    # Add diagnostic for python version.
    print("Running with python", sys.version)

    # List the installed packages.
    list_packages()

    # Handle green framework first so they can patch modules.
    if GREEN_FRAMEWORK:
        handle_green_framework()

    # Ensure C extensions if applicable.
    if not os.environ.get("NO_EXT"):
        handle_c_ext()

    if os.environ.get("PYMONGOCRYPT_LIB"):
        handle_pymongocrypt()

    LOGGER.info(f"Test setup:\n{AUTH=}\n{SSL=}\n{UV_ARGS=}\n{TEST_ARGS=}")

    # Record the start time for a perf test.
    if TEST_PERF:
        start_time = datetime.now()

    # Run mod_wsgi tests using the helper.
    if TEST_NAME == "mod_wsgi":
        from mod_wsgi_tester import test_mod_wsgi

        test_mod_wsgi()
        return

    # Send kms tests to run remotely.
    if TEST_NAME == "kms" and SUB_TEST_NAME in ["azure", "gcp"]:
        from kms_tester import test_kms_send_to_remote

        test_kms_send_to_remote(SUB_TEST_NAME)
        return

    # Handle doctests.
    if TEST_NAME == "doctest":
        from sphinx.cmd.build import main

        result = main("-E -b doctest doc ./doc/_build/doctest".split())
        sys.exit(result)

    # Send ecs tests to run remotely.
    if TEST_NAME == "auth_aws" and SUB_TEST_NAME == "ecs":
        run_command(f"{DRIVERS_TOOLS}/.evergreen/auth_aws/aws_setup.sh ecs")
        return

    # Send OIDC tests to run remotely.
    if (
        TEST_NAME == "auth_oidc"
        and SUB_TEST_NAME != "default"
        and not SUB_TEST_NAME.endswith("-remote")
    ):
        from oidc_tester import test_oidc_send_to_remote

        test_oidc_send_to_remote(SUB_TEST_NAME)
        return

    # Run deployed aws lambda tests.
    if TEST_NAME == "aws_lambda":
        handle_aws_lambda()
        return

    if os.environ.get("DEBUG_LOG"):
        TEST_ARGS.extend(f"-o log_cli_level={logging.DEBUG}".split())

    # Run local tests.
    ret = pytest.main(TEST_ARGS + sys.argv[1:])
    if ret != 0:
        sys.exit(ret)

    # Handle perf test post actions.
    if TEST_PERF:
        handle_perf(start_time)


if __name__ == "__main__":
    run()

from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import sys
from datetime import datetime

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
    if GREEN_FRAMEWORK == "eventlet":
        import eventlet

        # https://github.com/eventlet/eventlet/issues/401
        eventlet.sleep()
        eventlet.monkey_patch()
    elif GREEN_FRAMEWORK == "gevent":
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


def run() -> None:
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

    # Send kms tests to run remotely.
    if TEST_NAME == "kms" and SUB_TEST_NAME in ["azure", "gcp"]:
        from kms_tester import test_kms_send_to_remote

        test_kms_send_to_remote(SUB_TEST_NAME)
        return

    # Senc ecs tests to run remotely.
    if TEST_NAME == "auth_aws" and SUB_TEST_NAME == "ecs":
        run_command(f"{DRIVERS_TOOLS}/.evergreen/auth_aws/aws_setup.sh ecs")
        return

    # Send OIDC tests to run remotely.
    if (
        TEST_NAME == "auth_oidc"
        and SUB_TEST_NAME != "test"
        and not SUB_TEST_NAME.endswith("-remote")
    ):
        from oidc_tester import test_oidc_send_to_remote

        test_oidc_send_to_remote(SUB_TEST_NAME)
        return

    if os.environ.get("DEBUG_LOG"):
        TEST_ARGS.extend(f"-o log_cli_level={logging.DEBUG} -o log_cli=1".split())

    # Run local tests.
    ret = pytest.main(TEST_ARGS + sys.argv[1:])
    if ret != 0:
        sys.exit(ret)

    # Handle perf test post actions.
    if TEST_PERF:
        handle_perf(start_time)

    # Handle coverage post actions.
    if os.environ.get("COVERAGE"):
        shutil.rmtree(".pytest_cache", ignore_errors=True)


if __name__ == "__main__":
    run()

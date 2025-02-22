from __future__ import annotations

import json
import logging
import os
import platform
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pytest

HERE = Path(__file__).absolute().parent
ROOT = HERE.parent.parent
AUTH = os.environ.get("AUTH", "noauth")
SSL = os.environ.get("SSL", "nossl")
UV_ARGS = os.environ.get("UV_ARGS", "")
TEST_PERF = os.environ.get("TEST_PERF")
GREEN_FRAMEWORK = os.environ.get("GREEN_FRAMEWORK")
TEST_ARGS = os.environ.get("TEST_ARGS", "").split()

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")


def handle_perf(start_time: datetime, end_time: datetime):
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


def run() -> None:
    # Handle green frameworks first so they can patch modules.
    if GREEN_FRAMEWORK:
        handle_green_framework()

    # Ensure C extensions if applicable.
    if not os.environ.get("NO_EXT") and platform.python_implementation() == "CPython":
        sys.path.insert(0, str(ROOT / "tools"))
        from fail_if_no_c import main as fail_if_no_c

        fail_if_no_c()

    if os.environ.get("PYMONGOCRYPT_LIB"):
        # Ensure pymongocrypt is working properly.
        import pymongocrypt

        LOGGER.info(f"pymongocrypt version: {pymongocrypt.__version__})")
        LOGGER.info(f"libmongocrypt version: {pymongocrypt.libmongocrypt_version()})")

    LOGGER.info(f"Test setup:\n{AUTH=}\n{SSL=}\n{UV_ARGS=}\n{TEST_ARGS=}")

    # Record the start time for a perf test.
    if TEST_PERF:
        start_time = datetime.now()

    # Run the tests.
    pytest.main(TEST_ARGS)

    # Handle perf test post actions.
    if TEST_PERF:
        end_time = datetime.now()
        handle_perf(start_time, end_time)

    # Handle coverage post actions.
    if os.environ.get("COVERAGE"):
        shutil.rmtree(".pytest_cache", ignore_errors=True)


if __name__ == "__main__":
    run()

from __future__ import annotations

import asyncio
import sys
from test import pytest_conf, setup, teardown

import pytest

from pymongo.logger import _DEBUG_LOG_HANDLER

_IS_SYNC = True


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item):
    if _DEBUG_LOG_HANDLER is not None:
        outcome = yield
        rep = outcome.get_result()
        if rep.failed:
            _DEBUG_LOG_HANDLER.flush()
        else:
            _DEBUG_LOG_HANDLER.clear()
    else:
        yield


@pytest.fixture(scope="session")
def event_loop_policy():
    # The default asyncio loop implementation on Windows
    # has issues with sharing sockets across loops (https://github.com/python/cpython/issues/122240)
    # We explicitly use a different loop implementation here to prevent that issue
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]

    return asyncio.get_event_loop_policy()


@pytest.fixture(scope="package", autouse=True)
def test_setup_and_teardown():
    setup()
    yield
    teardown()


pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

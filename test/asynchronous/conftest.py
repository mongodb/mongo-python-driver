from __future__ import annotations

import asyncio
import sys
from test import pytest_conf
from test.asynchronous import async_setup, async_teardown

import pytest
import pytest_asyncio

_IS_SYNC = False


@pytest.fixture(scope="session")
def event_loop_policy():
    # The default asyncio loop implementation on Windows
    # has issues with sharing sockets across loops (https://github.com/python/cpython/issues/122240)
    # We explicitly use a different loop implementation here to prevent that issue
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]

    return asyncio.get_event_loop_policy()


@pytest_asyncio.fixture(scope="package", autouse=True)
async def test_setup_and_teardown():
    await async_setup()
    yield
    await async_teardown()


pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

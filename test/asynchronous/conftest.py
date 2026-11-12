from __future__ import annotations

import asyncio
import sys

import pytest_asyncio

from test import pytest_conf
from test.asynchronous import async_setup, async_teardown

_IS_SYNC = False

# The default asyncio loop implementation on Windows has issues with sharing
# sockets across loops (https://github.com/python/cpython/issues/122240).
# We explicitly use a different loop implementation here to prevent that issue
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())  # type: ignore[attr-defined]


@pytest_asyncio.fixture(scope="package", autouse=True)
async def test_setup_and_teardown():
    await async_setup()
    yield
    await async_teardown()


pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

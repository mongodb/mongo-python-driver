from __future__ import annotations

from test import pytest_conf
from test.asynchronous import async_setup, async_teardown

import pytest_asyncio

_IS_SYNC = False


@pytest_asyncio.fixture(scope="session", autouse=True)
async def test_setup_and_teardown():
    await async_setup()
    yield
    await async_teardown()


pytest_collection_modifyitems = pytest_conf.pytest_collection_modifyitems

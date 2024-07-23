from __future__ import annotations

from test import setup, teardown

import pytest

_IS_SYNC = True


@pytest.fixture(scope="session", autouse=True)
def test_setup_and_teardown():
    setup()
    yield
    teardown()


def pytest_collection_modifyitems(items, config):
    for item in items:
        if not any(item.iter_markers()):
            item.add_marker("default")

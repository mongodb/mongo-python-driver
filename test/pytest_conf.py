from __future__ import annotations


def pytest_collection_modifyitems(items, config):
    sync_items = []
    async_items = [
        item
        for item in items
        if "asynchronous" in item.fspath.dirname or sync_items.append(item)  # type: ignore[func-returns-value]
    ]
    for item in async_items:
        if not any(item.iter_markers()):
            item.add_marker("default_async")
    for item in sync_items:
        if not any(item.iter_markers()):
            item.add_marker("default")

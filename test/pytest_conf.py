from __future__ import annotations


def pytest_collection_modifyitems(items, config):
    # Markers that should overlap with the default markers.
    overlap_markers = ["async"]

    for item in items:
        if "asynchronous" in item.fspath.dirname:
            default_marker = "default_async"
        else:
            default_marker = "default"
        markers = [m for m in item.iter_markers() if m not in overlap_markers]
        if not markers:
            item.add_marker(default_marker)

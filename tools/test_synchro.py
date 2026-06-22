"""Smoke test for tools/synchro.py."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))


def test_synchro(monkeypatch: pytest.MonkeyPatch) -> None:
    """Synchro processes only the async files passed as arguments.

    collection.py is a valid async source that would be processed when
    synchro runs with no arguments. When mongo_client.py is passed instead,
    collection.py must not reach unasync_directory.
    """
    monkeypatch.setattr("sys.argv", ["synchro.py", "pymongo/asynchronous/mongo_client.py"])
    monkeypatch.delenv("CI", raising=False)

    processed: list[str] = []

    def capture(files: list[str], *args: object, **kwargs: object) -> None:
        processed.extend(files)

    with (
        patch("synchro.unasync_directory", side_effect=capture),
        patch("synchro.process_files"),
        patch("subprocess.run"),
        patch("pathlib.Path.write_text"),
    ):
        from synchro import main

        main()

    assert "./pymongo/asynchronous/mongo_client.py" in processed
    assert "./pymongo/asynchronous/collection.py" not in processed

# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Shared helper methods for pymongo, bson, and gridfs test suites."""
from __future__ import annotations

import asyncio
import threading
from typing import Optional

from bson import SON
from pymongo._asyncio_task import create_task
from pymongo.read_preferences import ReadPreference

_IS_SYNC = True


def repl_set_step_down(client, **kwargs):
    """Run replSetStepDown, first unfreezing a secondary with replSetFreeze."""
    cmd = SON([("replSetStepDown", 1)])
    cmd.update(kwargs)

    # Unfreeze a secondary to ensure a speedy election.
    client.admin.command("replSetFreeze", 0, read_preference=ReadPreference.SECONDARY)
    client.admin.command(cmd)


if _IS_SYNC:
    PARENT = threading.Thread
else:
    PARENT = object


class ConcurrentRunner(PARENT):
    def __init__(self, **kwargs):
        if _IS_SYNC:
            super().__init__(**kwargs)
        self.name = kwargs.get("name", "ConcurrentRunner")
        self.stopped = False
        self.task = None
        self.target = kwargs.get("target", None)
        self.args = kwargs.get("args", [])

    if not _IS_SYNC:

        def start(self):
            self.task = create_task(self.run(), name=self.name)

        def join(self, timeout: Optional[float] = None):  # type: ignore[override]
            if self.task is not None:
                asyncio.wait([self.task], timeout=timeout)

        def is_alive(self):
            return not self.stopped

    def run(self):
        try:
            self.target(*self.args)
        finally:
            self.stopped = True


class ExceptionCatchingTask(ConcurrentRunner):
    """A Task that stores any exception encountered while running."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.exc = None

    def run(self):
        try:
            super().run()
        except BaseException as exc:
            self.exc = exc
            raise

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
import traceback
from functools import wraps
from typing import Optional, no_type_check

from bson import SON
from pymongo import common
from pymongo._asyncio_task import create_task
from pymongo.read_preferences import ReadPreference

_IS_SYNC = False


async def async_repl_set_step_down(client, **kwargs):
    """Run replSetStepDown, first unfreezing a secondary with replSetFreeze."""
    cmd = SON([("replSetStepDown", 1)])
    cmd.update(kwargs)

    # Unfreeze a secondary to ensure a speedy election.
    await client.admin.command("replSetFreeze", 0, read_preference=ReadPreference.SECONDARY)
    await client.admin.command(cmd)


class client_knobs:
    def __init__(
        self,
        heartbeat_frequency=None,
        min_heartbeat_interval=None,
        kill_cursor_frequency=None,
        events_queue_frequency=None,
    ):
        self.heartbeat_frequency = heartbeat_frequency
        self.min_heartbeat_interval = min_heartbeat_interval
        self.kill_cursor_frequency = kill_cursor_frequency
        self.events_queue_frequency = events_queue_frequency

        self.old_heartbeat_frequency = None
        self.old_min_heartbeat_interval = None
        self.old_kill_cursor_frequency = None
        self.old_events_queue_frequency = None
        self._enabled = False
        self._stack = None

    def enable(self):
        self.old_heartbeat_frequency = common.HEARTBEAT_FREQUENCY
        self.old_min_heartbeat_interval = common.MIN_HEARTBEAT_INTERVAL
        self.old_kill_cursor_frequency = common.KILL_CURSOR_FREQUENCY
        self.old_events_queue_frequency = common.EVENTS_QUEUE_FREQUENCY

        if self.heartbeat_frequency is not None:
            common.HEARTBEAT_FREQUENCY = self.heartbeat_frequency

        if self.min_heartbeat_interval is not None:
            common.MIN_HEARTBEAT_INTERVAL = self.min_heartbeat_interval

        if self.kill_cursor_frequency is not None:
            common.KILL_CURSOR_FREQUENCY = self.kill_cursor_frequency

        if self.events_queue_frequency is not None:
            common.EVENTS_QUEUE_FREQUENCY = self.events_queue_frequency
        self._enabled = True
        # Store the allocation traceback to catch non-disabled client_knobs.
        self._stack = "".join(traceback.format_stack())

    def __enter__(self):
        self.enable()

    @no_type_check
    def disable(self):
        common.HEARTBEAT_FREQUENCY = self.old_heartbeat_frequency
        common.MIN_HEARTBEAT_INTERVAL = self.old_min_heartbeat_interval
        common.KILL_CURSOR_FREQUENCY = self.old_kill_cursor_frequency
        common.EVENTS_QUEUE_FREQUENCY = self.old_events_queue_frequency
        self._enabled = False

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disable()

    def __call__(self, func):
        def make_wrapper(f):
            @wraps(f)
            async def wrap(*args, **kwargs):
                with self:
                    return await f(*args, **kwargs)

            return wrap

        return make_wrapper(func)

    def __del__(self):
        if self._enabled:
            msg = (
                "ERROR: client_knobs still enabled! HEARTBEAT_FREQUENCY={}, "
                "MIN_HEARTBEAT_INTERVAL={}, KILL_CURSOR_FREQUENCY={}, "
                "EVENTS_QUEUE_FREQUENCY={}, stack:\n{}".format(
                    common.HEARTBEAT_FREQUENCY,
                    common.MIN_HEARTBEAT_INTERVAL,
                    common.KILL_CURSOR_FREQUENCY,
                    common.EVENTS_QUEUE_FREQUENCY,
                    self._stack,
                )
            )
            self.disable()
            raise Exception(msg)


# Global knobs to speed up the test suite.
global_knobs = client_knobs(events_queue_frequency=0.05)


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

        async def start(self):
            self.task = create_task(self.run(), name=self.name)

        async def join(self, timeout: Optional[float] = None):  # type: ignore[override]
            if self.task is not None:
                await asyncio.wait([self.task], timeout=timeout)

        def is_alive(self):
            return not self.stopped

    async def run(self):
        try:
            await self.target(*self.args)
        finally:
            self.stopped = True


class ExceptionCatchingTask(ConcurrentRunner):
    """A Task that stores any exception encountered while running."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.exc = None

    async def run(self):
        try:
            await super().run()
        except BaseException as exc:
            self.exc = exc
            raise

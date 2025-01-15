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

"""A separate ThreadPoolExecutor instance used internally to avoid competing for resources with the default asyncio ThreadPoolExecutor
that user code will use."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

_PYMONGO_EXECUTOR = ThreadPoolExecutor(thread_name_prefix="PYMONGO_EXECUTOR-")

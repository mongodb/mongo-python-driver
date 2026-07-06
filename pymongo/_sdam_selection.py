# Copyright 2026-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sans-I/O server-selection tie-breaking.

Pure implementation of the SDAM "power of two random choices" load balancing
used to pick among equally-suitable servers. It is generic over the server type
(via a ``load`` callable), performs no I/O, and is shared by the synchronous and
asynchronous topologies.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from typing import Callable, TypeVar

_T = TypeVar("_T")


def select_least_loaded(
    candidates: Sequence[_T],
    load: Callable[[_T], int],
    *,
    sample: Callable[[Sequence[_T], int], Sequence[_T]] = random.sample,
) -> _T:
    """Choose a server from ``candidates`` using power-of-two random choices.

    Per the SDAM spec, when more than one server is suitable we sample two at
    random and pick the one with the lower in-progress operation count, which
    spreads load without a full scan.

    :param candidates: The suitable servers; must be non-empty.
    :param load: Returns a candidate's current operation count.
    :param sample: Injectable sampler (defaults to :func:`random.sample`),
        used to make selection deterministic in tests.
    """
    if len(candidates) == 1:
        return candidates[0]
    first, second = sample(candidates, 2)
    return first if load(first) <= load(second) else second

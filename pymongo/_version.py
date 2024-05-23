# Copyright 2022-present MongoDB, Inc.
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

"""Current version of PyMongo."""
from __future__ import annotations

import re
from typing import List, Tuple, Union

__version__ = "4.8.0.dev1"

pattern = r"(?P<major>\d+).(?P<minor>\d+).(?P<patch>\d+)(?P<rest>.*)"
match = re.match(pattern, __version__)
if match:
    parts: List[Union[int, str]] = [int(match[part]) for part in ["major", "minor", "patch"]]
    if match["rest"]:
        parts.append(match["rest"])
else:
    parts = []
version_tuple: Tuple[Union[int, str], ...] = tuple(parts)
version = __version__


def get_version_string() -> str:
    return __version__

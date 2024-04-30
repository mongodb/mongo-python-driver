# Copyright 2024-Present MongoDB, Inc.
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
from __future__ import annotations

import sys

base_sha = sys.argv[-1]
head_sha = sys.argv[-2]


def get_total_time(sha: str) -> int:
    with open(f"pymongo-{sha}.log") as fid:
        last_line = fid.readlines()[-1]
    return int(last_line.split()[4])


base_time = get_total_time(base_sha)
curr_time = get_total_time(head_sha)

# Check if we got 20% or more slower.
change = int((curr_time - base_time) / base_time * 100)
if change > 20:
    print(f"PyMongo import got {change} percent worse")
    sys.exit(1)

print(f"Import time changed by {change} percent")

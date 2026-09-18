from __future__ import annotations

import json

from generate_config_utils import ALL_VERSIONS, CPYTHONS

# MongoDB 8.0 with the oldest supported CPython and minimum dependencies, and
# the latest MongoDB with the newest supported CPython. ubuntu-24.04 runners
# cannot install MongoDB older than 8.0, so no earlier version can be tested.
VERSIONS = [
    {
        "python-version": CPYTHONS[0],
        "mongodb-version": "8.0",
    },
    {
        "python-version": CPYTHONS[-1],
        "mongodb-version": ALL_VERSIONS[-1],
    },
]

if __name__ == "__main__":
    print(json.dumps(VERSIONS))

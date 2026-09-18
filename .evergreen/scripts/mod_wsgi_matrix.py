from __future__ import annotations

import json

from generate_config_utils import ALL_VERSIONS, CPYTHONS

# MongoDB 6.0 with the oldest supported CPython and minimum dependencies, and
# the latest MongoDB with the newest supported CPython. The jobs run on
# ubuntu-22.04 runners, where MongoDB 6.0 is the oldest version that installs.
VERSIONS = [
    {
        "python-version": CPYTHONS[0],
        "mongodb-version": "6.0",
    },
    {
        "python-version": CPYTHONS[-1],
        "mongodb-version": ALL_VERSIONS[-1],
    },
]

if __name__ == "__main__":
    print(json.dumps(VERSIONS))

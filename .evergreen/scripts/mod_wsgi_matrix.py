from __future__ import annotations

import json

from generate_config_utils import ALL_VERSIONS, CPYTHONS

# The lowest supported MongoDB with the oldest supported CPython and minimum
# dependencies, and the latest MongoDB with the newest supported CPython.
VERSIONS = [
    {
        "python-version": CPYTHONS[0],
        "mongodb-version": ALL_VERSIONS[0],
    },
    {
        "python-version": CPYTHONS[-1],
        "mongodb-version": ALL_VERSIONS[-1],
    },
]

if __name__ == "__main__":
    print(json.dumps(VERSIONS))

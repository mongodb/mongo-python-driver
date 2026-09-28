from __future__ import annotations

import json
import sys

from generate_config_utils import ALL_VERSIONS, CPYTHONS

# MongoDB 6.0 with the oldest supported CPython and minimum dependencies, and
# the latest MongoDB with the newest supported CPython. MongoDB 6.0 is the
# oldest version that installs on the GitHub runners.
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
    if len(sys.argv) == 3 and sys.argv[1] == "--index":
        # Emit one version pair in GITHUB_OUTPUT format.
        entry = VERSIONS[int(sys.argv[2])]
        print(f"python-version={entry['python-version']}")
        print(f"mongodb-version={entry['mongodb-version']}")
    else:
        print(json.dumps(VERSIONS))

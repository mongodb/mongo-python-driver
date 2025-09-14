# /// script
# dependencies = [
#   "uvloop>=0.18"
# ]
# requires-python = ">=3.11"
# ///
from __future__ import annotations

import sys
from pathlib import Path

import uvloop

# Use pymongo from parent directory.
root = Path(__file__).parent.parent
sys.path.insert(0, str(root))

from pymongo import AsyncMongoClient  # noqa: E402


async def main():
    client = AsyncMongoClient()
    print(await client.admin.command("ping"))


uvloop.run(main())

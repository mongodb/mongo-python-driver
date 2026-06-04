"""
POC: Atlas Stream Processing — create / start / stop / drop a stream processor.

Fill in the FILL_ME values below before running:
    python3 poc_asp.py

Pipeline used:
    $source  → sample_stream_solar
    $emit    → __testLog
"""
from __future__ import annotations

import asyncio
import pprint

from pymongo import AsyncStreamProcessingClient
from pymongo.errors import OperationFailure

# ---------------------------------------------------------------------------
# Configuration — fill these in before running
# ---------------------------------------------------------------------------

# Workspace connection string from Atlas UI:
#   Stream Processing → your workspace → Connect
# Format: mongodb://<host>/   (no mongodb+srv://)
WORKSPACE_URI = "mongodb://atlas-stream-69ed590869155100cecc8b33-lulzki.virginia-usa.a.query.mongodb-dev.net/"  # e.g. "mongodb://atlas-stream-<id>.<region>.a.query.mongodb.net/"

# Atlas DB user credentials (must have atlasAdmin role on the workspace project)
USERNAME = "streams"
PASSWORD = "letsdostreaming123"

# ---------------------------------------------------------------------------
# Pipeline — hardcoded per your setup
# ---------------------------------------------------------------------------

PROCESSOR_NAME = "simpletestSP"

PIPELINE = [
    {
        "$source": {
            "connectionName": "sample_stream_solar",
        }
    },
    {
        "$emit": {
            "connectionName": "__testLog",
        }
    },
]

# ---------------------------------------------------------------------------
# POC steps
# ---------------------------------------------------------------------------

async def main() -> None:
    if "FILL_ME" in (WORKSPACE_URI, USERNAME, PASSWORD):
        raise SystemExit("Fill in WORKSPACE_URI, USERNAME, and PASSWORD at the top of this file.")

    async with AsyncStreamProcessingClient(WORKSPACE_URI, username=USERNAME, password=PASSWORD) as client:
        sps = client.stream_processors()

        # ------------------------------------------------------------------
        # 1. Create
        # ------------------------------------------------------------------
        print(f"\n[1] Creating processor '{PROCESSOR_NAME}' ...")
        try:
            await sps.create(PROCESSOR_NAME, pipeline=PIPELINE)
            print("    Created OK")
        except OperationFailure as e:
            raise SystemExit(f"    Create failed (code {e.code}): {e}") from e

        # ------------------------------------------------------------------
        # 2. Inspect before starting
        # ------------------------------------------------------------------
        print("\n[2] Getting info ...")
        info = await sps.get_info(PROCESSOR_NAME)
        print(f"    state            : {info.state}")
        print(f"    pipeline_version : {info.pipeline_version}")
        print(f"    has_started      : {info.has_started}")

        # ------------------------------------------------------------------
        # 3. Start
        # ------------------------------------------------------------------
        proc = sps.get(PROCESSOR_NAME)
        print("\n[3] Starting processor ...")
        try:
            await proc.start()
            print("    Start command sent OK")
        except OperationFailure as e:
            raise SystemExit(f"    Start failed (code {e.code}): {e}") from e

        # Give the server a moment to transition state
        await asyncio.sleep(2)

        info = await sps.get_info(PROCESSOR_NAME)
        print(f"    state after start: {info.state}")

        # ------------------------------------------------------------------
        # 4. Stats
        # ------------------------------------------------------------------
        print("\n[4] Fetching stats ...")
        try:
            raw_stats = await proc.stats()
            pprint.pprint(raw_stats)
        except OperationFailure as e:
            print(f"    Stats unavailable (code {e.code}): {e}")

        # ------------------------------------------------------------------
        # 5. Sample (up to 5 docs)
        # Note: breaking manually after N docs because the dev server does not
        # signal cursor exhaustion with cursorId=0 as the spec requires.
        # ------------------------------------------------------------------
        print("\n[5] Sampling up to 5 documents ...")
        try:
            count = 0
            async for doc in proc.sample():
                print(f"    doc: {doc}")
                count += 1
                if count >= 5:
                    break
            print(f"    Sampled {count} document(s)")
        except OperationFailure as e:
            print(f"    Sample unavailable (code {e.code}): {e}")

        # ------------------------------------------------------------------
        # 6. Stop
        # ------------------------------------------------------------------
        print("\n[6] Stopping processor ...")
        try:
            await proc.stop()
            print("    Stop command sent OK")
        except OperationFailure as e:
            raise SystemExit(f"    Stop failed (code {e.code}): {e}") from e

        await asyncio.sleep(1)
        info = await sps.get_info(PROCESSOR_NAME)
        print(f"    state after stop : {info.state}")

        # ------------------------------------------------------------------
        # 7. Drop (permanent — comment out to keep the processor alive)
        # ------------------------------------------------------------------
        print("\n[7] Dropping processor ...")
        try:
            await proc.drop()
            print("    Dropped OK")
        except OperationFailure as e:
            raise SystemExit(f"    Drop failed (code {e.code}): {e}") from e

        print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())

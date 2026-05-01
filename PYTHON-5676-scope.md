# PYTHON-5676: Consolidate Command Execution Logic — Scope Document

## Context

PyMongo has accumulated several distinct code paths for executing commands against the server. When cross-cutting logic (e.g., backpressure, CSOT, monitoring) needs to be added or changed, engineers must find and update multiple paths. The goal is a single central path for all database operations, making future changes local to one place.

PYTHON-1357 (Refactor Cursor and CommandCursor) is now Closed — the prerequisite work is done.

---

## Current Command Execution Paths

All paths are duplicated across `pymongo/asynchronous/` (source of truth) and `pymongo/synchronous/` (auto-generated via `tools/synchro.sh`).

### Path 1 — Standard commands (most operations)
```
Collection._command() / Database.command() / conn.command() (direct)
  → Connection.command()      [pool.py:344]  — session, write concern, server API, reauth
    → network.command()       [network.py:61] — encode OP_MSG, APM, logging, CSOT, encryption, send/recv
```
Used by: insert, update, delete, aggregate, distinct, find_one, createIndex, dropCollection, db.command(), etc.

### Path 2 — Cursor operations (find / getMore)
```
Cursor._send_message()
  → MongoClient._run_operation()  [mongo_client.py:1911]
    → Server.run_operation()      [server.py:138]  — send/recv + FULL APM/logging (≈80 lines, near-identical to network.command())
```
**Bypasses `network.command()` entirely.** Has its own APM/monitoring and logging code.

### Path 3 — Acknowledged bulk writes (collection-level)
```
_Bulk._execute_batch()
  → _Bulk.write_command()     [bulk.py:244]  — APM/logging wrapper (≈80 lines)
    → Connection.write_command()  [pool.py:480] — pre-encoded bytes, raw send+recv, no APM
```
**Bypasses both `Connection.command()` and `network.command()`.** Note: for *encrypted* bulk writes, this path already uses `Connection.command()` — the non-encrypted path is the outlier.

### Path 4 — Unacknowledged bulk writes
```
_Bulk._execute_batch_unack()
  → _Bulk.unack_write()       [bulk.py:329]  — APM/logging wrapper (≈70 lines)
    → Connection.unack_write()    [pool.py:469] — raw send only, no recv
```
**Bypasses `Connection.command()` and `network.command()`.**

### Path 5 — Client-level bulk writes (cross-collection)
`client_bulk.py` mirrors Paths 3 and 4 with its own `write_command()` and `unack_write()` wrappers — another full copy of the APM/logging boilerplate.

---

## Key Findings

**1. APM/logging code is copy-pasted 5+ times.**
`Server.run_operation()`, `_Bulk.write_command()`, `_Bulk.unack_write()`, `_ClientBulk.write_command()`, `_ClientBulk.unack_write()` all contain the same ~70-80 line block of `_COMMAND_LOGGER.isEnabledFor(DEBUG)` + `_debug_log(...)` + `listeners.publish_command_start/success/failure(...)`. The reference implementation is `network.command()`.

**2. Dead legacy OP_QUERY code in `Server.run_operation()`.**
`MIN_SUPPORTED_WIRE_VERSION = 8` (MongoDB 4.2+, `common.py`). OP_MSG was introduced at wire version 6. The `use_cmd = False` branch (legacy OP_QUERY path) in `run_operation()` can never be reached for any currently supported server. The `_Query.use_command()` check at `message.py:1622` confirms: for wire version ≥ 8, it is unconditionally True.

**3. `_op_msg()` already handles bulk write Type 1 sections.**
`message._op_msg()` (line 394) detects insert/update/delete commands, pops the documents field, and encodes them as a Type 1 section. The non-encrypted bulk write path bypasses this and does its own single-pass encode+batch-size-check via `_do_batched_op_msg`. Unifying would require separating batch determination from encoding (currently combined for performance).

**4. Encrypted bulk writes already use `Connection.command()`.**
In `_Bulk._execute_batch()` (bulk.py:444), the `if self.is_encrypted:` branch calls `bwc.conn.command()`. The non-encrypted `else` branch goes through `Connection.write_command()`. These should be the same path.

**5. Async is generated from async source.**
All `pymongo/synchronous/*.py` files are auto-generated from `pymongo/asynchronous/*.py` via `tools/synchro.sh`. All code changes must be made in the `asynchronous/` files only.

---

## Proposed Consolidation: Phased Approach

### Phase 1 — Remove dead OP_QUERY code from `run_operation()` *(Low risk)*

**What:** Delete the `use_cmd = False` branches from `Server.run_operation()`. Remove the conditional `if use_cmd: ... else: ...` blocks for `user_fields`/`legacy_response` and response-building.

**Files:** `pymongo/asynchronous/server.py`, `pymongo/asynchronous/cursor.py` (remove the dead `else` branches referencing `_OpReply`)

**Why now:** This is purely dead code removal — no behavior change. It simplifies Phase 3 significantly and is independently safe.

**Verification:** Full test suite; specifically `test/test_cursor.py` and `test/test_unified_format.py`.

---

### Phase 2 — Extract shared APM/logging helpers *(Low risk)*

**What:** Extract the duplicated APM/logging block into shared helper functions in `pymongo/helpers_shared.py` (or a new `pymongo/command_helpers.py`). All five duplicate sites call the helpers instead of inlining the code.

```python
# No sync/async split needed — these functions contain no I/O
def _log_and_publish_command_started(client, listeners, cmd, dbname, request_id, conn):
    ...


def _log_and_publish_command_succeeded(
    client, listeners, cmd, dbname, request_id, conn, reply, duration
):
    ...


def _log_and_publish_command_failed(
    client, listeners, cmd, dbname, request_id, conn, failure, duration
):
    ...
```

**Files:** `pymongo/helpers_shared.py` (or new `pymongo/command_helpers.py`), `pymongo/asynchronous/network.py`, `pymongo/asynchronous/server.py`, `pymongo/asynchronous/bulk.py`, `pymongo/asynchronous/client_bulk.py`

**Why this matters:** This is the direct solution to the stated problem. When future cross-cutting logic needs to be added to "command execution", there is one place to add it.

**Note on APM operationId:** `_BulkWriteContext` passes `op_id` (not `request_id`) as the `operationId` in APM events. This is spec-required behavior that must be preserved in the helpers.

**Verification:** `test/test_command_monitoring.py`, `test/test_monitoring.py`, `test/test_unified_format.py`.

---

### Phase 3 — Unify `Server.run_operation()` with `network.command()` *(Medium risk)*

**What:** Refactor `network.command()` into a two-layer structure:

- `_network_command_core()` — performs all work (encode, APM, send, recv) and returns `(response_doc, raw_reply, request_id, duration)`
- `command()` — thin wrapper returning just `response_doc` (existing callers unchanged)

`Server.run_operation()` (after Phase 1+2) calls `_network_command_core()` for the actual transport, then wraps the result in `Response`/`PinnedResponse`.

**Key complexity:** `RawBatchCursor._unpack_response()` (`cursor.py:1196`) calls `response.raw_response(cursor_id)` on the raw `_OpMsg` object — it needs the raw reply, not just the decoded dict. The `_network_command_core()` design satisfies this by exposing `raw_reply`. The `@_handle_reauth` decorator on `run_operation()` must also be preserved.

**Files:** `pymongo/asynchronous/network.py`, `pymongo/asynchronous/server.py`

**Result:** Cursor operations (find/getMore) fully share the command execution path with all other operations. Single place to add transport-level logic.

**Verification:** `test/test_cursor.py`, exhaust cursor tests, `test/test_encryption.py`, `test/test_csot.py`.

---

### Phase 4 — Unify non-encrypted bulk write path with `Connection.command()` *(Higher risk, defer)*

**What:** Change `_Bulk._execute_batch()` to use `Connection.command()` for non-encrypted bulk writes, matching the already-unified encrypted path. Requires `_BulkWriteContext.batch_command()` to return a command dict instead of pre-encoded bytes.

**The challenge:** `_do_batched_op_msg` performs batch-size determination and encoding in a single pass (including a C extension: `_cmessage._batched_op_msg`). Separating batch determination from encoding requires a two-pass approach, adding encoding overhead. **Needs benchmarking before committing.**

`_EncryptedBulkWriteContext.batch_command()` encodes to bytes and then deserializes back via `_inflate_bson` — aligning the non-encrypted path with this approach may be the right model.

**Result after Phase 4:** `Connection.write_command()`, `Connection.unack_write()`, `_Bulk.write_command()`, `_Bulk.unack_write()`, `_ClientBulk.write_command()`, and `_ClientBulk.unack_write()` have no callers and can be removed.

**Gate:** Bulk write benchmarks before/after. Suggested regression threshold: ≤2% throughput degradation.

---

## Definition of Done

- All database operations route through `Connection.command()` → `network.command()` for actual transport
- APM/logging code exists in one location
- `Connection.write_command()`, `Connection.unack_write()`, `_Bulk.write_command()`, `_Bulk.unack_write()`, `_ClientBulk.write_command()`, `_ClientBulk.unack_write()` are removed (or reduced to thin wrappers)
- No performance regression (bulk write benchmarks)
- No behavioral regression (full spec test suite passes)

---

## Risk Summary

| Phase | Risk | Effort | Value |
|-------|------|--------|-------|
| 1 — Remove dead OP_QUERY code | Low | ~1 day | Medium (simplification) |
| 2 — Extract APM helpers | Low | ~2 days | **High** (solves the stated problem) |
| 3 — Unify run_operation with network.command | Medium | ~3 days | High (true path consolidation) |
| 4 — Unify bulk write bytes path | Higher | ~1 week + benchmarking | Medium (removes last bypass) |

Phases 1 and 2 can be done together in one PR. Phase 3 should be a separate PR. Phase 4 should be gated on benchmarking.

---

## Critical Files

| File | Role |
|------|------|
| `pymongo/asynchronous/network.py:61` | Central `command()` — reference implementation |
| `pymongo/asynchronous/server.py:138` | `run_operation()` — cursor path bypass |
| `pymongo/asynchronous/pool.py:344,469,480` | `Connection.command`, `unack_write`, `write_command` |
| `pymongo/asynchronous/bulk.py:244,329,444` | `_Bulk.write_command`, `unack_write`, `_execute_batch` |
| `pymongo/asynchronous/client_bulk.py:229,320` | `_ClientBulk.write_command`, `unack_write` |
| `pymongo/message.py:394,695,1622` | `_op_msg`, `_BulkWriteContext.batch_command`, `_Query.use_command` |
| `pymongo/common.py` | `MIN_SUPPORTED_WIRE_VERSION = 8` |

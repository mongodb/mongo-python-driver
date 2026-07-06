# Spike: sans-I/O extraction to reduce `unasync` duplication

**Status:** exploratory spike (branch `state-machines`, draft PR #2918). Not intended to merge as-is.

**Question:** How far can the sans-I/O (state-machine) pattern reduce the async/sync code that `unasync` must generate — and what does it cost?

## Background

PyMongo ships two drivers for the same functionality:

- `pymongo/asynchronous/` — hand-written (`AsyncMongoClient`, `AsyncCollection`, …).
- `pymongo/synchronous/` — generated from the async source by `unasync` (token replacement: `await x` → `x`, `AsyncFoo` → `Foo`, `async def` → `def`, …).

This duplicates a lot of code that is not actually async/sync-specific — pure protocol/decision logic that happens to sit next to an `await`. Two costs follow:

1. **Maintenance:** every change must survive naive text substitution and is effectively written twice.
2. **Divergence risk:** the two copies can drift. This spike found a live example — see PYTHON-5906 below.

## The idea: separate "colorless" logic from the "colored" shell

Borrowing the *sans-I/O* framing (and the "what color is your function" metaphor):

- **Colored** code is tied to one driver: it contains `await`, an `asyncio`/`threading` lock or condition, a blocking or awaited socket call, a background loop. It *cannot* be shared and must be generated.
- **Colorless / sans-I/O** code performs no I/O and holds no locks — pure decisions over data. It can be written once in a shared top-level module and imported by both drivers.

The method for each extraction:

1. Find a pure decision embedded in a colored method (e.g. "given this error, should we reset the pool?").
2. Lift it into a shared module as a pure function.
3. Leave a thin call site in the colored method that executes the decision.

## What was extracted

| Area | Shared module / function | Was in |
| --- | --- | --- |
| Wire-message framing + validation | `network_layer.parse_wire_header()` | `network_layer.py` (both `receive_message` and `PyMongoProtocol.process_header`) |
| SDAM application-error decision tree | `pymongo/_sdam_error.py` — `decide_error_action()`, `_SDAMAction` | `topology._handle_error` |
| topologyVersion staleness | `_sdam_error.py` — `error_topology_version`, `is_stale_error_topology_version`, `is_stale_server_description` | `topology` |
| Monitor streaming predicate | `pymongo/_sdam_monitor.py` — `is_streaming_check()` | `monitor.py` (deduped 3 call sites) |
| Server-selection tie-break | `pymongo/_sdam_selection.py` — `select_least_loaded()` | `topology._select_server` |
| Selection-failure message | `_sdam_selection.py` — `format_selection_error()` | `topology._error_message` |
| Data-bearing servers | `_sdam_selection.py` — `data_bearing_servers()` | `topology` |
| **Whole config module** | `pymongo/settings.py` (shared; async/sync copies deleted) | `pymongo/{asynchronous,synchronous}/settings.py` |

~41 new unit tests were added for the extracted logic. All are **server-free** — they exercise SDAM/wire-protocol/selection decisions that previously could only be tested against a live `mongod`.

## Bug found and fixed: PYTHON-5906

Unifying the wire-header framing surfaced a real divergence. The sync `receive_message` was missing the `OP_COMPRESSED` minimum-length guard (`length <= 25`) that the async `process_header` had. A malformed compressed header with `16 < length <= 25` made the sync path compute a negative body length and raise `ValueError` instead of `ProtocolError`. Sharing `parse_wire_header()` fixes both paths from one place. (Blocked on triage; can ship as its own small PR.)

## Findings

1. **Leaf extraction is cheap and high-value.** Lifting a pure decision into a shared module + thin call site is low-risk, deletes duplicated logic, and unlocks server-free tests. This is the bulk of the branch (6 of 7 commits) and would be safe to do incrementally on `master`.

2. **A whole module *can* leave `unasync` — but the cost is the import graph, not the code.** `settings.py` is pure config, yet it was split only because it defaulted `pool_class`/`monitor_class` to the flavor-specific `pool.Pool`/`monitor.Monitor`. Relocating that one default into `topology.py` (`self._settings.pool_class or Pool`) let the module become shared with **no behavior change** — but the move touched ~17 files, because every `from pymongo.{flavor}.settings import` caller (many themselves generated) had to be rewritten. **The barrier to de-duplicating a module is how deeply its flavor-specific import path is woven through the generated layer, not how pure it is.**

3. **Zero `unasync` is not reachable — and that's fine.** The irreducibly colored shell stays: lock/condition acquisition, condition-variable waits (`asyncio` vs `threading`), background monitor loops, task lifecycle, socket I/O. No trick shares those. But essentially all the *logic* can move out of the shell, leaving generated code that contains nothing worth testing or worth risking divergence over.

## Validation

- Local: `just typing` (mypy + pyright) clean, `ruff`/`synchro` clean, full topology/SDAM/selection suites pass (sync and async twins).
- Evergreen targeted patch (server-free + `mongodb-latest`, both drivers, all topologies): green.
- Evergreen broad stress patch (server v4.2→rapid, all compression codecs, load-balancer, mockupdb, gevent, no-c-ext, encryption, pyopenssl, macOS/Win64): 113 tasks, 81 passed; all 32 failures triaged to pre-existing/unrelated causes (encryption `prefix` feature gap, mongod-start infra flakiness, timing-sensitive backpressure/handshake tests that passed under the identical diff, a Windows infra glitch). No failure attributable to this diff.

## Open question / possible follow-up

The extractions above pull *decisions* out of the shell but leave *control flow* (retry loops, waits) colored. The remaining question is whether a colored control-flow shell — e.g. `topology._select_servers_loop` — can be modeled as an explicit sans-I/O state machine that **emits intents** ("wait for a topology change", "re-apply the selector") consumed by a tiny colored driver. That would shrink even the shell, but it is a redesign rather than a leaf extraction and was left out of this spike.

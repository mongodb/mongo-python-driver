# OTel Span Coverage Gaps — Design

## Context

PYTHON-5947 (merged as PR #2964 against the `otel` branch) added operation-level
spans and transaction pseudo-spans on top of PYTHON-5945's command spans. The
final whole-branch review of that work identified four remaining gaps in span
coverage, deliberately deferred as follow-up rather than blocking that PR. This
spec covers fixing all four rather than filing JIRA tickets and deferring them.

All four gaps share one root cause: they involve call paths that either bypass
`_retry_internal`/`_OperationTelemetry` entirely, or that create/end spans in a
way that doesn't match the shape of the logical operation they represent. Each
fix reuses the existing primitives (`_OperationTelemetry`, ambient-context span
parenting) rather than inventing new mechanisms.

## Global constraints (carried over from the original PYTHON-5947 plan)

- Only edit `pymongo/asynchronous/*` and `test/asynchronous/*`; run `just synchro`
  to generate `pymongo/synchronous/*` and mirrored `test/*` files. Never
  hand-edit generated files.
- `pymongo/_otel.py` must stay the only module with a direct `opentelemetry`
  import; `pymongo/_telemetry.py` only calls into `pymongo._otel` functions.
- All span attributes must match the OTel driver spec exactly: `db.system.name`
  (always `"mongodb"`), `db.namespace`, `db.collection.name` (only when
  available), `db.operation.name`, `db.operation.summary` (same string as the
  span name).
- Run `just typing` and the affected test files before considering any part of
  this done. Every new integration test requires a live MongoDB replica set.

---

## Component 1: dbname/collection threading through `_retry_internal`

### Problem

`_OperationTelemetry`/`start_operation_span` only receive a bare operation name
string (e.g. `"find"`) at span-creation time — `db.namespace`/`db.collection.name`
are set later, lazily, once the first real command's attributes get backfilled
onto the ambient span (via `start_command_span`'s existing backfill block and
the `_CURRENT_OPERATION_NAME` contextvar). If an operation fails *before* any
command is ever sent (e.g. a server-selection timeout), the operation span is
left with only `db.system.name`/`db.operation.name` — missing `db.namespace`
and the **spec-required** `db.operation.summary`.

### Design

- `_OperationTelemetry.__init__` (`pymongo/_telemetry.py`) gains two new
  optional parameters: `dbname: Optional[str] = None`, `collection: Optional[str] = None`.
- `start_operation_span` (`pymongo/_otel.py`) gains matching parameters. When
  given, it sets `db.namespace`/`db.collection.name`/`db.operation.summary`
  (via the existing `_build_query_summary` helper) **at span creation**,
  instead of leaving them unset until backfill.
- The existing lazy backfill in `start_command_span` is unchanged and still
  fires on every real command, overwriting the eager values with the
  authoritative ones derived from the actual wire command (which may differ —
  e.g. `explain` wrapping, `getMore`'s `collection` field). The eager values
  only matter for the case where no command is ever sent.
- `_retryable_read`, `_retryable_write`, `_retry_internal`, and
  `_ClientConnectionRetryable.__init__` (`pymongo/asynchronous/mongo_client.py`)
  each gain matching optional `dbname`/`collection` passthrough parameters,
  threaded straight through to `_OperationTelemetry`.

### Call sites (~48, across 6 files)

Each call site passes what it already has on hand:

| File | What's passed |
|---|---|
| `pymongo/asynchronous/collection.py` | `dbname=self._database.name, collection=self.name` |
| `pymongo/asynchronous/database.py` | `dbname=self.name, collection=None` |
| `pymongo/asynchronous/bulk.py` | Same as `collection.py` (holds a `Collection` reference) |
| `pymongo/asynchronous/client_bulk.py` | `dbname="admin", collection=None` (acknowledged path; the unacknowledged path already handles this manually per PYTHON-5947's Task 6) |
| `pymongo/asynchronous/client_session.py` (`_finish_transaction_with_retry`) | `dbname="admin", collection=None` (commit/abortTransaction always target admin) |
| `pymongo/asynchronous/change_stream.py` | The watched target's db/collection name, or `dbname=None` if watching an entire client/cluster |

---

## Component 2: `getMore` nesting under the originating find/aggregate span

### Problem

`AsyncCursor._refresh()` handles both the initial query and every later
`getMore` identically, and each call independently invokes `_run_operation` →
`_retryable_read` → `_retry_internal`, which constructs a brand-new
`_OperationTelemetry` every time. The result: one `find` operation span, plus
one *sibling* `getMore` operation span per batch, with no parent/child
relationship — even though the spec's covered-operations table doesn't list
`getMore` as its own operation at all.

### Design

The operation span must live for the cursor's entire lifetime, not one
`_refresh()` call:

- `AsyncCursor` gains a new attribute, `self._operation_telemetry: Optional[_OperationTelemetry]`,
  initialized to `None`.
- On the **first** `_refresh()` call (`self._id is None`, about to send the
  initial find/aggregate), the cursor creates the `_OperationTelemetry`
  itself — via `_OperationTelemetry(tracing_options, operation.name, session, dbname=..., collection=...)`
  using Component 1's new parameters — and stores it on `self._operation_telemetry`,
  entering it (`__enter__`).
- `_retry_internal` gains a new optional parameter, `operation_telemetry: Optional[_OperationTelemetry] = None`.
  When provided, `_retry_internal` does **not** construct its own
  `_OperationTelemetry`; instead it makes the given one's span current for the
  duration of just that call via `opentelemetry.trace.use_span(span, end_on_exit=False)`
  — the standard OTel API for "make an existing span current without ending
  it" — and does not call `succeeded()`/`failed()` on it (lifecycle stays with
  the caller).
- `AsyncCursor._refresh()`/`_send_message()` (`pymongo/asynchronous/cursor.py`)
  passes `self._operation_telemetry` into `_run_operation`/`_retryable_read` on
  every call, first and subsequent alike, so every `getMore`'s command span
  nests under the same still-open span.
- The span ends exactly once, via a new `_end_operation_telemetry()` helper
  called from whichever of these fires first: the cursor detecting exhaustion
  (`self._id` becomes `0` after a `getMore` reply), `AsyncCursor.close()`, or
  — as a best-effort fallback for abandoned cursors that are never exhausted
  or explicitly closed — `__del__`, mirroring the existing pattern used for
  `_Transaction.__del__`'s connection cleanup.
- `AsyncCommandCursor` (used by `aggregate`, `list_indexes`, etc.) gets the
  identical treatment, since it shares the same `_refresh`/exhaustion shape.

---

## Component 3: `killCursors` / `endSessions` operation spans

### Problem

Both commands are sent via `conn.command(...)` directly, bypassing
`_retry_internal` entirely (they are fire-and-forget, error-swallowing,
must-never-retry operations, so they don't belong in the retry/backoff
machinery). They get a command span (via `_run_command`'s existing
`_CommandTelemetry`) but no operation span — violating the spec's "command
spans MUST be nested to the corresponding operation span" requirement.

### Design

`_OperationTelemetry` is already a plain context manager, not intrinsically
tied to retries — the fix is to wrap the existing call sites directly, with no
changes to `_retry_internal`:

- `_kill_cursor_impl` (`pymongo/asynchronous/mongo_client.py`): wrap the
  existing `await conn.command(db, spec, ...)` call in
  `with _OperationTelemetry(self.options.tracing, _Op.KILL_CURSORS, session, dbname=db, collection=coll):`
  — `db`/`coll` are already parsed from `address.namespace` right above this
  call.
- `_end_sessions` (`pymongo/asynchronous/mongo_client.py`): wrap each batched
  `await conn.command("admin", spec, ...)` call in
  `with _OperationTelemetry(self.options.tracing, _Op.END_SESSIONS, None, dbname="admin"):`.
- Both `_Op.KILL_CURSORS` and `_Op.END_SESSIONS` already exist as operation-id
  enum values (`pymongo/operations.py`) — reused here as the span's operation
  name.

---

## Component 4: one span per `with_transaction()` call, plus the retried-commit gap

### Problem

`with_transaction()`'s retry loop (`pymongo/asynchronous/client_session.py`)
calls `start_transaction()` fresh on every full-transaction retry, and each
call creates a brand-new `"transaction"` span — so a retried `with_transaction()`
produces multiple *sibling* transaction spans instead of one span representing
the whole logical call, contrary to the spec's `withTransaction` section.

A related bug found during design: when `commit_transaction()` is retried
directly (state `COMMITTED` → `IN_PROGRESS`, the "explicitly retrying the
commit" branch), the prior attempt's `finally` block already ended and cleared
`session._transaction.span` — so the retried commit runs with **no** transaction
span at all (not stale, just absent), and the resulting command span has no
transaction-span parent.

### Design

- Wrap the entire `with_transaction()` body — both the outer full-retry loop
  and the inner commit-retry loop — in one new span for the whole logical
  call: `with _OperationTelemetry(tracing_options, "withTransaction", session):`,
  entered before the loop begins (making it current via `start_as_current_span`
  under the hood) and ended once when the method returns or raises.
- No changes needed to `start_transaction`/`commit_transaction`/`abort_transaction`
  for the nesting itself: `start_transaction_span` already has no explicit
  `context=` (confirmed correct/intentional in the PYTHON-5947 review), so it
  already inherits whatever is ambiently current — once the `withTransaction`
  span is current, every retry's `"transaction"` span automatically nests
  under it for free.
- Separately, fix the retried-commit gap: in `commit_transaction()`'s
  "explicitly retrying the commit" branch (`state is _TxnState.COMMITTED`),
  create a fresh `session._transaction.span` via `_otel.start_transaction_span(...)`
  if one isn't already present, before calling `_finish_transaction_with_retry`
  again.

---

## Testing

Each component gets integration tests against a live replica set (no mocks),
following the existing patterns in `test/asynchronous/test_otel.py`:

- **Component 1**: force a failure before any command is sent (e.g. an
  unreachable server address) and assert the resulting operation span still
  has `db.namespace`/`db.collection.name`/`db.operation.summary`.
- **Component 2**: iterate a cursor with a small `batch_size` across multiple
  `getMore`s; assert exactly one `find`/`aggregate`-named operation span
  exists, and every `getMore` appears only as a command span nested under it
  (no sibling `getMore` operation spans). Also test the abandoned-cursor
  fallback (drop all references, force GC, confirm the span still ends).
- **Component 3**: force a killCursors (abandon a cursor with pending
  batches, or call `close()` on one) and an endSessions (call `client.close()`
  with an implicit session in use); assert a `killCursors`/`endSessions`-named
  operation span appears for each.
- **Component 4**: inject a `TransientTransactionError` to force a full
  transaction retry; assert one `withTransaction` span with multiple nested
  `transaction` child spans. Inject an `UnknownTransactionCommitResult` to
  force a commit retry; assert the retried commit's command span still has a
  transaction-span parent.

## Edge cases

- Abandoned, never-exhausted, never-closed cursors: handled by the `__del__`
  best-effort fallback in Component 2, mirroring the existing
  `_Transaction.__del__` pattern.
- `bulk.py`'s legacy `Bulk` API holds a `Collection` reference, so
  `dbname`/`collection` are available the same way as ordinary collection
  methods (Component 1).
- Nested transactions aren't a MongoDB concept, so Component 4 has no
  re-entrancy case.

## Out of scope

- Any further OTel spec gaps not named in the four components above.
- Changing `start_transaction_span`'s ambient-context parenting behavior
  itself (already confirmed correct in the original PYTHON-5947 review).

# PYTHON-5947: OpenTelemetry Operation and Transaction Spans Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend PyMongo's OpenTelemetry command-span support (PYTHON-5945) with operation-level spans (one per public-API call, spanning all retry attempts) and transaction pseudo-spans (one per `start_transaction()`...`commit_transaction()`/`abort_transaction()` lifecycle), plus unified-test-format wiring so the spec's tracing test suite can run.

**Architecture:** Command spans (existing, unmodified in their own creation logic) already nest correctly under whatever span is "current" when `_TRACER.start_span()` is called, with zero changes needed to that call. Operation spans are entered via `start_as_current_span()` at the single retry-loop choke point (`_retry_internal` in `mongo_client.py`), so every command attempt inside a retried operation automatically nests under it. A `ContextVar` carries the operation name across that ambient boundary so the first command executed inside an operation span can backfill its name/namespace attributes (dbname/collection aren't known until then). Transaction spans are *not* pushed as ambient/current — they're stored explicitly on `session._transaction.span` and passed as an explicit parent context to `start_operation_span()` only when `session.in_transaction`, avoiding ambient-context leakage across unrelated sessions in the same coroutine.

**Tech Stack:** Python, `opentelemetry-api` (already an optional dependency via the `pymongo[opentelemetry]` extra from PYTHON-5945), `opentelemetry-sdk` (test-only, for `InMemorySpanExporter`).

## Global Constraints

- Only edit `pymongo/asynchronous/*` and `test/asynchronous/*`; run `just synchro` to generate `pymongo/synchronous/*` and mirrored `test/*` files. Never hand-edit generated files.
- `pymongo/_otel.py` must stay the only module with a direct `opentelemetry` import; `pymongo/_telemetry.py` only calls into `pymongo._otel` functions.
- All new/changed span attributes must match the OTel driver spec exactly: `db.system.name` (always `"mongodb"`), `db.namespace`, `db.collection.name` (only when available), `db.operation.name`, `db.operation.summary` (same string as the span name). Transaction spans have exactly one attribute: `db.system.name="mongodb"`.
- Operation span name format: `"{operation} {dbname}.{collection}"` if a collection applies, else `"{operation} {dbname}"` — reuse the existing `_build_query_summary()` helper, don't reimplement it.
- Every new integration test requires a live MongoDB server (`just run-server`); run `just typing` and the affected test files before considering any task done.
- Existing regression test `TestOTelTracerCaching.test_start_command_span_does_not_call_get_tracer` (test/asynchronous/test_otel.py) must keep passing — never call `trace.get_tracer()` outside the module-level `_TRACER` cache.

---

## Design reference (for the implementer's own understanding, not to re-derive)

- **Where operation spans hook in:** `AsyncMongoClient._retry_internal` (`pymongo/asynchronous/mongo_client.py:2013-2057`) is the *only* caller of `_ClientConnectionRetryable(...).run()`, and `run()` (2831-2981) is the single retry loop shared by all reads, writes, and `commitTransaction`/`abortTransaction` (via `_finish_transaction_with_retry`). Wrapping `_retry_internal` itself — not touching `run()`'s internals — covers every one of these uniformly, with no changes to the already-complex retry/backoff logic.
- **Why dbname/collection are set lazily:** `_retry_internal`/`_ClientConnectionRetryable` only carry a bare `operation` string (e.g. `"find"`, `"bulkWrite"`, `"commitTransaction"`); dbname/collection only become known once a command is actually constructed, deep inside `_run_command` (`pymongo/asynchronous/command_runner.py`). The OTel spec requires starting the span "as soon as possible", so the operation span starts immediately with a provisional name/attributes, and the first command executed inside it backfills the real name via `Span.update_name()` (attributes may always be added after creation; renaming is the same idea applied to the name).
- **Why transaction spans are not ambient:** two unrelated `ClientSession`s could have operations interleaved in the same `asyncio` task (e.g. via `asyncio.gather`). If the transaction span were pushed via `context.attach()`/ambient `start_as_current_span()`, a second session's unrelated operation running after the first session's `start_transaction()` would incorrectly inherit that context. Storing the span explicitly on `session._transaction.span` and passing it as an explicit `parent_span` to `start_operation_span()` only when that specific session is in a transaction has no such risk.
- **Client bulk write (`client_bulk.py`) acknowledged path needs no separate wiring**: `execute()` → `execute_command()` → `self.client._retryable_write(...)` → `_retry_internal` (same path as any other write), so it gets an operation span "bulkWrite" for free, and since the underlying `bulkWrite` server command always targets the `admin` database, the lazy-tagging backfill naturally produces `db.namespace="admin"` with no `db.collection.name` (matches spec: no single collection for a multi-namespace bulk write). Only the **unacknowledged** (`w=0`) path bypasses `_retry_internal` entirely and needs a manual span wrapped directly around it in `execute()`, with `dbname="admin"` passed explicitly (no lazy tagging available on that path).

---

### Task 1: Operation-span primitives in `pymongo/_otel.py`

**Files:**
- Modify: `pymongo/_otel.py`
- Test: `test/asynchronous/test_otel.py` (new unit tests, no live server needed — these test `_otel.py` functions directly against a `TracerProvider`/`InMemorySpanExporter`)

**Interfaces:**
- Produces: `_otel.start_operation_span(tracing_options, operation, parent_span) -> Optional[_OperationSpanHandle]`, `_otel.end_operation_span_success(handle) -> None`, `_otel.end_operation_span_failure(handle, exc) -> None`, `_otel._CURRENT_OPERATION_NAME: ContextVar[Optional[str]]` — consumed by Task 2's `_OperationTelemetry` and by the modification to `start_command_span` in this same task.

- [ ] **Step 1: Write the failing unit tests**

Add to `test/asynchronous/test_otel.py` (near the existing `TestOTelTracerCaching`/module-level helpers, using the existing `_shared_test_provider()` and an `InMemorySpanExporter`):

```python
class TestOTelOperationSpanPrimitives(unittest.TestCase):
    """Unit tests for the pymongo._otel operation-span primitives (no live server needed)."""

    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.exporter.clear()

    def test_start_operation_span_disabled_returns_none(self):
        handle = _otel.start_operation_span(None, "find", None)
        self.assertIsNone(handle)

    def test_start_operation_span_success_sets_provisional_attributes(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "find", None)
        self.assertIsNotNone(handle)
        _otel.end_operation_span_success(handle)
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")
        self.assertEqual(span.attributes["db.system.name"], "mongodb")
        self.assertEqual(span.attributes["db.operation.name"], "find")
        self.assertEqual(span.status.status_code, StatusCode.UNSET)

    def test_start_operation_span_failure_records_exception(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        handle = _otel.start_operation_span(opts, "insert", None)
        _otel.end_operation_span_failure(handle, ValueError("boom"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(len(span.events), 1)
        self.assertEqual(span.events[0].name, "exception")

    def test_start_operation_span_with_parent(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        parent_handle = _otel.start_operation_span(opts, "transaction", None)
        handle = _otel.start_operation_span(opts, "insert", parent_handle.span)
        _otel.end_operation_span_success(handle)
        _otel.end_operation_span_success(parent_handle)
        child, parent = self.exporter.get_finished_spans()
        self.assertEqual(child.parent.span_id, parent.context.span_id)

    def test_current_operation_name_contextvar_scoped_correctly(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
        handle = _otel.start_operation_span(opts, "find", None)
        self.assertEqual(_otel._CURRENT_OPERATION_NAME.get(), "find")
        _otel.end_operation_span_success(handle)
        self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
```

Add the needed imports at the top of `test/asynchronous/test_otel.py` if not already present: `from pymongo import _otel` (it's likely already imported as the module is under test) and `StatusCode` from `opentelemetry.trace` (already imported for other tests in this file per the existing `test_otel.py` content).

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest test/asynchronous/test_otel.py -k TestOTelOperationSpanPrimitives -v`
Expected: FAIL/ERROR with `AttributeError: module 'pymongo._otel' has no attribute 'start_operation_span'`

- [ ] **Step 3: Implement the primitives in `pymongo/_otel.py`**

Add near the top, after the existing `_TRACER`/`_HAS_OPENTELEMETRY` setup (after line 46, before the `if TYPE_CHECKING:` block at line 48):

```python
from contextvars import ContextVar

# The operation name of whichever operation span is currently active (entered
# via start_operation_span), so start_command_span can backfill the operation
# span's name/namespace attributes from the first command executed inside it
# (dbname/collection aren't known until then -- see start_operation_span).
_CURRENT_OPERATION_NAME: ContextVar[Optional[str]] = ContextVar(
    "_CURRENT_OPERATION_NAME", default=None
)
```

Add near the bottom of the file, after `end_command_span_failure`:

```python
class _OperationSpanHandle:
    """Bundles what start_operation_span hands back so callers can end the span correctly.

    ``span`` is exposed directly so a transaction span can be looked up
    (``handle.span``) and passed as another operation span's ``parent_span``.
    """

    __slots__ = ("span", "_cm", "_name_token")

    def __init__(self, span: Span, cm: Any, name_token: Any) -> None:
        self.span = span
        self._cm = cm
        self._name_token = name_token


def start_operation_span(
    tracing_options: Optional[TracingOptions],
    operation: str,
    parent_span: Optional[Span],
) -> Optional[_OperationSpanHandle]:
    """Start (and make current) a CLIENT-kind span for one logical operation, or None.

    Spans all retry attempts of one call to _retry_internal. Named
    provisionally after the bare operation name -- dbname/collection aren't
    known yet, since server selection hasn't happened -- and backfilled by
    start_command_span once the first command inside it is built.

    ``parent_span`` (the active transaction span, if any) becomes this span's
    *explicit* parent; it is deliberately not read from ambient context, to
    avoid a concurrently-running unrelated session's operations picking up
    this transaction by accident. Pass None outside of a transaction.
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    assert _TRACER is not None  # _is_tracing_enabled already checked _HAS_OPENTELEMETRY
    context = (
        trace.set_span_in_context(parent_span) if parent_span is not None else None
    )
    cm = _TRACER.start_as_current_span(
        operation,
        kind=SpanKind.CLIENT,
        context=context,
        attributes={"db.system.name": "mongodb", "db.operation.name": operation},
    )
    span = cm.__enter__()
    name_token = _CURRENT_OPERATION_NAME.set(operation)
    return _OperationSpanHandle(span, cm, name_token)


def end_operation_span_success(handle: Optional[_OperationSpanHandle]) -> None:
    """End the operation span with no error status."""
    if handle is None:
        return
    _CURRENT_OPERATION_NAME.reset(handle._name_token)
    handle._cm.__exit__(None, None, None)


def end_operation_span_failure(
    handle: Optional[_OperationSpanHandle], exc: BaseException
) -> None:
    """Record the exception, set the error status, and end the operation span."""
    if handle is None:
        return
    _CURRENT_OPERATION_NAME.reset(handle._name_token)
    handle.span.record_exception(exc)
    handle.span.set_status(Status(StatusCode.ERROR, description=str(exc)))
    handle._cm.__exit__(type(exc), exc, exc.__traceback__)
```

Then modify `start_command_span` to backfill the ambient operation span. Insert this right after the existing `collection = _extract_collection_name(command_name, dbname, cmd)` line (currently line 211), before the `address = conn.address` line:

```python
collection = _extract_collection_name(command_name, dbname, cmd)
current_operation = _CURRENT_OPERATION_NAME.get()
if current_operation is not None:
    current_span = trace.get_current_span()
    if current_span.is_recording():
        summary = _build_query_summary(current_operation, dbname, collection)
        current_span.update_name(summary)
        current_span.set_attribute("db.namespace", dbname)
        current_span.set_attribute("db.operation.summary", summary)
        if collection:
            current_span.set_attribute("db.collection.name", collection)
address = conn.address
```

(Idempotent: harmless to run again on every retry attempt/command, since one logical operation always targets the same namespace.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest test/asynchronous/test_otel.py -k "TestOTelOperationSpanPrimitives or TestOTelTracerCaching" -v`
Expected: PASS (all new tests, and the pre-existing tracer-caching regression test still passes)

- [ ] **Step 5: Run `just synchro` and commit**

```bash
just synchro
just typing
git add pymongo/_otel.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add operation-span primitives to pymongo._otel"
```

---

### Task 2: Transaction-span primitives in `pymongo/_otel.py`

**Files:**
- Modify: `pymongo/_otel.py`
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Produces: `_otel.start_transaction_span(tracing_options) -> Optional[Span]`, `_otel.end_transaction_span(span) -> None` — consumed by Task 5 (`client_session.py`).

- [ ] **Step 1: Write the failing unit tests**

Add to `test/asynchronous/test_otel.py`, in the same `TestOTelOperationSpanPrimitives` class (or a sibling `TestOTelTransactionSpanPrimitives` — keep it a separate class for clarity):

```python
class TestOTelTransactionSpanPrimitives(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.exporter.clear()

    def test_start_transaction_span_disabled_returns_none(self):
        self.assertIsNone(_otel.start_transaction_span(None))

    def test_start_transaction_span_has_only_one_attribute(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        span = _otel.start_transaction_span(opts)
        _otel.end_transaction_span(span)
        (finished,) = self.exporter.get_finished_spans()
        self.assertEqual(finished.name, "transaction")
        self.assertEqual(dict(finished.attributes), {"db.system.name": "mongodb"})

    def test_end_transaction_span_is_none_safe(self):
        _otel.end_transaction_span(None)  # must not raise
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest test/asynchronous/test_otel.py -k TestOTelTransactionSpanPrimitives -v`
Expected: FAIL with `AttributeError: module 'pymongo._otel' has no attribute 'start_transaction_span'`

- [ ] **Step 3: Implement in `pymongo/_otel.py`**

Add after the operation-span functions from Task 1:

```python
def start_transaction_span(tracing_options: Optional[TracingOptions]) -> Optional[Span]:
    """Start (but do not make current) the ``"transaction"`` pseudo-span, or None.

    Not pushed as ambient/current context -- it's stored explicitly on
    ``session._transaction.span`` and passed as the explicit ``parent_span``
    wherever an operation span is started under this transaction (see
    :func:`start_operation_span`). Per the OTel driver spec, this span has
    exactly one attribute.
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    assert _TRACER is not None
    return _TRACER.start_span(
        "transaction", kind=SpanKind.CLIENT, attributes={"db.system.name": "mongodb"}
    )


def end_transaction_span(span: Optional[Span]) -> None:
    """End the transaction span, if any."""
    if span is None:
        return
    span.end()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest test/asynchronous/test_otel.py -k TestOTelTransactionSpanPrimitives -v`
Expected: PASS

- [ ] **Step 5: Run `just synchro` and commit**

```bash
just synchro
just typing
git add pymongo/_otel.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add transaction pseudo-span primitives to pymongo._otel"
```

---

### Task 3: `_OperationTelemetry` in `pymongo/_telemetry.py`

**Files:**
- Modify: `pymongo/_telemetry.py`
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes: `_otel.start_operation_span`, `_otel.end_operation_span_success`, `_otel.end_operation_span_failure` (Task 1).
- Produces: `_telemetry._OperationTelemetry(tracing_options, operation, session)`, with `.succeeded() -> None` and `.failed(exc: BaseException) -> None` — consumed by Task 4 (`mongo_client.py`) and Task 6 (`client_bulk.py`).

- [ ] **Step 1: Write the failing unit test**

Add to `test/asynchronous/test_otel.py`:

```python
class TestOperationTelemetry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        cls.exporter = InMemorySpanExporter()
        _shared_test_provider().add_span_processor(SimpleSpanProcessor(cls.exporter))

    def setUp(self):
        self.exporter.clear()

    def test_succeeded_with_no_session(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _telemetry._OperationTelemetry(opts, "find", None)
        telemetry.succeeded()
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "find")
        self.assertIsNone(span.parent)

    def test_failed_records_exception(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _telemetry._OperationTelemetry(opts, "insert", None)
        telemetry.failed(RuntimeError("nope"))
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)

    def test_nests_under_active_transaction_span(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        txn_span = _otel.start_transaction_span(opts)

        class _FakeTransaction:
            span = txn_span

        class _FakeSession:
            in_transaction = True
            _transaction = _FakeTransaction()

        telemetry = _telemetry._OperationTelemetry(opts, "insert", _FakeSession())
        telemetry.succeeded()
        _otel.end_transaction_span(txn_span)
        child, parent = self.exporter.get_finished_spans()
        self.assertEqual(child.parent.span_id, parent.context.span_id)

    def test_disabled_is_a_no_op(self):
        telemetry = _telemetry._OperationTelemetry(None, "find", None)
        telemetry.succeeded()  # must not raise
        telemetry2 = _telemetry._OperationTelemetry(None, "find", None)
        telemetry2.failed(RuntimeError("x"))  # must not raise
        self.assertEqual(self.exporter.get_finished_spans(), ())
```

Add `from pymongo import _telemetry` to the test file's imports if not already present (check first — `_telemetry` may already be imported since `test_otel.py` likely references `_CommandTelemetry` indirectly via integration tests, but the direct module import may not exist yet).

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/asynchronous/test_otel.py -k TestOperationTelemetry -v`
Expected: FAIL with `AttributeError: module 'pymongo._telemetry' has no attribute '_OperationTelemetry'`

- [ ] **Step 3: Implement `_OperationTelemetry` in `pymongo/_telemetry.py`**

Add near `_CommandTelemetry` (after its class definition), following the same `__slots__`-based, lifecycle-method idiom:

```python
class _OperationTelemetry:
    """One span-scoped context per logical operation (spanning all retry attempts).

    Construct once per call to ``_retry_internal``; call :meth:`succeeded` or
    :meth:`failed` exactly once when the operation's outcome is known. A
    no-op throughout when tracing is disabled.
    """

    __slots__ = ("_handle",)

    def __init__(
        self,
        tracing_options: Optional[_otel.TracingOptions],
        operation: str,
        session: Optional[Any],
    ) -> None:
        parent_span = None
        if session is not None and session.in_transaction:
            parent_span = session._transaction.span
        self._handle = _otel.start_operation_span(
            tracing_options, operation, parent_span
        )

    def succeeded(self) -> None:
        _otel.end_operation_span_success(self._handle)

    def failed(self, exc: BaseException) -> None:
        _otel.end_operation_span_failure(self._handle, exc)
```

Check the top of `pymongo/_telemetry.py` for an existing `from pymongo import _otel` (or `from pymongo import _otel as _otel`) import — it's already imported there since `_CommandTelemetry` calls `_otel.start_command_span` etc. Reuse it; do not add a second import.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest test/asynchronous/test_otel.py -k TestOperationTelemetry -v`
Expected: PASS

- [ ] **Step 5: Run `just synchro` and commit**

```bash
just synchro
just typing
git add pymongo/_telemetry.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add _OperationTelemetry class"
```

---

### Task 4: Wire operation spans into `_retry_internal` (`pymongo/asynchronous/mongo_client.py`)

**Files:**
- Modify: `pymongo/asynchronous/mongo_client.py:2013-2057` (`_retry_internal`)
- Test: `test/asynchronous/test_otel.py` (new integration tests, live server required)

**Interfaces:**
- Consumes: `_telemetry._OperationTelemetry` (Task 3).
- Produces: every `find`/`insert`/etc. now gets an operation span wrapping all its command-span retry attempts — consumed by Task 7 (docstring/changelog) and exercised by Task 9 (spec tests).

- [ ] **Step 1: Write the failing integration test**

Add to the `TestOTelSpans` class in `test/asynchronous/test_otel.py` (it already has `self.exporter`/`self.spans()` set up per the existing class):

```python
async def test_operation_span_wraps_command_span_for_find(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client[self.db.name].test
    await coll.insert_one({"x": 1})
    self.exporter.clear()
    await coll.find_one({"x": 1})

    operation_spans = self.spans("find")
    command_spans = [
        s for s in self.exporter.get_finished_spans() if s.kind == s.kind.CLIENT
    ]
    # Exactly one operation span and one nested command span with the same name.
    matching = [
        s for s in operation_spans if s.attributes.get("db.operation.name") == "find"
    ]
    self.assertEqual(len(matching), 1)
    op_span = matching[0]
    self.assertEqual(op_span.attributes["db.namespace"], self.db.name)
    self.assertEqual(op_span.attributes["db.collection.name"], "test")
    cmd_spans = [
        s
        for s in command_spans
        if s.parent and s.parent.span_id == op_span.context.span_id
    ]
    self.assertEqual(len(cmd_spans), 1)
    self.assertEqual(cmd_spans[0].name, "find")


async def test_operation_span_records_failure(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client[self.db.name].test
    self.exporter.clear()
    with self.assertRaises(Exception):
        await coll.find_one({"$invalidOperator": 1})
    matching = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.operation.name") == "find"
    ]
    self.assertEqual(len(matching), 1)
    self.assertEqual(matching[0].status.status_code, StatusCode.ERROR)
```

(`self.spans(name)` is the existing helper at `test_otel.py:78` filtering `self.exporter.get_finished_spans()` by span name; reuse it. `self.db` is the standard `AsyncIntegrationTest` fixture database.)

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest test/asynchronous/test_otel.py -k "test_operation_span_wraps_command_span_for_find or test_operation_span_records_failure" -v`
Expected: FAIL — no operation span is produced yet (only the command span exists, unparented).

- [ ] **Step 3: Wire `_OperationTelemetry` into `_retry_internal`**

In `pymongo/asynchronous/mongo_client.py`, add the import near the top with the other `pymongo` internal imports (check for an existing `from pymongo import ...` block and the existing `from pymongo._telemetry import ...` or similar — if `_telemetry` isn't imported yet in this file, add `from pymongo._telemetry import _OperationTelemetry`).

Replace the body of `_retry_internal` (currently a single `return await _ClientConnectionRetryable(...).run()`, lines 2044-2057):

```python
@_csot.apply
async def _retry_internal(
    self,
    func: _WriteCall[T] | _ReadCall[T],
    session: Optional[AsyncClientSession],
    bulk: Optional[Union[_AsyncBulk, _AsyncClientBulk]],
    operation: str,
    is_read: bool = False,
    address: Optional[_Address] = None,
    read_pref: Optional[_ServerMode] = None,
    retryable: bool = False,
    operation_id: Optional[int] = None,
    is_run_command: bool = False,
    is_aggregate_write: bool = False,
) -> T:
    """Internal retryable helper for all client transactions.

    :param func: Callback function we want to retry
    :param session: Client Session on which the transaction should occur
    :param bulk: Abstraction to handle bulk write operations
    :param operation: The name of the operation that the server is being selected for
    :param is_read: If this is an exclusive read transaction, defaults to False
    :param address: Server Address, defaults to None
    :param read_pref: Topology of read operation, defaults to None
    :param retryable: If the operation should be retried once, defaults to None
    :param is_run_command: If this is a runCommand operation, defaults to False
    :param is_aggregate_write: If this is a aggregate operation with a write, defaults to False.
    :param operation_id: Stable operation id shared across retries, defaults to None

    :return: Output of the calling func()
    """
    operation_telemetry = _OperationTelemetry(self.options.tracing, operation, session)
    try:
        result = await _ClientConnectionRetryable(
            mongo_client=self,
            func=func,
            bulk=bulk,
            operation=operation,
            is_read=is_read,
            session=session,
            read_pref=read_pref,
            address=address,
            retryable=retryable,
            operation_id=operation_id,
            is_run_command=is_run_command,
            is_aggregate_write=is_aggregate_write,
        ).run()
    except BaseException as exc:
        operation_telemetry.failed(exc)
        raise
    else:
        operation_telemetry.succeeded()
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest test/asynchronous/test_otel.py -k "test_operation_span_wraps_command_span_for_find or test_operation_span_records_failure" -v`
Expected: PASS

Also re-run the full existing suite to check for regressions (operation spans must not appear when tracing is disabled):

Run: `pytest test/asynchronous/test_otel.py -v`
Expected: All PASS, including the pre-existing `test_span_created_for_insert_and_find`/`test_query_text_included_when_configured`/prose tests (these don't assert operation-span absence, so they should be unaffected either way; if any assert `len(self.spans()) == 1`, they'll now need widening — check and fix here, and note the ones with `TODO(PYTHON-5947)` for Task 9's cleanup instead).

- [ ] **Step 5: Run `just synchro`, full typing, and commit**

```bash
just synchro
just typing
git add pymongo/asynchronous/mongo_client.py pymongo/synchronous/mongo_client.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Wire operation spans into the retryable read/write loop"
```

---

### Task 5: Transaction pseudo-span in `pymongo/asynchronous/client_session.py`

**Files:**
- Modify: `pymongo/asynchronous/client_session.py` — `_Transaction` class (417-476), `start_transaction` (830-868), `commit_transaction` (870-911), `abort_transaction` (913-939)
- Test: `test/asynchronous/test_otel.py` (integration, requires a replica set / transactions support)

**Interfaces:**
- Consumes: `_otel.start_transaction_span`, `_otel.end_transaction_span` (Task 2). Sets `session._transaction.span`, consumed by `_telemetry._OperationTelemetry` (Task 3, already reads it).

- [ ] **Step 1: Write the failing integration test**

Add to `TestOTelSpans` in `test/asynchronous/test_otel.py` (guard with the existing transaction-support skip mechanism used elsewhere in the test suite, e.g. `@client_context.require_transactions`):

```python
@async_client_context.require_transactions
async def test_transaction_span_parents_operation_and_command_spans(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client[self.db.name].test
    await coll.insert_one({"x": 1})
    self.exporter.clear()

    async with client.start_session() as session:
        async with session.start_transaction():
            await coll.insert_one({"x": 2}, session=session)
            await coll.insert_one({"x": 3}, session=session)

    finished = self.exporter.get_finished_spans()
    txn_span = next(s for s in finished if s.name == "transaction")
    self.assertEqual(dict(txn_span.attributes), {"db.system.name": "mongodb"})

    insert_op_spans = [
        s for s in finished if s.attributes.get("db.operation.name") == "insert"
    ]
    self.assertEqual(len(insert_op_spans), 2)
    for op_span in insert_op_spans:
        self.assertEqual(op_span.parent.span_id, txn_span.context.span_id)

    commit_op_spans = [
        s
        for s in finished
        if s.attributes.get("db.operation.name") == "commitTransaction"
    ]
    self.assertEqual(len(commit_op_spans), 1)
    self.assertEqual(commit_op_spans[0].parent.span_id, txn_span.context.span_id)


@async_client_context.require_transactions
async def test_aborted_transaction_still_ends_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client[self.db.name].test
    self.exporter.clear()

    async with client.start_session() as session:
        async with session.start_transaction():
            await coll.insert_one({"x": 4}, session=session)
            await session.abort_transaction()

    finished = self.exporter.get_finished_spans()
    txn_span = next(s for s in finished if s.name == "transaction")
    self.assertTrue(txn_span.end_time is not None)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest test/asynchronous/test_otel.py -k "test_transaction_span_parents or test_aborted_transaction_still_ends_span" -v`
Expected: FAIL — no `"transaction"` span exists yet (`next()` raises `StopIteration`).

- [ ] **Step 3: Implement in `pymongo/asynchronous/client_session.py`**

Add the `span` field to `_Transaction.__init__` (line ~420-429) and clear it in `reset()` (line ~463-469):

```python
def __init__(self, opts: Optional[TransactionOptions], client: AsyncMongoClient[Any]):
    self.opts = opts
    self.state = _TxnState.NONE
    self.sharded = False
    self.pinned_address: Optional[_Address] = None
    self.conn_mgr: Optional[_ConnectionManager] = None
    self.recovery_token = None
    self.attempt = 0
    self.client = client
    self.has_completed_command = False
    self.span: Optional[Any] = None
```

```python
async def reset(self) -> None:
    await self.unpin()
    self.state = _TxnState.NONE
    self.sharded = False
    self.recovery_token = None
    self.attempt = 0
    self.has_completed_command = False
    self.span = None
```

Add `from pymongo import _otel` to this file's imports if not already present.

In `start_transaction`, right after `self._transaction.state = _TxnState.STARTING` (line 866):

```python
await self._transaction.reset()
self._transaction.state = _TxnState.STARTING
self._transaction.span = _otel.start_transaction_span(
    self._transaction.client.options.tracing
)
self._start_retryable_write()
return _TransactionContext(self)
```

In `commit_transaction`, the "transaction never started server-side" early return (line 879-882) and the final `finally` (line 910-911):

```
        elif state in (_TxnState.STARTING, _TxnState.COMMITTED_EMPTY):
            # Server transaction was never started, no need to send a command.
            self._transaction.state = _TxnState.COMMITTED_EMPTY
            _otel.end_transaction_span(self._transaction.span)
            self._transaction.span = None
            return
```

```
        finally:
            self._transaction.state = _TxnState.COMMITTED
            _otel.end_transaction_span(self._transaction.span)
            self._transaction.span = None
```

In `abort_transaction`, the "transaction never started" early return (line 923-926) and the final `finally` (line 937-939):

```
        elif state is _TxnState.STARTING:
            # Server transaction was never started, no need to send a command.
            self._transaction.state = _TxnState.ABORTED
            _otel.end_transaction_span(self._transaction.span)
            self._transaction.span = None
            return
```

```
        finally:
            self._transaction.state = _TxnState.ABORTED
            _otel.end_transaction_span(self._transaction.span)
            self._transaction.span = None
            await self._unpin()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest test/asynchronous/test_otel.py -k "test_transaction_span_parents or test_aborted_transaction_still_ends_span" -v`
Expected: PASS (requires a replica-set test server: `MONGODB_VERSION=8.0 just run-server` with a replica set topology, or whatever this repo's default `run-server` provides — transactions require it).

- [ ] **Step 5: Run `just synchro`, full typing, and commit**

```bash
just synchro
just typing
git add pymongo/asynchronous/client_session.py pymongo/synchronous/client_session.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add transaction pseudo-spans"
```

---

### Task 6: Operation span for unacknowledged client bulk writes (`pymongo/asynchronous/client_bulk.py`)

**Files:**
- Modify: `pymongo/asynchronous/client_bulk.py` — `_AsyncClientBulk.execute()`
- Test: `test/asynchronous/test_otel.py` (integration, requires MongoDB 8.0+ for `bulk_write`)

**Interfaces:**
- Consumes: `_telemetry._OperationTelemetry` (Task 3).

- [ ] **Step 1: Write the failing integration test**

Add to `TestOTelSpans`:

```python
async def test_bulk_write_acknowledged_gets_operation_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    self.exporter.clear()
    await client.bulk_write(
        [InsertOne(namespace=f"{self.db.name}.test", document={"x": 1})]
    )
    matching = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.operation.name") == "bulkWrite"
    ]
    self.assertEqual(len(matching), 1)
    self.assertEqual(matching[0].attributes["db.namespace"], "admin")
    self.assertNotIn("db.collection.name", matching[0].attributes)


async def test_bulk_write_unacknowledged_gets_operation_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True}, w=0)
    self.exporter.clear()
    await client.bulk_write(
        [InsertOne(namespace=f"{self.db.name}.test", document={"x": 1})]
    )
    matching = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.operation.name") == "bulkWrite"
    ]
    self.assertEqual(len(matching), 1)
    self.assertEqual(matching[0].attributes["db.namespace"], "admin")
```

(Add `from pymongo.operations import InsertOne` to `test_otel.py`'s imports if not already present.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest test/asynchronous/test_otel.py -k "test_bulk_write_acknowledged_gets_operation_span or test_bulk_write_unacknowledged_gets_operation_span" -v`
Expected: The acknowledged test likely already PASSES (it flows through `_retry_internal` from Task 4 automatically — verify this; if it does, that's expected and requires no code change here, just confirms the design). The unacknowledged test FAILS with no matching span.

- [ ] **Step 3: Implement in `pymongo/asynchronous/client_bulk.py`**

Add `from pymongo._telemetry import _OperationTelemetry` to the imports if not already present.

Modify `execute()` (the unacknowledged branch only — leave the acknowledged branch untouched, it's already covered via Task 4):

```python
async def execute(
    self,
    session: Optional[AsyncClientSession],
    operation: str,
) -> Any:
    """Execute operations."""
    if not self.ops:
        raise InvalidOperation("No operations to execute")
    if self.executed:
        raise InvalidOperation("Bulk operations can only be executed once.")
    self.executed = True
    session = _validate_session_write_concern(session, self.write_concern)

    if not self.write_concern.acknowledged:
        operation_telemetry = _OperationTelemetry(
            self.client.options.tracing, operation, session
        )
        try:
            async with await self.client._conn_for_writes(
                session, operation
            ) as connection:
                if connection.max_wire_version < 25:
                    raise InvalidOperation(
                        "MongoClient.bulk_write requires MongoDB server version 8.0+."
                    )
                await self.execute_no_results(connection)
        except BaseException as exc:
            operation_telemetry.failed(exc)
            raise
        else:
            operation_telemetry.succeeded()
        return ClientBulkWriteResult(None, False, False)  # type: ignore[arg-type]

    result = await self.execute_command(session, operation)
    return ClientBulkWriteResult(
        result,
        self.write_concern.acknowledged,
        self.verbose_results,
    )
```

Since this path never reaches `_run_command`'s lazy-tagging (the unacknowledged wire send bypasses `_CommandTelemetry`/`start_command_span` entirely per the write-command construction in this file), the operation span's `db.namespace`/`db.operation.summary` need to be set explicitly rather than relying on backfill. Since `db_name = "admin"` is already hardcoded for `bulkWrite` in this file's acknowledged path (`_execute_command`, line 379) and the unacknowledged path sends the identical `bulkWrite` command shape, pass this statically: extend `_OperationTelemetry.__init__` is not needed — instead, set the two attributes directly on the handle's span right after creation, mirroring what the lazy tagging would have produced:

```
        if not self.write_concern.acknowledged:
            operation_telemetry = _OperationTelemetry(self.client.options.tracing, operation, session)
            if operation_telemetry._handle is not None:
                span = operation_telemetry._handle.span
                span.update_name(f"{operation} admin")
                span.set_attribute("db.namespace", "admin")
                span.set_attribute("db.operation.summary", f"{operation} admin")
            try:
                # ... rest of the existing try/except/else block from Step 3's
                # full replacement above, unchanged ...
```

(This reaches into `_handle`/`.span` directly rather than adding a new public parameter to `_OperationTelemetry`, since this is the *only* call site that needs to bypass the lazy-tagging mechanism — introducing a namespace-override parameter used exactly once isn't worth the added surface area. If a second such call site appears later, promote this to a proper keyword argument on `_OperationTelemetry.__init__` instead.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest test/asynchronous/test_otel.py -k "test_bulk_write_acknowledged_gets_operation_span or test_bulk_write_unacknowledged_gets_operation_span" -v`
Expected: PASS

- [ ] **Step 5: Run `just synchro`, full typing, and commit**

```bash
just synchro
just typing
git add pymongo/asynchronous/client_bulk.py pymongo/synchronous/client_bulk.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add operation span for unacknowledged client bulk writes"
```

---

### Task 7: Update `tracing` docstrings and changelog

**Files:**
- Modify: `pymongo/asynchronous/mongo_client.py:620-643`, `pymongo/synchronous/mongo_client.py:624-643` (kept in sync by `just synchro` — but since this is prose, not code, verify `just synchro` actually regenerates docstring text identically; if not, hand-edit both, matching the existing convention that these two files' docstrings are already identical)
- Modify: `doc/changelog.rst`

**Interfaces:** None (documentation only).

- [ ] **Step 1: Update the `tracing` option docstring**

In both `pymongo/asynchronous/mongo_client.py` and `pymongo/synchronous/mongo_client.py`, change:

```
          | **OpenTelemetry options:**
          | (Requires the ``opentelemetry-api`` package; install with the ``pymongo[opentelemetry]`` extra.)

          - `tracing`: (dict) Configuration for OpenTelemetry command spans, with keys:
```

to:

```
          | **OpenTelemetry options:**
          | (Requires the ``opentelemetry-api`` package; install with the ``pymongo[opentelemetry]`` extra.)

          - `tracing`: (dict) Configuration for OpenTelemetry command, operation, and
            transaction spans, with keys:
```

and add a new `.. versionchanged::` line after the existing one at the end of that section:

```
        .. versionchanged:: 4.18
           Added the ``tracing`` keyword argument.

        .. versionchanged:: 4.19
           The ``tracing`` option now also creates one span per public API call
           (nesting each call's command spans underneath) and a ``"transaction"``
           pseudo-span wrapping ``start_transaction()`` through
           ``commit_transaction()``/``abort_transaction()``.
```

(Confirm the actual next version number against `pymongo/_version.py` / the in-progress changelog section header at the time this task runs — it may not be 4.19 if other changes have landed first.)

- [ ] **Step 2: Add the changelog entry**

In `doc/changelog.rst`, find the in-progress version section (the one with the OpenTelemetry command-span bullet added by PYTHON-5945) and replace that bullet:

```
- Added optional OpenTelemetry command-span support, conforming to the
  `OpenTelemetry driver specification <https://github.com/mongodb/specifications/blob/master/source/open-telemetry/open-telemetry.md>`_.
  Enable it with the ``tracing`` :class:`~pymongo.mongo_client.MongoClient`
  option or the ``OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED`` environment
  variable. Install the ``opentelemetry-api`` package, or use the
  ``pymongo[opentelemetry]`` extra, to enable this feature.
```

with:

```
- Added optional OpenTelemetry command, operation, and transaction span
  support, conforming to the
  `OpenTelemetry driver specification <https://github.com/mongodb/specifications/blob/master/source/open-telemetry/open-telemetry.md>`_.
  Enable it with the ``tracing`` :class:`~pymongo.mongo_client.MongoClient`
  option or the ``OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED`` environment
  variable. Install the ``opentelemetry-api`` package, or use the
  ``pymongo[opentelemetry]`` extra, to enable this feature.
```

- [ ] **Step 3: Verify with `just typing` (docstrings are part of the Sphinx build, not mypy, but confirm nothing else broke)**

```bash
just typing
```
Expected: no new errors.

- [ ] **Step 4: Commit**

```bash
git add pymongo/asynchronous/mongo_client.py pymongo/synchronous/mongo_client.py doc/changelog.rst
git commit -m "PYTHON-5947 Update tracing docs for operation/transaction spans"
```

---

### Task 8: Unified test format wiring — `observeTracingMessages`/`expectTracingMessages`

**Files:**
- Modify: `test/asynchronous/unified_format.py` (`EntityMapUtil._create_entity`'s `client` branch at line 279-347; `_run_scenario` at line 1514-1566)
- Modify: `test/unified_format_shared.py` (new `TracingListenerUtil`-equivalent helper, alongside the existing `EventListenerUtil` at line 255)
- Test: the unified-format runner itself, exercised once Task 9 vendors real spec test files

**Interfaces:**
- Consumes: `pymongo._otel`'s `_shared_test_provider()`-style pattern (currently only in `test/asynchronous/test_otel.py` — extract it, see Step 1).
- Produces: `check_tracing_messages(operations, spec)` on the unified-format test case, and `observeTracingMessages` handling on client-entity creation — consumed by Task 9's vendored spec tests.

**This task's schema (the exact shape of `observeTracingMessages`/`expectTracingMessages` in the unified test format) is not fully available from the Jira ticket or its linked gist — the spec text fetched only describes operation/transaction span semantics, not the test-format JSON schema.** Do not guess field names. The first step below fetches the authoritative schema before writing any code.

- [ ] **Step 1: Fetch and read the actual unified-test-format tracing schema**

```bash
curl -sL https://raw.githubusercontent.com/mongodb/specifications/master/source/open-telemetry/tests/README.md -o /tmp/otel-tests-readme.md
curl -sL https://raw.githubusercontent.com/mongodb/specifications/master/source/unified-test-format/unified-test-format.md -o /tmp/utf.md
grep -n -i -A 40 "observeTracingMessages\|expectTracingMessages" /tmp/otel-tests-readme.md /tmp/utf.md
```

Read the matched sections in full (open the files directly, don't rely only on `grep` context) to get the exact field names, nesting structure for parent/child span assertions, and how `enableCommandPayload` (referenced in `test/asynchronous/test_otel.py`'s existing `TODO(PYTHON-5947)` comments) maps to client options. If the unified-test-format repo has changed shape and these files 404 or don't contain the expected sections, fall back to browsing `https://github.com/mongodb/specifications/tree/master/source/open-telemetry/tests` directly and adjust the fetch paths.

- [ ] **Step 2: Extract the shared `TracerProvider` helper out of `test/asynchronous/test_otel.py`**

Move `_shared_test_provider()` (currently at `test/asynchronous/test_otel.py:51-63`) into `test/unified_format_shared.py` (or `test/utils_shared.py` if that file already hosts other cross-suite test infra — check both before choosing) as a plain module-level function, and update `test_otel.py` to import it from there instead of defining it locally. Keep its exact current behavior (return the existing SDK `TracerProvider` if one's already registered, otherwise create and register one) — this is a pure move, not a rewrite.

- [ ] **Step 3: Add `observeTracingMessages` handling to `EntityMapUtil._create_entity`'s `client` branch**

Following the exact pattern of `observeEvents`/`EventListenerUtil` at `test/asynchronous/unified_format.py:281-313`, add (guided by whatever Step 1 found for the real field name — this sketch assumes `observeTracingMessages: {enableCommandPayload: bool}` per the gist's design note; adjust field names to match the real schema):

```python
observe_tracing = spec.get("observeTracingMessages")
if observe_tracing is not None:
    enable_payload = observe_tracing.get("enableCommandPayload", False)
    kwargs["tracing"] = {
        "enabled": True,
        # find.yml asserts db.query.text via exact match against the
        # full, untruncated command -- an effectively-unlimited
        # length avoids truncating and failing that assertion.
        "query_text_max_length": 1_000_000 if enable_payload else None,
    }
```

Register per-client span capture the same way `listener = EventListenerUtil(...)` is registered at line 305-312 — store something keyed by `spec["id"]` that `check_tracing_messages` (Step 4) can look up later. Since (per the gist's own investigated note) no span attribute identifies which client emitted it, and every spec test file has at most one client with `observeTracingMessages` active, a minimal `self._tracing_enabled_client_id = spec["id"]` on the entity map (or a small list, to fail loudly if a second one ever appears) is sufficient — don't build multi-client correlation machinery that nothing exercises yet.

- [ ] **Step 4: Add `check_tracing_messages` to the unified-format test case**

Modeled directly on `check_log_messages` (`test/asynchronous/unified_format.py:1411-1464`), but capturing spans from the shared `InMemorySpanExporter` instead of wrapping `assertLogs`:

```python
async def check_tracing_messages(self, operations, spec):
    exporter = self._tracing_exporter  # set up in asyncSetUp, see below
    exporter.clear()
    await self.run_operations(operations)
    finished_spans = exporter.get_finished_spans()

    for client in spec:
        # (Adjust this loop body once Step 1 confirms the real per-client
        # `spec["spans"]` / nesting schema; the structure below is a
        # starting sketch mirroring check_log_messages's shape.)
        expected_spans = client["spans"]
        self.assertTrue(expected_spans, "expectTracingMessages spans must be non-empty")
        # Reconstruct the parent/child tree from the flat exporter list via
        # each span's parent id, matching expected_spans' nested structure.
        ...
        for expected, actual in zip(expected_spans, finished_spans):
            self.match_evaluator.match_result(expected, actual)
```

Add the exporter setup to whatever this test class's `asyncSetUp` is (create an `InMemorySpanExporter`, register it via `SimpleSpanProcessor` on the shared provider from Step 2, store as `self._tracing_exporter`) — but only when at least one entity in this test's `createEntities` had `observeTracingMessages` (avoid registering exporters for every unified-format test in the suite; check how `EventListenerUtil` avoids similar overhead for tests with no `observeEvents`, and mirror that gating).

- [ ] **Step 5: Wire the `expectTracingMessages` branch into `_run_scenario`**

In `_run_scenario` (`test/asynchronous/unified_format.py:1514-1566`), add a branch alongside the existing `expectLogMessages` one (line 1550-1556):

```python
if "expectLogMessages" in spec:
    expect_log_messages = spec["expectLogMessages"]
    self.assertTrue(expect_log_messages, "expectEvents must be non-empty")
    await self.check_log_messages(spec["operations"], expect_log_messages)
elif "expectTracingMessages" in spec:
    expect_tracing_messages = spec["expectTracingMessages"]
    self.assertTrue(expect_tracing_messages, "expectTracingMessages must be non-empty")
    await self.check_tracing_messages(spec["operations"], expect_tracing_messages)
else:
    # process operations
    await self.run_operations(spec["operations"])
```

(Using `elif` mirrors today's mutual exclusivity between `expectLogMessages` and plain `run_operations`; revisit only if Step 1's real spec tests actually combine `expectLogMessages` and `expectTracingMessages` in the same test case, which today's `check_log_messages`/`check_events` split suggests is not how this test format composes.)

- [ ] **Step 6: Run `just synchro` and the existing unified-format suite for regressions**

```bash
just synchro
just typing
pytest test/test_crud_unified.py -v
```
Expected: PASS — confirms the new branches didn't break existing non-tracing unified-format tests.

- [ ] **Step 7: Commit**

```bash
git add test/asynchronous/unified_format.py test/unified_format.py test/unified_format_shared.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Wire observeTracingMessages/expectTracingMessages into the unified test format runner"
```

---

### Task 9: Vendor OpenTelemetry spec tests and drop redundant prose tests

**Files:**
- Modify: `.evergreen/resync-specs.sh`
- Create: `test/asynchronous/test_open_telemetry_unified.py` (+ mirror via `just synchro`)
- Create: vendored spec test data directory (exact path confirmed against the real `mongodb/specifications` layout in Step 1)
- Modify: `test/asynchronous/test_otel.py` — remove `test_span_created_for_insert_and_find`, `test_query_text_included_when_configured`, and the two `TODO(PYTHON-5947)`-tagged prose tests' now-superseded assertions

**Interfaces:** None new — this task consumes Task 8's `check_tracing_messages`/`expectTracingMessages` wiring by exercising it against real vendored spec data.

- [ ] **Step 1: Confirm the spec repo's OpenTelemetry test directory layout**

```bash
curl -sL "https://api.github.com/repos/mongodb/specifications/contents/source/open-telemetry/tests" | python3 -c "import json,sys; [print(f['name']) for f in json.load(sys.stdin)]"
```

Confirm there's a `unified` (or similarly named) subdirectory containing `find.yml`, `insert.yml`, `find_without_query_text.yml` (names referenced by the `TODO(PYTHON-5947)` comments already in `test/asynchronous/test_otel.py`) plus their generated `.json` counterparts.

- [ ] **Step 2: Add the `open-telemetry` vendoring case to `.evergreen/resync-specs.sh`**

Following the exact pattern of the `command-logging`/`clam` entries (existing lines ~117-129):

```bash
    open-telemetry|otel|open_telemetry)
      cpjson open-telemetry/tests/unified open_telemetry
      ;;
```

(Adjust the source subdirectory name — `open-telemetry/tests/unified` — if Step 1 found a different actual path.)

- [ ] **Step 3: Run the vendoring script and inspect the result**

```bash
.evergreen/resync-specs.sh open-telemetry
git status --short test/open_telemetry
```
Expected: a new `test/open_telemetry/` directory populated with `.json` files.

- [ ] **Step 4: Write the new unified-test-format runner file**

Create `test/asynchronous/test_open_telemetry_unified.py`, following the exact structure of another small unified-format runner in this repo (e.g. `test/asynchronous/test_command_logging.py` or `test/asynchronous/test_command_monitoring.py` — read whichever one is smallest as the concrete template, since this repo already has an established pattern for "one spec-driven unified-format test file per spec directory"):

```python
"""Test suite for the OpenTelemetry unified spec tests."""
from __future__ import annotations

import os
import sys

sys.path[0:0] = [""]

from test.asynchronous.unified_format import generate_test_classes

_IS_SYNC = False

TEST_PATH = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), os.path.join("..", "open_telemetry")
)

globals().update(
    generate_test_classes(
        TEST_PATH,
        module=__name__,
    )
)

if __name__ == "__main__":
    import unittest

    unittest.main()
```

(Match whatever `generate_test_classes` call signature the template file actually uses — some spec suites pass extra kwargs like `RUN_ON_SERVERLESS` or `expected_failures`; check the template before assuming this minimal form is sufficient.)

Mark it with the existing `otel` pytest marker at the top, matching `test_otel.py:48`'s `pytestmark = pytest.mark.otel`.

- [ ] **Step 5: Run the new spec suite**

Run: `pytest test/asynchronous/test_open_telemetry_unified.py -v`
Expected: PASS for every vendored test case. If any fail, the failure is either a real bug in Tasks 1-8 (fix it there) or a gap in Task 8's `check_tracing_messages`/`observeTracingMessages` schema-matching (go back and fix Task 8 using the now-concrete real test data as the schema reference, more reliable than Step 1's README).

- [ ] **Step 6: Remove the now-redundant prose tests**

In `test/asynchronous/test_otel.py`, delete `test_span_created_for_insert_and_find` and `test_query_text_included_when_configured` entirely (both already carry a `TODO(PYTHON-5947)` marking them as superseded by `insert.yml`/`find.yml`/`find_without_query_text.yml` from the now-vendored spec suite).

In `test_prose_1_tracing_enable_disable_via_env_var` and `test_prose_2_command_payload_emission_via_env_var`, resolve their `TODO(PYTHON-5947)` comments by extending the assertions to also check for the operation span (not just the command span) being absent/present, and to disambiguate via `db.operation.name` vs `db.command.name` as those TODOs specify — for example:

```python
async def test_prose_1_tracing_enable_disable_via_env_var(self):
    """Prose Test 1: Tracing Enable/Disable via Environment Variable."""
    with patch.dict(
        os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "false"}
    ):
        client = await self.async_rs_or_single_client()
        self.exporter.clear()
        await client.admin.command("ping")
    self.assertEqual(self.spans(), [])

    with patch.dict(
        os.environ, {"OTEL_PYTHON_INSTRUMENTATION_MONGODB_ENABLED": "true"}
    ):
        client = await self.async_rs_or_single_client()
        self.exporter.clear()
        await client.admin.command("ping")
    finished = self.exporter.get_finished_spans()
    self.assertIn("ping", [s.attributes.get("db.command.name") for s in finished])
    self.assertIn("ping", [s.attributes.get("db.operation.name") for s in finished])
```

(`client.admin.command("ping")` goes through `run_command`, not `_retryable_read`/`_retryable_write` — verify whether it actually produces an operation span at all; if `command()` doesn't route through `_retry_internal`, adjust this assertion to use a real CRUD call like `db.test.find_one()` instead, so both an operation and a command span genuinely exist to assert on.)

- [ ] **Step 7: Run the full otel test file one more time**

Run: `pytest test/asynchronous/test_otel.py -v`
Expected: All PASS.

- [ ] **Step 8: Run `just synchro`, `just lint-manual`, `just typing`, and commit**

```bash
just synchro
just lint-manual
just typing
git add .evergreen/resync-specs.sh test/open_telemetry test/asynchronous/test_open_telemetry_unified.py test/test_open_telemetry_unified.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Vendor OpenTelemetry unified spec tests and drop redundant prose tests"
```

---

## Final verification

- [ ] Run the complete otel suite end to end against a replica set (for transaction coverage):

```bash
MONGODB_VERSION=8.0 just run-server
just test test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py
```

- [ ] Run `just typing` and `just pre-commit` (per this repo's PR checklist in `AGENTS.md`) before opening a PR.
- [ ] Confirm `git diff main...HEAD -- pymongo/synchronous test/` is empty of hand-edits (everything there should be `just synchro` output only).

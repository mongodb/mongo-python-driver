# OTel Span Coverage Gaps Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the four OpenTelemetry span-coverage gaps deferred from PYTHON-5947: early-failure operation spans missing required attributes, `getMore` spans not nesting under their originating cursor operation, `killCursors`/`endSessions` having no operation span, and `with_transaction` producing sibling transaction spans instead of one enclosing span.

**Architecture:** Three mechanisms, applied across the four gaps. (1) `_OperationTelemetry`/`start_operation_span` gain optional `dbname`/`collection` parameters so spec-required attributes are set eagerly at span creation, threaded through `_retry_internal` from 25 call sites — the existing lazy backfill still overwrites them with authoritative values once a real command runs. (2) A new "detached" operation-span mode lets a caller own a span's lifetime across multiple `_retry_internal` calls, which is what makes cursor `getMore`s nest under their originating find/aggregate. (3) `_OperationTelemetry` is used directly as a context manager at the two call sites that legitimately bypass the retry machinery (`killCursors`/`endSessions`) and around `with_transaction`'s retry loop.

**Tech Stack:** Python, `opentelemetry-api` (optional dependency via the `pymongo[opentelemetry]` extra), `opentelemetry-sdk` (test-only, for `InMemorySpanExporter`).

## Global Constraints

- Only edit `pymongo/asynchronous/*`, `pymongo/_otel.py`, `pymongo/_telemetry.py`, `pymongo/cursor_shared.py`, and `test/asynchronous/*`; run `just synchro` to generate `pymongo/synchronous/*` and mirrored `test/*` files. Never hand-edit generated files.
- `pymongo/_otel.py` must stay the only module with a direct `opentelemetry` import; `pymongo/_telemetry.py` only calls into `pymongo._otel` functions.
- All span attributes must match the OTel driver spec exactly: `db.system.name` (always `"mongodb"`), `db.namespace`, `db.collection.name` (only when available), `db.operation.name`, `db.operation.summary` (same string as the span name).
- Operation span name format: `"{operation} {dbname}.{collection}"` if a collection applies, else `"{operation} {dbname}"` — reuse the existing `_build_query_summary()` helper in `pymongo/_otel.py`, don't reimplement it.
- Do NOT modify `requirements/opentelemetry.txt` or any other shipped requirements/dependency file. If you need `opentelemetry-sdk` locally, install it into the venv directly (`uv pip install opentelemetry-sdk`).
- Run `just typing` and `just lint-manual` before considering any task done. Every new integration test requires a live MongoDB replica set (already running at the default connection string, replica set `repl0`).
- The `otel` pytest marker is excluded by default `addopts`; use `-m otel` to select these tests.
- Existing regression test `TestOTelTracerCaching.test_start_command_span_does_not_call_get_tracer` must keep passing — never call `trace.get_tracer()` outside the module-level `_TRACER` cache.
- Verify at least once on Python 3.11+ (e.g. `uv run --python 3.13 --extra opentelemetry --extra test --with opentelemetry-sdk python -m pytest ...`), not only the default 3.10 venv — a prior Critical bug in this feature was invisible on 3.10.

---

## Current-state reference (verified; do not re-derive)

- `_OperationTelemetry.__init__(self, tracing_options, operation, session, is_run_command=False)` lives at `pymongo/_telemetry.py:290`. It sets `self.operation_name` and `self._handle`, and has `__slots__ = ("_handle", "operation_name")`. Methods: `succeeded()`, `failed(exc)`.
- `_otel.start_operation_span(tracing_options, operation, parent_span)` (`pymongo/_otel.py:339`) calls `_TRACER.start_as_current_span(...)`, immediately `__enter__`s it, sets the `_CURRENT_OPERATION_NAME` contextvar, and returns `_OperationSpanHandle(span, cm, name_token)`.
- `_otel.end_operation_span_success(handle)` / `end_operation_span_failure(handle, exc)` (`pymongo/_otel.py:371`, `:379`) reset the contextvar token and `__exit__` the cm.
- `start_command_span` (`pymongo/_otel.py`) already contains the lazy-backfill block that reads `_CURRENT_OPERATION_NAME`, then sets `db.namespace`/`db.collection.name`/`db.operation.summary` and calls `update_name(...)` on the ambient operation span. That block runs before the `_is_sensitive_command` early-return. **Leave it unchanged.**
- `_retry_internal` (`pymongo/asynchronous/mongo_client.py:2018`) constructs `_OperationTelemetry` at line 2048, then runs `_ClientConnectionRetryable(...).run()` in a `try/except BaseException/else`, calling `failed(exc)`/`succeeded()`.
- `_retryable_read` (`:2073`) and `_retryable_write` (`:2124`) wrap `_retry_internal` / `_retry_with_session`.
- `AsyncMongoClient._run_operation` (`:1940`) is the single entry point for both `_Query` and `_GetMore`; it calls `self._retryable_read(_cmd, ..., operation=operation.name)` at line 1978.
- `AsyncCursor._refresh()` (`pymongo/asynchronous/cursor.py:1044`) sends the initial `_Query` when `self._id is None` and a `_GetMore` when `self._id` is truthy, both via `self._send_message(...)` → `client._run_operation(...)`.
- `AsyncCommandCursor` (`pymongo/asynchronous/command_cursor.py:44`) receives an already-fetched first batch in `__init__`; only its `getMore`s go through `_send_message` → `_run_operation`.
- `_AsyncCursorBase.close()` → `_die_lock()` (`pymongo/asynchronous/cursor_base.py:190`, `:170`); `_AgnosticCursorBase.__del__()` → `_die_no_lock()` (`pymongo/cursor_shared.py:64`, `:118`). Both call `_prepare_to_die`.
- `with_transaction` (`pymongo/asynchronous/client_session.py:674-830`): bare outer `while True:` at line 777, `start_transaction(...)` at 786, `callback(self)` at 790, `abort_transaction()` at 795, inner commit-retry `while True:` at 809, `commit_transaction()` at 811. No existing wrapper around either loop.
- `commit_transaction` (`:875`) has an `elif state is _TxnState.COMMITTED:` branch that sets state back to `IN_PROGRESS` to retry a commit; its `finally` already ended and cleared `self._transaction.span`.
- `_kill_cursor_impl` (`pymongo/asynchronous/mongo_client.py:2252-2262`) parses `db, coll = namespace.split(".", 1)` then `await conn.command(db, spec, session=session, client=self)`.
- `_end_sessions` (`:1730-1750`) loops batches, calling `await conn.command("admin", spec, read_preference=read_pref, client=self)`.
- `_Op.KILL_CURSORS == "killCursors"` and `_Op.END_SESSIONS == "endSessions"` already exist in `pymongo/operations.py`.

---

### Task 1: Eager attributes and detached-mode operation spans in `_otel.py` / `_telemetry.py`

**Files:**
- Modify: `pymongo/_otel.py` (`start_operation_span`, `end_operation_span_success`, `end_operation_span_failure`; add `use_operation_span`)
- Modify: `pymongo/_telemetry.py` (`_OperationTelemetry`)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes: existing `_otel._OperationSpanHandle`, `_otel._build_query_summary(command_name, dbname, collection)`, `_otel._CURRENT_OPERATION_NAME`, `_otel._is_tracing_enabled`, `_otel._TRACER`.
- Produces, used by Tasks 2-7:
  - `_otel.start_operation_span(tracing_options, operation, parent_span, dbname=None, collection=None, set_current=True) -> Optional[_OperationSpanHandle]`
  - `_otel.use_operation_span(handle: Optional[_OperationSpanHandle]) -> ContextManager[None]`
  - `_OperationSpanHandle` gains a `_cm` that may be `None` (detached mode) and a new `operation_name: str` field.
  - `_OperationTelemetry(tracing_options, operation, session, is_run_command=False, dbname=None, collection=None, set_current=True)`; new attribute `handle` (public alias of `_handle`); new methods `use()` (returns the `use_operation_span` context manager) and `__enter__`/`__exit__` (context-manager protocol calling `succeeded()`/`failed(exc)`).

- [ ] **Step 1: Write the failing tests**

Add to `test/asynchronous/test_otel.py`, in the existing `TestOTelOperationSpanPrimitives` class (which already has `setUpClass` registering an `InMemorySpanExporter` on `_shared_test_provider()` and a `setUp` calling `self.exporter.clear()`):

```python
def test_eager_dbname_and_collection_set_at_creation(self):
    opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
    handle = _otel.start_operation_span(
        opts, "find", None, dbname="mydb", collection="mycoll"
    )
    self.assertIsNotNone(handle)
    _otel.end_operation_span_success(handle)
    (span,) = self.exporter.get_finished_spans()
    self.assertEqual(span.name, "find mydb.mycoll")
    self.assertEqual(span.attributes["db.namespace"], "mydb")
    self.assertEqual(span.attributes["db.collection.name"], "mycoll")
    self.assertEqual(span.attributes["db.operation.summary"], "find mydb.mycoll")
    self.assertEqual(span.attributes["db.operation.name"], "find")


def test_eager_dbname_without_collection_omits_collection_attribute(self):
    opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
    handle = _otel.start_operation_span(opts, "listCollections", None, dbname="mydb")
    _otel.end_operation_span_success(handle)
    (span,) = self.exporter.get_finished_spans()
    self.assertEqual(span.name, "listCollections mydb")
    self.assertEqual(span.attributes["db.operation.summary"], "listCollections mydb")
    self.assertNotIn("db.collection.name", span.attributes)


def test_no_eager_attributes_leaves_provisional_name(self):
    opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
    handle = _otel.start_operation_span(opts, "find", None)
    _otel.end_operation_span_success(handle)
    (span,) = self.exporter.get_finished_spans()
    self.assertEqual(span.name, "find")
    self.assertNotIn("db.namespace", span.attributes)


def test_detached_span_is_not_current_until_used(self):
    from opentelemetry import trace

    opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
    handle = _otel.start_operation_span(opts, "find", None, set_current=False)
    self.assertIsNotNone(handle)
    # Not current, and the operation-name contextvar is untouched.
    self.assertIsNot(trace.get_current_span(), handle.span)
    self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
    with _otel.use_operation_span(handle):
        self.assertIs(trace.get_current_span(), handle.span)
        self.assertEqual(_otel._CURRENT_OPERATION_NAME.get(), "find")
    # Restored afterwards, and the span is still open (not ended).
    self.assertIsNot(trace.get_current_span(), handle.span)
    self.assertIsNone(_otel._CURRENT_OPERATION_NAME.get())
    self.assertEqual(self.exporter.get_finished_spans(), ())
    _otel.end_operation_span_success(handle)
    (span,) = self.exporter.get_finished_spans()
    self.assertEqual(span.name, "find")


def test_detached_span_reused_across_multiple_use_blocks(self):
    opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
    handle = _otel.start_operation_span(opts, "find", None, set_current=False)
    for _ in range(3):
        with _otel.use_operation_span(handle):
            pass
    self.assertEqual(self.exporter.get_finished_spans(), ())
    _otel.end_operation_span_success(handle)
    self.assertEqual(len(self.exporter.get_finished_spans()), 1)


def test_use_operation_span_with_none_handle_is_noop(self):
    with _otel.use_operation_span(None):
        pass
    self.assertEqual(self.exporter.get_finished_spans(), ())
```

Add a second new class for the `_telemetry.py` side:

```python
class TestOperationTelemetryContextManager(unittest.TestCase):
    """Unit tests for _OperationTelemetry's context-manager and detached modes."""

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

    def test_context_manager_success_ends_span(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        with _OperationTelemetry(
            opts, "killCursors", None, dbname="mydb", collection="c"
        ):
            pass
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.name, "killCursors mydb.c")
        self.assertEqual(span.status.status_code, StatusCode.UNSET)

    def test_context_manager_failure_records_exception(self):
        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        with self.assertRaises(ValueError):
            with _OperationTelemetry(opts, "killCursors", None, dbname="mydb"):
                raise ValueError("boom")
        (span,) = self.exporter.get_finished_spans()
        self.assertEqual(span.status.status_code, StatusCode.ERROR)
        self.assertEqual(span.attributes["exception.type"], "ValueError")

    def test_context_manager_disabled_is_noop(self):
        with _OperationTelemetry(None, "killCursors", None, dbname="mydb"):
            pass
        self.assertEqual(self.exporter.get_finished_spans(), ())

    def test_detached_telemetry_use_makes_span_current(self):
        from opentelemetry import trace

        opts: _otel.TracingOptions = {"enabled": True, "query_text_max_length": None}
        telemetry = _OperationTelemetry(
            opts, "find", None, dbname="mydb", collection="c", set_current=False
        )
        self.assertIsNot(trace.get_current_span(), telemetry.handle.span)
        with telemetry.use():
            self.assertIs(trace.get_current_span(), telemetry.handle.span)
        self.assertEqual(self.exporter.get_finished_spans(), ())
        telemetry.succeeded()
        self.assertEqual(len(self.exporter.get_finished_spans()), 1)
```

Ensure `from pymongo._telemetry import _OperationTelemetry` and `from opentelemetry.trace import StatusCode` are imported in the test module (check the existing imports first — `StatusCode` is likely already imported for existing tests).

- [ ] **Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k "TestOTelOperationSpanPrimitives or TestOperationTelemetryContextManager" -v`
Expected: FAIL — `TypeError: start_operation_span() got an unexpected keyword argument 'dbname'`, `AttributeError: module 'pymongo._otel' has no attribute 'use_operation_span'`, and `TypeError: __init__() got an unexpected keyword argument 'dbname'`.

- [ ] **Step 3: Add the `contextlib` import and extend `_OperationSpanHandle` in `pymongo/_otel.py`**

Add `import contextlib` to the imports at the top of `pymongo/_otel.py` (alongside the existing `import os`, `import traceback`).

Find the existing `_OperationSpanHandle` class and change it so `_cm` is optional and the operation name is retained. Replace the whole class with:

```python
class _OperationSpanHandle:
    """Bundles an operation span with what's needed to end it later.

    ``_cm`` is the ``start_as_current_span`` context manager when the span was
    made current at creation, or None in detached mode (see
    ``start_operation_span``'s ``set_current``), where the span is made current
    per-use by ``use_operation_span`` instead.
    """

    __slots__ = ("_cm", "_name_token", "operation_name", "span")

    def __init__(
        self,
        span: Span,
        cm: Any,
        name_token: Any,
        operation_name: str,
    ) -> None:
        self.span = span
        self._cm = cm
        self._name_token = name_token
        self.operation_name = operation_name
```

- [ ] **Step 4: Rewrite `start_operation_span` in `pymongo/_otel.py`**

Replace the existing `start_operation_span` function body with:

```python
def start_operation_span(
    tracing_options: Optional[TracingOptions],
    operation: str,
    parent_span: Optional[Span],
    dbname: Optional[str] = None,
    collection: Optional[str] = None,
    set_current: bool = True,
) -> Optional[_OperationSpanHandle]:
    """Start a CLIENT-kind span for one logical operation, or None.

    Spans all retry attempts of one call to _retry_internal. When ``dbname`` is
    given, the spec-required ``db.namespace``/``db.operation.summary`` (and
    ``db.collection.name``, when ``collection`` is given) are set immediately,
    so an operation that fails before any command is ever built -- e.g. server
    selection timing out -- still produces a conformant span.
    ``start_command_span`` still backfills these from the real command once one
    is built, overwriting these values with the authoritative ones.

    ``parent_span`` (the active transaction span, if any) becomes this span's
    *explicit* parent; it is deliberately not read from ambient context, to
    avoid a concurrently-running unrelated session's operations picking up
    this transaction by accident. Pass None outside of a transaction.

    With ``set_current=False`` the span is created but not made current and the
    operation-name contextvar is left alone -- for spans whose lifetime spans
    several ``_retry_internal`` calls (cursor getMores), where the caller makes
    it current per-call via ``use_operation_span``.
    """
    if not _is_tracing_enabled(tracing_options):
        return None
    assert _TRACER is not None  # _is_tracing_enabled already checked _HAS_OPENTELEMETRY
    context = (
        trace.set_span_in_context(parent_span) if parent_span is not None else None
    )
    attributes: dict[str, Any] = {
        "db.system.name": "mongodb",
        "db.operation.name": operation,
    }
    name = operation
    if dbname:
        name = _build_query_summary(operation, dbname, collection)
        attributes["db.namespace"] = dbname
        attributes["db.operation.summary"] = name
        if collection:
            attributes["db.collection.name"] = collection
    if not set_current:
        span = _TRACER.start_span(
            name, kind=SpanKind.CLIENT, context=context, attributes=attributes
        )
        return _OperationSpanHandle(span, None, None, operation)
    cm = _TRACER.start_as_current_span(
        name,
        kind=SpanKind.CLIENT,
        context=context,
        attributes=attributes,
    )
    span = cm.__enter__()
    name_token = _CURRENT_OPERATION_NAME.set(operation)
    return _OperationSpanHandle(span, cm, name_token, operation)
```

- [ ] **Step 5: Add `use_operation_span` and update the two end functions in `pymongo/_otel.py`**

Add this function immediately after `start_operation_span`:

```python
@contextlib.contextmanager
def use_operation_span(handle: Optional[_OperationSpanHandle]) -> Iterator[None]:
    """Make a detached operation span current for the duration of the block.

    Does not end the span -- the owner (e.g. a cursor, across all of its
    getMore calls) ends it explicitly. A no-op when ``handle`` is None.
    """
    if handle is None:
        yield
        return
    token = _CURRENT_OPERATION_NAME.set(handle.operation_name)
    try:
        with trace.use_span(handle.span, end_on_exit=False):
            yield
    finally:
        _CURRENT_OPERATION_NAME.reset(token)
```

Add `Iterator` to the `typing`/`collections.abc` imports at the top of the file (`from collections.abc import Iterator, Mapping, MutableMapping` — check the existing import line and extend it).

Then update both end functions so they handle a `None` `_cm` (detached mode):

```python
def end_operation_span_success(handle: Optional[_OperationSpanHandle]) -> None:
    """End the operation span with no error status."""
    if handle is None:
        return
    if handle._cm is None:
        handle.span.end()
        return
    _CURRENT_OPERATION_NAME.reset(handle._name_token)
    handle._cm.__exit__(None, None, None)


def end_operation_span_failure(
    handle: Optional[_OperationSpanHandle], exc: BaseException
) -> None:
    """Record the exception, set the error status, and end the operation span."""
    if handle is None:
        return
    handle.span.record_exception(exc)
    _set_exception_attributes(handle.span, exc)
    handle.span.set_status(Status(StatusCode.ERROR, description=str(exc)))
    if handle._cm is None:
        handle.span.end()
        return
    _CURRENT_OPERATION_NAME.reset(handle._name_token)
    handle._cm.__exit__(None, None, None)
```

- [ ] **Step 6: Extend `_OperationTelemetry` in `pymongo/_telemetry.py`**

Replace the `_OperationTelemetry` class's `__slots__`, `__init__`, and add the new methods (keep `succeeded`/`failed` bodies as they are):

```python
class _OperationTelemetry:
    """One span-scoped context per logical operation (spanning all retry attempts).

    Construct once per call to ``_retry_internal``; call :meth:`succeeded` or
    :meth:`failed` exactly once when the operation's outcome is known, or use
    it as a context manager to do so automatically. A no-op throughout when
    tracing is disabled.

    With ``set_current=False`` the span is not made current at construction --
    for spans outliving one ``_retry_internal`` call (cursor getMores), where
    each call makes it current via :meth:`use`.
    """

    __slots__ = ("handle", "operation_name")

    def __init__(
        self,
        tracing_options: Optional[_otel.TracingOptions],
        operation: str,
        session: Optional[Any],
        is_run_command: bool = False,
        dbname: Optional[str] = None,
        collection: Optional[str] = None,
        set_current: bool = True,
    ) -> None:
        parent_span = None
        if session is not None and session.in_transaction:
            parent_span = session._transaction.span
        if is_run_command:
            otel_operation = _RUN_COMMAND_OPERATION_NAME
        else:
            name = _normalize_operation_name(operation)
            otel_operation = _OTEL_OPERATION_NAME_OVERRIDES.get(name, name)
        self.operation_name = otel_operation
        self.handle = _otel.start_operation_span(
            tracing_options,
            otel_operation,
            parent_span,
            dbname=dbname,
            collection=collection,
            set_current=set_current,
        )

    def use(self) -> Any:
        """Make this operation's span current for the duration of a block."""
        return _otel.use_operation_span(self.handle)

    def succeeded(self) -> None:
        _otel.end_operation_span_success(self.handle)

    def failed(self, exc: BaseException) -> None:
        _otel.end_operation_span_failure(self.handle, exc)

    def __enter__(self) -> _OperationTelemetry:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_val is None:
            self.succeeded()
        else:
            self.failed(exc_val)
```

Note the `_handle` → `handle` rename. Fix the one existing reader of the old name: `pymongo/asynchronous/client_bulk.py` (the unacknowledged-bulk-write branch added by PYTHON-5947's Task 6) references `operation_telemetry._handle` — change it to `operation_telemetry.handle`. Search for `._handle` across `pymongo/` and update every hit.

- [ ] **Step 7: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -v`
Expected: PASS — all new tests plus every pre-existing test in the file.

- [ ] **Step 8: Run synchro, typing, and lint**

```bash
just synchro
just typing
just lint-manual
```
Expected: all clean; `git status` shows the sync mirrors (`pymongo/synchronous/client_bulk.py`, `test/test_otel.py`) regenerated.

- [ ] **Step 9: Commit**

```bash
git add pymongo/_otel.py pymongo/_telemetry.py pymongo/asynchronous/client_bulk.py pymongo/synchronous/client_bulk.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add eager attributes and detached mode to operation spans"
```

---

### Task 2: Plumb `dbname`/`collection`/`operation_telemetry` through the retry layer

**Files:**
- Modify: `pymongo/asynchronous/mongo_client.py` (`_retry_internal`, `_retryable_read`, `_retryable_write`, `_retry_with_session`, `_run_operation`)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes from Task 1: `_OperationTelemetry(tracing_options, operation, session, is_run_command=False, dbname=None, collection=None, set_current=True)`, its `.use()` method, `.succeeded()`, `.failed(exc)`.
- Produces, used by Tasks 3-5:
  - `_retry_internal(func, session, bulk, operation, is_read=False, address=None, read_pref=None, retryable=False, operation_id=None, is_run_command=False, is_aggregate_write=False, dbname=None, collection=None, operation_telemetry=None)`
  - `_retryable_read(func, read_pref, session, operation, address=None, retryable=True, operation_id=None, is_run_command=False, is_aggregate_write=False, dbname=None, collection=None, operation_telemetry=None)`
  - `_retryable_write(retryable, func, session, operation, bulk=None, operation_id=None, dbname=None, collection=None)`
  - `_run_operation(operation, run_with_conn, address=None, operation_telemetry=None)`
  - Semantics: when `operation_telemetry` is passed, `_retry_internal` does NOT create or end a span — it makes the given one current for the call via `.use()` and leaves its lifecycle to the caller.

- [ ] **Step 1: Write the failing test**

Add to `test/asynchronous/test_otel.py` in the existing `TestOTelSpans` class (which has `self.exporter`, a `self.spans(name=None)` helper, and uses `self.async_rs_or_single_client(...)`):

```python
async def test_operation_span_has_namespace_when_no_command_is_sent(self):
    # An operation that fails during server selection never builds a
    # command, so the lazy backfill never runs -- the eagerly-set
    # namespace/summary attributes are the only ones it will ever have.
    client = await self.async_rs_or_single_client(
        "mongodb://localhost:1/",
        tracing={"enabled": True},
        serverSelectionTimeoutMS=10,
        connect=False,
    )
    self.exporter.clear()
    with self.assertRaises(ServerSelectionTimeoutError):
        await client.mydb.mycoll.find_one({})
    (span,) = [
        s for s in self.exporter.get_finished_spans() if s.name.startswith("find")
    ]
    self.assertEqual(span.name, "find mydb.mycoll")
    self.assertEqual(span.attributes["db.namespace"], "mydb")
    self.assertEqual(span.attributes["db.collection.name"], "mycoll")
    self.assertEqual(span.attributes["db.operation.summary"], "find mydb.mycoll")
    self.assertEqual(span.status.status_code, StatusCode.ERROR)


async def test_caller_owned_operation_telemetry_is_not_ended_by_retry_internal(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    telemetry = _OperationTelemetry(
        client.options.tracing,
        "find",
        None,
        dbname="mydb",
        collection="c",
        set_current=False,
    )
    self.exporter.clear()

    async def _noop_read(_session, _server, _conn, _read_pref):
        return "ok"

    result = await client._retryable_read(
        _noop_read,
        ReadPreference.PRIMARY,
        None,
        operation="find",
        operation_telemetry=telemetry,
    )
    self.assertEqual(result, "ok")
    # _retry_internal must not have ended the caller's span.
    self.assertEqual(
        [s for s in self.exporter.get_finished_spans() if s.name.startswith("find")], []
    )
    telemetry.succeeded()
    self.assertEqual(
        len(
            [s for s in self.exporter.get_finished_spans() if s.name.startswith("find")]
        ),
        1,
    )
```

Ensure `ServerSelectionTimeoutError`, `ReadPreference`, `StatusCode`, and `_OperationTelemetry` are imported in the test module (add whichever are missing).

- [ ] **Step 2: Run the test to verify it fails**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k "no_command_is_sent or caller_owned" -v`
Expected: FAIL — `TypeError: _retryable_read() got an unexpected keyword argument 'operation_telemetry'`, and the namespace test fails with `KeyError: 'db.namespace'` (or the span name is the bare `"find"`).

- [ ] **Step 3: Rewrite `_retry_internal` in `pymongo/asynchronous/mongo_client.py`**

Replace the signature and body (currently lines 2018-2071) with:

```python
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
    dbname: Optional[str] = None,
    collection: Optional[str] = None,
    operation_telemetry: Optional[_OperationTelemetry] = None,
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
    :param dbname: The database this operation targets, for the operation span's
        ``db.namespace``, defaults to None
    :param collection: The collection this operation targets, for the operation
        span's ``db.collection.name``, defaults to None
    :param operation_telemetry: A caller-owned operation span outliving this call
        (a cursor's, shared by its getMores). When given, this method neither
        creates nor ends a span -- it only makes the caller's current for this
        call. Defaults to None, meaning this method owns a fresh span.

    :return: Output of the calling func()
    """
    if operation_telemetry is not None:
        with operation_telemetry.use():
            return await _ClientConnectionRetryable(
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

    owned_telemetry = _OperationTelemetry(
        self.options.tracing,
        operation,
        session,
        is_run_command=is_run_command,
        dbname=dbname,
        collection=collection,
    )
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
        owned_telemetry.failed(exc)
        raise
    else:
        owned_telemetry.succeeded()
        return result
```

- [ ] **Step 4: Add the passthrough parameters to `_retryable_read`, `_retryable_write`, and `_retry_with_session`**

In `_retryable_read` (currently line 2073), add these three parameters to the signature after `is_aggregate_write: bool = False,`:

```python
dbname: Optional[str] = (None,)
collection: Optional[str] = (None,)
operation_telemetry: Optional[_OperationTelemetry] = (None,)
```

and add these three arguments to its `self._retry_internal(...)` call:

```python
dbname = (dbname,)
collection = (collection,)
operation_telemetry = (operation_telemetry,)
```

In `_retryable_write` (currently line 2124), add after `operation_id: Optional[int] = None,`:

```python
dbname: Optional[str] = (None,)
collection: Optional[str] = (None,)
```

and change its body's call to pass them through:

```python
async with self._tmp_session(session) as s:
    return await self._retry_with_session(
        retryable,
        func,
        s,
        bulk,
        operation,
        operation_id,
        dbname=dbname,
        collection=collection,
    )
```

Then find `_retry_with_session` (search `def _retry_with_session` in the same file), add the same two parameters to its signature, and pass them through to its own `_retry_internal(...)` call as `dbname=dbname, collection=collection`.

- [ ] **Step 5: Add `operation_telemetry` passthrough to `_run_operation`**

In `_run_operation` (currently line 1940), add a parameter after `address: Optional[_Address] = None,`:

```python
operation_telemetry: Optional[_OperationTelemetry] = (None,)
```

document it in the docstring:

```
        :param operation_telemetry: The cursor's caller-owned operation span, shared
            across its initial query and every getMore, or None.
```

and add `operation_telemetry=operation_telemetry,` to the `self._retryable_read(...)` call at the end of the method (currently line 1978).

Note the early-return branch at the top of `_run_operation` (`if operation.conn_mgr:`) bypasses `_retryable_read` entirely for exhaust/pinned cursors; wrap its `return await run_with_conn(...)` in `with _otel.use_operation_span(operation_telemetry.handle if operation_telemetry else None):` so exhaust-cursor `getMore` command spans still nest. Add `from pymongo import _otel` to this module's imports if not already present.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -v`
Expected: PASS, including the two new tests.

- [ ] **Step 7: Run synchro, typing, lint, and a regression slice**

```bash
just synchro
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_collection.py test/asynchronous/test_cursor.py -q
```
Expected: all clean; the collection/cursor suites pass (they exercise `_retry_internal` heavily).

- [ ] **Step 8: Commit**

```bash
git add pymongo/asynchronous/mongo_client.py pymongo/synchronous/mongo_client.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Plumb dbname/collection and caller-owned spans through the retry layer"
```

---

### Task 3: Thread `dbname`/`collection` through all 25 call sites

**Files:**
- Modify: `pymongo/asynchronous/collection.py` (15 call sites), `pymongo/asynchronous/database.py` (6), `pymongo/asynchronous/bulk.py` (1), `pymongo/asynchronous/client_bulk.py` (1), `pymongo/asynchronous/client_session.py` (1), `pymongo/asynchronous/change_stream.py` (1)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes from Task 2: the `dbname=`/`collection=` keyword arguments on `_retryable_read`, `_retryable_write`, and `_retry_internal`.
- Produces: nothing new — this task only passes existing arguments at existing call sites.

- [ ] **Step 1: Write the failing test**

Add to `test/asynchronous/test_otel.py`'s `TestOTelSpans`:

```python
async def test_eager_namespace_for_collection_and_database_operations(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    db = client.pymongo_test
    cases = [
        # (coroutine factory, expected span name)
        (lambda: db.mycoll.insert_one({"x": 1}), "insert pymongo_test.mycoll"),
        (lambda: db.mycoll.find_one({}), "find pymongo_test.mycoll"),
        (lambda: db.mycoll.count_documents({}), "count pymongo_test.mycoll"),
        (lambda: db.list_collection_names(), "listCollections pymongo_test"),
    ]
    for factory, expected_name in cases:
        with self.subTest(expected_name=expected_name):
            self.exporter.clear()
            await factory()
            names = [s.name for s in self.exporter.get_finished_spans()]
            self.assertIn(expected_name, names)
            (span,) = [
                s for s in self.exporter.get_finished_spans() if s.name == expected_name
            ]
            self.assertEqual(span.attributes["db.operation.summary"], expected_name)
            self.assertEqual(span.attributes["db.namespace"], "pymongo_test")
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k eager_namespace_for_collection -v`
Expected: FAIL — `AssertionError: 'insert pymongo_test.mycoll' not found in [...]`, because without eager attributes the span is named from the lazy backfill's timing and the assertion on the very first subTest fails.

(The lazy backfill does produce these same names on success paths, so if this test unexpectedly passes before implementation, note it in your report and rely on Task 2's `test_operation_span_has_namespace_when_no_command_is_sent` as the failing-first test for this behavior instead.)

- [ ] **Step 3: Update the 15 `collection.py` call sites**

`self` is an `AsyncCollection` at every one of these. Add `dbname=`/`collection=` keyword arguments to each call, using the expression noted per line:

| Line | Method | Add |
|---|---|---|
| ~662 | `_create_helper` | `dbname=self._database.name, collection=name` |
| ~824 | `_insert_one` | `dbname=self._database.name, collection=self.name` |
| ~1111 | `_update_retryable` | `dbname=self._database.name, collection=self.name` |
| ~1580 | `_delete_retryable` | `dbname=self._database.name, collection=self.name` |
| ~2172 | `_retryable_non_cursor_read` | `dbname=self._database.name, collection=self._name` |
| ~2269 | `_create_indexes` | `dbname=self._database.name, collection=self.name` |
| ~2504 | `_drop_index` | `dbname=self._database.name, collection=self._name` |
| ~2587 | `_list_indexes` | `dbname=self._database.name, collection=self._name` |
| ~2686 | `list_search_indexes` | `dbname=self._database.name, collection=self.name` |
| ~2781 | `_create_search_indexes` | `dbname=self._database.name, collection=self.name` |
| ~2823 | `drop_search_index` | `dbname=self._database.name, collection=self._name` |
| ~2865 | `update_search_index` | `dbname=self._database.name, collection=self._name` |
| ~2935 | `_aggregate` | `dbname=self._database.name, collection=self._name` |
| ~3161 | `rename` | `dbname=self._database.name, collection=self.name` |
| ~3321 | `_find_and_modify` | `dbname=self._database.name, collection=self.name` |

Line numbers are approximate — locate each by its enclosing method name. Note `_create_helper`'s `collection=name` uses the local `name` parameter (which may be an ESC/ECOC state-collection name, not `self._name`), and several methods use `self._name` vs `self.name` — these are equivalent (`name` is a property returning `_name`); match whichever the surrounding code already uses.

- [ ] **Step 4: Update the 6 `database.py` call sites**

`self` is an `AsyncDatabase`; there is never a single target collection except in `_drop_helper`:

| Line | Method | Add |
|---|---|---|
| ~711 | `aggregate` | `dbname=self.name` |
| ~946 | `command` | `dbname=self.name` |
| ~1054 | `cursor_command` | `dbname=self.name` |
| ~1080 | `_retryable_read_command` | `dbname=self.name` |
| ~1152 | `_list_collections_helper` | `dbname=self.name` |
| ~1271 | `_drop_helper` | `dbname=self.name, collection=name` |

- [ ] **Step 5: Update the remaining 4 call sites**

In `pymongo/asynchronous/bulk.py`, `_AsyncBulk.execute_command` (~line 474) — `self.collection` is an `AsyncCollection`:

```python
dbname = (self.collection.database.name,)
collection = (self.collection.name,)
```

In `pymongo/asynchronous/client_bulk.py`, `_AsyncClientBulk.execute_command` (~line 548) — a client-level bulk write spans namespaces, so the command targets `admin` with no single collection:

```python
dbname = ("admin",)
```

In `pymongo/asynchronous/client_session.py`, `_finish_transaction_with_retry` (~line 965) — `commitTransaction`/`abortTransaction` always run against `admin`:

```python
return await self._client._retry_internal(
    func, self, None, retryable=True, operation=command_name, dbname="admin"
)
```

In `pymongo/asynchronous/change_stream.py`, `_run_aggregation_cmd` (~line 253) — the watch target varies by subclass, so add a small helper to the base `AsyncChangeStream` class and use it at the call site:

```python
def _target_namespace(self) -> tuple[Optional[str], Optional[str]]:
    """Return (dbname, collection) for the watched target, for span attributes."""
    target = self._target
    if isinstance(target, AsyncCollection):
        return target.database.name, target.name
    if isinstance(target, AsyncDatabase):
        return target.name, None
    return None, None
```

At the call site:

```
        dbname, collname = self._target_namespace()
        ...
            dbname=dbname,
            collection=collname,
```

`AsyncCollectionChangeStream`'s target is an `AsyncCollection`; `AsyncDatabaseChangeStream`'s (and its `AsyncClusterChangeStream` subclass's) is an `AsyncDatabase` — `AsyncClusterChangeStream` is always constructed with `target=client.admin`, so it correctly yields `("admin", None)`. Add whatever `AsyncCollection`/`AsyncDatabase` imports the `isinstance` checks need; if importing them at module scope would create a circular import, do the checks inside `TYPE_CHECKING`-safe order by testing for the attribute instead:

```python
target = self._target
database = getattr(target, "database", None)
if database is not None:  # an AsyncCollection
    return database.name, target.name
name = getattr(target, "name", None)
if name is not None:  # an AsyncDatabase
    return name, None
return None, None
```

Prefer the `isinstance` version if imports allow it; fall back to the attribute version if they don't. Note in your report which you used and why.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py -m otel -v`
Expected: PASS — including the vendored spec suite, which asserts exact span names and attributes for most of these operations.

- [ ] **Step 7: Run synchro, typing, lint, and a broad regression slice**

```bash
just synchro
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_collection.py test/asynchronous/test_database.py test/asynchronous/test_bulk.py test/asynchronous/test_client_bulk_write.py test/asynchronous/test_change_stream.py test/asynchronous/test_transactions.py -q
```
Expected: all clean. This task touches 6 widely-used driver files, so the regression slice matters more here than elsewhere.

- [ ] **Step 8: Commit**

```bash
git add pymongo/asynchronous pymongo/synchronous test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Thread dbname/collection to operation spans from all call sites"
```

---

### Task 4: Nest `find` cursor getMores under one operation span

**Files:**
- Modify: `pymongo/asynchronous/cursor.py` (`_refresh`, `_send_message`), `pymongo/asynchronous/cursor_base.py` (`_die_lock`), `pymongo/cursor_shared.py` (`_AgnosticCursorBase` class attributes, `_die_no_lock`)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes from Tasks 1-2: `_OperationTelemetry(..., dbname=, collection=, set_current=False)`, its `.succeeded()`/`.failed(exc)`; `_run_operation(operation, run_with_conn, address=None, operation_telemetry=None)`.
- Produces, used by Task 5: `_AgnosticCursorBase._operation_telemetry: Optional[Any]` (declared on the shared base, defaulting to `None`) and `_AgnosticCursorBase._end_operation_telemetry(exc: Optional[BaseException] = None) -> None`, which ends the span exactly once and is idempotent.

- [ ] **Step 1: Write the failing test**

Add to `test/asynchronous/test_otel.py`'s `TestOTelSpans`:

```python
async def test_find_getmores_nest_under_one_operation_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client.pymongo_test.getmore_nesting
    await coll.drop()
    await coll.insert_many([{"i": i} for i in range(10)])
    self.exporter.clear()

    docs = await coll.find({}, batch_size=2).to_list()
    self.assertEqual(len(docs), 10)

    finished = self.exporter.get_finished_spans()
    # Exactly one operation span for the whole cursor.
    find_op_spans = [
        s
        for s in finished
        if s.attributes.get("db.operation.name") == "find"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(len(find_op_spans), 1, [s.name for s in finished])
    op_span = find_op_spans[0]
    self.assertEqual(op_span.name, "find pymongo_test.getmore_nesting")

    # No getMore *operation* spans at all.
    getmore_op_spans = [
        s
        for s in finished
        if s.attributes.get("db.operation.name") == "getMore"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(getmore_op_spans, [])

    # Every getMore *command* span is a child of that one operation span.
    getmore_cmd_spans = [
        s for s in finished if s.attributes.get("db.command.name") == "getMore"
    ]
    self.assertGreater(len(getmore_cmd_spans), 1)
    for cmd_span in getmore_cmd_spans:
        self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)


async def test_abandoned_cursor_still_ends_operation_span(self):
    import gc

    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client.pymongo_test.getmore_abandoned
    await coll.drop()
    await coll.insert_many([{"i": i} for i in range(10)])
    self.exporter.clear()

    cursor = coll.find({}, batch_size=2)
    await cursor.next()  # Leaves the cursor open with batches pending.
    del cursor
    gc.collect()

    find_op_spans = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.operation.name") == "find"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(len(find_op_spans), 1)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k "getmores_nest or abandoned_cursor" -v`
Expected: FAIL on the first test with `AssertionError: 5 != 1` (or similar) — today each `getMore` produces its own sibling operation span; and FAIL on the second because no span is ever ended for the abandoned cursor.

- [ ] **Step 3: Add the shared lifecycle helper in `pymongo/cursor_shared.py`**

In `_AgnosticCursorBase`, add `_operation_telemetry` to the class-level attribute declarations (which already list `_collection`, `_id`, `_data`, `_address`, `_sock_mgr`, `_session`, `_killed`):

```python
_operation_telemetry: Optional[Any] = None
```

Add `Any` to the `typing` imports on that file if it isn't already imported.

Add this method to the same class:

```python
def _end_operation_telemetry(self, exc: Optional[BaseException] = None) -> None:
    """End this cursor's operation span, exactly once.

    The span covers the cursor's whole lifetime -- its initial query plus
    every getMore -- so it is ended by whichever comes first: exhaustion,
    an explicit close(), or __del__ for an abandoned cursor. Idempotent, so
    all three paths can call it unconditionally.
    """
    telemetry = self._operation_telemetry
    if telemetry is None:
        return
    self._operation_telemetry = None
    if exc is None:
        telemetry.succeeded()
    else:
        telemetry.failed(exc)
```

Then call it from `_die_no_lock`, immediately after the `already_killed` `AttributeError` guard returns (so a partially-initialized cursor is still skipped) and before `_prepare_to_die`:

```python
self._end_operation_telemetry()
```

- [ ] **Step 4: Call the helper from the async close path in `pymongo/asynchronous/cursor_base.py`**

In `_die_lock`, add the same call in the same position — after the `already_killed` guard, before `_prepare_to_die`:

```python
self._end_operation_telemetry()
```

- [ ] **Step 5: Create and use the telemetry in `pymongo/asynchronous/cursor.py`**

Add the import at the top of the file:

```python
from pymongo._telemetry import _OperationTelemetry
```

In `_refresh`, inside the `if self._id is None:` branch (the initial query), create the detached telemetry just before `await self._send_message(q)`:

```python
client = self._collection.database.client
self._operation_telemetry = _OperationTelemetry(
    client.options.tracing,
    q.name,
    self._session,
    dbname=self._collection.database.name,
    collection=self._collection.name,
    set_current=False,
)
await self._send_message(q)
```

In `_send_message`, pass it into `_run_operation` — change the existing call:

```python
response = await client._run_operation(
    operation,
    self._run_with_conn,
    address=self._address,
    operation_telemetry=self._operation_telemetry,
)
```

`_send_message`'s existing exception handlers all end with `await self.close()` or `self._die_no_lock()`, both of which now end the span via Steps 3-4 — but they end it as a *success*. Record the failure instead by ending it explicitly at the top of each handler, before the existing cleanup. In the `except OperationFailure as exc:` handler add `self._end_operation_telemetry(exc)` as the first statement; in `except ConnectionFailure:` change to `except ConnectionFailure as exc:` and add `self._end_operation_telemetry(exc)`; in `except BaseException:` change to `except BaseException as exc:` and add `self._end_operation_telemetry(exc)`. Because `_end_operation_telemetry` is idempotent, the later `close()`/`_die_no_lock()` calls become no-ops for the span.

The success path needs nothing extra: `_send_message` already calls `await self.close()` when `self._id == 0` (exhausted) or when the limit is reached, and `close()` now ends the span.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py -m otel -v`
Expected: PASS. Note the vendored `find.json`/`retries.json` spec fixtures also assert cursor span structure — if any of them now fail, the fixture is the authority: report the mismatch rather than adjusting the fixture.

- [ ] **Step 7: Run synchro, typing, lint, and a cursor regression slice**

```bash
just synchro
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_cursor.py test/asynchronous/test_collection.py -q
```
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add pymongo/cursor_shared.py pymongo/asynchronous/cursor.py pymongo/asynchronous/cursor_base.py pymongo/synchronous test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Nest find cursor getMores under one operation span"
```

---

### Task 5: Nest command-cursor getMores under their originating operation span

**Files:**
- Modify: `pymongo/asynchronous/command_cursor.py` (`_send_message`), `pymongo/asynchronous/aggregation.py` (`get_cursor`), `pymongo/asynchronous/collection.py` (`_list_indexes`, `list_search_indexes`), `pymongo/asynchronous/database.py` (`cursor_command`, `_list_collections_helper`), `pymongo/asynchronous/mongo_client.py` (`list_databases`), `pymongo/asynchronous/client_bulk.py` (the `AsyncCommandCursor` construction)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes from Task 4: `_AgnosticCursorBase._operation_telemetry`, `_end_operation_telemetry(exc=None)` (both already handle the close/`__del__`/idempotency concerns).
- Consumes from Task 2: `_run_operation(..., operation_telemetry=None)`, and `_retryable_read(..., operation_telemetry=...)`.
- Produces: nothing new.

The mechanism differs from Task 4 because an `AsyncCommandCursor`'s first batch is fetched *before* the cursor exists — inside the `_retryable_read` call whose span must stay open. So each constructing method creates the detached telemetry first, passes it into `_retryable_read` (which keeps it open, per Task 2), and attaches it to the cursor it gets back.

- [ ] **Step 1: Write the failing test**

Add to `test/asynchronous/test_otel.py`'s `TestOTelSpans`:

```python
async def test_aggregate_getmores_nest_under_one_operation_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client.pymongo_test.agg_nesting
    await coll.drop()
    await coll.insert_many([{"i": i} for i in range(10)])
    self.exporter.clear()

    docs = await (await coll.aggregate([{"$match": {}}], batchSize=2)).to_list()
    self.assertEqual(len(docs), 10)

    finished = self.exporter.get_finished_spans()
    agg_op_spans = [
        s
        for s in finished
        if s.attributes.get("db.operation.name") == "aggregate"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(len(agg_op_spans), 1, [s.name for s in finished])
    op_span = agg_op_spans[0]

    getmore_op_spans = [
        s
        for s in finished
        if s.attributes.get("db.operation.name") == "getMore"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(getmore_op_spans, [])

    getmore_cmd_spans = [
        s for s in finished if s.attributes.get("db.command.name") == "getMore"
    ]
    self.assertGreater(len(getmore_cmd_spans), 1)
    for cmd_span in getmore_cmd_spans:
        self.assertEqual(cmd_span.parent.span_id, op_span.context.span_id)
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k aggregate_getmores_nest -v`
Expected: FAIL with `AssertionError: [...] != []` on the `getmore_op_spans` assertion — today each command-cursor `getMore` creates its own operation span.

- [ ] **Step 3: Pass the cursor's telemetry into `_run_operation` in `pymongo/asynchronous/command_cursor.py`**

In `_send_message`, change the `_run_operation` call to:

```python
response = await client._run_operation(
    operation,
    self._run_with_conn,
    address=self._address,
    operation_telemetry=self._operation_telemetry,
)
```

Then, exactly as in Task 4, record failures on the way out: in `except OperationFailure as exc:` add `self._end_operation_telemetry(exc)` as the first statement; change `except ConnectionFailure:` to `except ConnectionFailure as exc:` and `except Exception:` to `except Exception as exc:`, adding `self._end_operation_telemetry(exc)` as the first statement of each.

- [ ] **Step 4: Attach the telemetry at the `aggregate` construction site**

`_AggregationCommand.get_cursor` (`pymongo/asynchronous/aggregation.py`) runs *inside* the `_retryable_read` callback, so the operation span is already open and current there — but the telemetry object itself isn't reachable from that scope. Rather than plumbing it into the callback, attach it in the caller after `_retryable_read` returns.

In `pymongo/asynchronous/collection.py`'s `_aggregate` method (the ~line 2935 call site updated in Task 3), replace the `return await self._database.client._retryable_read(...)` (or equivalent assignment) with:

```python
operation_telemetry = _OperationTelemetry(
    self._database.client.options.tracing,
    _Op.AGGREGATE,
    session,
    dbname=self._database.name,
    collection=self._name,
    set_current=False,
)
try:
    cmd_cursor = await self._database.client._retryable_read(
        cmd.get_cursor,
        cmd.get_read_preference(session),  # type: ignore[arg-type]
        session,
        retryable=not cmd._performs_write,
        operation=_Op.AGGREGATE,
        is_aggregate_write=cmd._performs_write,
        dbname=self._database.name,
        collection=self._name,
        operation_telemetry=operation_telemetry,
    )
except BaseException as exc:
    operation_telemetry.failed(exc)
    raise
cmd_cursor._operation_telemetry = operation_telemetry
return cmd_cursor
```

Match the existing argument list exactly — read the current call and keep every argument it already passes, only adding `operation_telemetry=` (and the `dbname=`/`collection=` from Task 3). Add `from pymongo._telemetry import _OperationTelemetry` to the file's imports.

If the cursor is already exhausted in its first batch (`cursor["id"] == 0`), `AsyncCommandCursor.__init__` calls `self._end_session()` but not `close()`, so nothing has ended the span yet — assigning it after construction is still correct, and it will be ended by the first `close()`/`__del__`. Confirm this by checking that `test_aggregate_getmores_nest_under_one_operation_span` isn't the only aggregate test passing; the vendored `aggregate.json` fixture covers the single-batch case.

- [ ] **Step 5: Apply the identical pattern at the other command-cursor construction sites**

The same wrap-and-attach applies to each remaining method that builds an `AsyncCommandCursor` from a `_retryable_read`/`_retryable_write` result. For each, create the detached `_OperationTelemetry` before the call with that site's `dbname`/`collection` (as established in Task 3), pass `operation_telemetry=`, call `.failed(exc)` on `BaseException`, and assign `cursor._operation_telemetry = operation_telemetry` before returning:

| File | Method | operation | dbname / collection |
|---|---|---|---|
| `collection.py` | `_list_indexes` | `_Op.LIST_INDEXES` | `self._database.name` / `self._name` |
| `collection.py` | `list_search_indexes` | `_Op.LIST_SEARCH_INDEX` | `self._database.name` / `self.name` |
| `database.py` | `aggregate` | `_Op.AGGREGATE` | `self.name` / `None` |
| `database.py` | `cursor_command` | `command_name` | `self.name` / `None` |
| `database.py` | `_list_collections_helper` | `_Op.LIST_COLLECTIONS` | `self.name` / `None` |
| `mongo_client.py` | `list_databases` | `_Op.LIST_DATABASES` | `"admin"` / `None` |
| `client_bulk.py` | the `AsyncCommandCursor` construction (~line 332) | `_Op.BULK_WRITE` | `"admin"` / `None` |

For `database.py`'s `aggregate` and `client_bulk.py`, the cursor may be constructed inside a helper (`_AggregationCommand.get_cursor` / the bulk result path) rather than directly in the method — in that case still attach after the `_retryable_read`/`_retryable_write` call returns the cursor. If any site's structure makes this genuinely impossible (e.g. the cursor never surfaces to the calling method), stop and report it rather than restructuring the method.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py -m otel -v`
Expected: PASS, including the vendored `aggregate.json`, `list_indexes.json`, `list_collections.json`, `list_databases.json`, and `atlas_search.json` fixtures.

- [ ] **Step 7: Run synchro, typing, lint, and a regression slice**

```bash
just synchro
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_collection.py test/asynchronous/test_database.py test/asynchronous/test_client.py test/asynchronous/test_change_stream.py test/asynchronous/test_client_bulk_write.py -q
```
Expected: all clean.

- [ ] **Step 8: Commit**

```bash
git add pymongo/asynchronous pymongo/synchronous test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Nest command cursor getMores under their operation span"
```

---

### Task 6: Operation spans for `killCursors` and `endSessions`

**Files:**
- Modify: `pymongo/asynchronous/mongo_client.py` (`_kill_cursor_impl`, `_end_sessions`)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes from Task 1: `_OperationTelemetry` used directly as a context manager, with `dbname=`/`collection=`.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Add to `test/asynchronous/test_otel.py`'s `TestOTelSpans`:

```python
async def test_kill_cursors_gets_operation_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client.pymongo_test.kill_cursors_span
    await coll.drop()
    await coll.insert_many([{"i": i} for i in range(10)])
    cursor = coll.find({}, batch_size=2)
    await cursor.next()
    self.exporter.clear()
    await cursor.close()  # Sends killCursors, since batches remain.

    op_spans = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.operation.name") == "killCursors"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(
        len(op_spans), 1, [s.name for s in self.exporter.get_finished_spans()]
    )
    (op_span,) = op_spans
    self.assertEqual(op_span.name, "killCursors pymongo_test.kill_cursors_span")
    self.assertEqual(op_span.attributes["db.namespace"], "pymongo_test")
    self.assertEqual(op_span.attributes["db.collection.name"], "kill_cursors_span")

    cmd_spans = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.command.name") == "killCursors"
    ]
    self.assertEqual(len(cmd_spans), 1)
    self.assertEqual(cmd_spans[0].parent.span_id, op_span.context.span_id)


async def test_end_sessions_gets_operation_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    await client.pymongo_test.end_sessions_span.find_one(
        {}
    )  # Uses an implicit session.
    self.exporter.clear()
    await client.close()  # Sends endSessions.

    op_spans = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.operation.name") == "endSessions"
        and "db.command.name" not in s.attributes
    ]
    self.assertEqual(
        len(op_spans), 1, [s.name for s in self.exporter.get_finished_spans()]
    )
    (op_span,) = op_spans
    self.assertEqual(op_span.name, "endSessions admin")
    self.assertEqual(op_span.attributes["db.namespace"], "admin")
    self.assertNotIn("db.collection.name", op_span.attributes)

    cmd_spans = [
        s
        for s in self.exporter.get_finished_spans()
        if s.attributes.get("db.command.name") == "endSessions"
    ]
    self.assertEqual(len(cmd_spans), 1)
    self.assertEqual(cmd_spans[0].parent.span_id, op_span.context.span_id)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k "kill_cursors_gets or end_sessions_gets" -v`
Expected: FAIL with `AssertionError: 0 != 1` on both — the command spans exist today but no operation span wraps them.

- [ ] **Step 3: Wrap `_kill_cursor_impl`**

Replace the body of `_kill_cursor_impl` (currently lines 2252-2262) with:

```python
async def _kill_cursor_impl(
    self,
    cursor_ids: Sequence[int],
    address: _CursorAddress,
    session: Optional[AsyncClientSession],
    conn: AsyncConnection,
) -> None:
    namespace = address.namespace
    db, coll = namespace.split(".", 1)
    spec = {"killCursors": coll, "cursors": cursor_ids}
    # killCursors deliberately bypasses _retry_internal (it must never be
    # retried), so its operation span is created here instead.
    with _OperationTelemetry(
        self.options.tracing, _Op.KILL_CURSORS, session, dbname=db, collection=coll
    ):
        await conn.command(db, spec, session=session, client=self)
```

`_Op` is already imported in this module (it's used throughout for server-selection operation names); confirm with a quick grep and add the import if not.

- [ ] **Step 4: Wrap `_end_sessions`**

In `_end_sessions` (currently lines 1730-1750), wrap the per-batch `conn.command(...)` call inside the existing `for i in range(...)` loop:

```python
for i in range(0, len(session_ids), common._MAX_END_SESSIONS):
    spec = {"endSessions": session_ids[i : i + common._MAX_END_SESSIONS]}
    # endSessions deliberately bypasses _retry_internal (errors are
    # ignored per spec, and it must not be retried), so its
    # operation span is created here instead.
    with _OperationTelemetry(
        self.options.tracing, _Op.END_SESSIONS, None, dbname="admin"
    ):
        await conn.command("admin", spec, read_preference=read_pref, client=self)
```

Keep the enclosing `try/except PyMongoError: pass` exactly as it is — the spec requires ignoring `endSessions` errors, and `_OperationTelemetry.__exit__` will still have recorded the failure on the span before that outer handler swallows the exception.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py -m otel -v`
Expected: PASS.

- [ ] **Step 6: Run synchro, typing, lint, and a regression slice**

```bash
just synchro
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_cursor.py test/asynchronous/test_session.py test/asynchronous/test_client.py -q
```
Expected: all clean.

- [ ] **Step 7: Commit**

```bash
git add pymongo/asynchronous/mongo_client.py pymongo/synchronous/mongo_client.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add operation spans for killCursors and endSessions"
```

---

### Task 7: One `withTransaction` span per call, and a span for retried commits

**Files:**
- Modify: `pymongo/asynchronous/client_session.py` (`with_transaction`, `commit_transaction`)
- Test: `test/asynchronous/test_otel.py`

**Interfaces:**
- Consumes from Task 1: `_OperationTelemetry` as a context manager (default `set_current=True`, so it becomes the ambient parent).
- Consumes existing: `_otel.start_transaction_span(tracing_options)`, which reads ambient context for its parent — this is what makes each retry's `"transaction"` span nest under the `withTransaction` span with no further wiring.
- Produces: nothing new.

- [ ] **Step 1: Write the failing tests**

Add to `test/asynchronous/test_otel.py`'s `TestOTelSpans`. These use `fail_point` to inject the errors that force retries — check how the existing transaction tests in this file (or `test/asynchronous/test_transactions.py`) enter a fail point and follow that pattern exactly; the helper is typically `self.fail_point({...})` on the test-case base class.

```python
@async_client_context.require_transactions
async def test_with_transaction_retry_nests_transaction_spans(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client.pymongo_test.with_txn_spans
    await coll.drop()
    await client.pymongo_test.create_collection("with_txn_spans")

    attempts = []

    async def callback(session):
        attempts.append(1)
        await coll.insert_one({"n": len(attempts)}, session=session)
        if len(attempts) == 1:
            exc = OperationFailure("transient", 251)
            exc._add_error_label("TransientTransactionError")
            raise exc

    self.exporter.clear()
    async with client.start_session() as session:
        await session.with_transaction(callback)

    self.assertEqual(len(attempts), 2)
    finished = self.exporter.get_finished_spans()
    with_txn_spans = [s for s in finished if s.name.startswith("withTransaction")]
    self.assertEqual(len(with_txn_spans), 1, [s.name for s in finished])
    (with_txn_span,) = with_txn_spans

    txn_spans = [s for s in finished if s.name == "transaction"]
    self.assertEqual(len(txn_spans), 2)
    for txn_span in txn_spans:
        self.assertEqual(txn_span.parent.span_id, with_txn_span.context.span_id)


@async_client_context.require_transactions
async def test_retried_commit_has_a_transaction_span(self):
    client = await self.async_rs_or_single_client(tracing={"enabled": True})
    coll = client.pymongo_test.retried_commit_spans
    await coll.drop()
    await client.pymongo_test.create_collection("retried_commit_spans")

    async with client.start_session() as session:
        await session.start_transaction()
        await coll.insert_one({"x": 1}, session=session)
        await session.commit_transaction()
        self.exporter.clear()
        # An explicit second commit re-enters the COMMITTED -> IN_PROGRESS
        # branch, which previously ran with no transaction span at all.
        await session.commit_transaction()

    finished = self.exporter.get_finished_spans()
    txn_spans = [s for s in finished if s.name == "transaction"]
    self.assertEqual(len(txn_spans), 1, [s.name for s in finished])
    commit_cmd_spans = [
        s
        for s in finished
        if s.attributes.get("db.command.name") == "commitTransaction"
    ]
    self.assertGreaterEqual(len(commit_cmd_spans), 1)
    for cmd_span in commit_cmd_spans:
        self.assertIsNotNone(cmd_span.parent)
```

Ensure `OperationFailure` and `async_client_context` are imported in the test module (they very likely already are).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py -m otel -k "with_transaction_retry_nests or retried_commit_has" -v`
Expected: FAIL — the first with `AssertionError: 0 != 1` (no `withTransaction` span exists yet), the second with `AssertionError: 0 != 1` on `txn_spans` (the retried commit currently produces no transaction span).

- [ ] **Step 3: Wrap `with_transaction`'s retry loop**

In `with_transaction` (`pymongo/asynchronous/client_session.py`), add the import at the top of the file if absent:

```python
from pymongo._telemetry import _OperationTelemetry
```

Then wrap everything from the `start_time = time.monotonic()` line (currently ~774) through the end of the method in a single `with` block. The whole existing body moves one indent level to the right, unchanged:

```python
# One span for the whole logical withTransaction call. Made current, so
# each retry's "transaction" span (started by start_transaction, which
# reads ambient context for its parent) nests under this one instead of
# becoming a sibling.
with _OperationTelemetry(
    self._client.options.tracing, "withTransaction", None, dbname="admin"
):
    start_time = time.monotonic()
    retry = 0
    last_error: Optional[BaseException] = None
    while True:
        ...  # the rest of the existing body, re-indented
```

Pass `None` for `session` (not `self`): the `session` argument exists only so `_OperationTelemetry` can look up an active transaction span to parent to, and at this point there is no transaction yet — passing `self` would be wrong once a retry is in flight.

Keep every `return`/`raise`/`continue`/`break` exactly as-is; `__exit__` ends the span on all of those paths.

- [ ] **Step 4: Give the retried-commit branch a transaction span**

In `commit_transaction`, find the `elif state is _TxnState.COMMITTED:` branch that sets the state back to `IN_PROGRESS`, and create a replacement span there, since the previous attempt's `finally` already ended and cleared it:

```
        elif state is _TxnState.COMMITTED:
            # We're explicitly retrying the commit, move the state back to
            # "in progress" so that in_transaction returns true.
            self._transaction.state = _TxnState.IN_PROGRESS
            # The prior attempt's finally block already ended and cleared the
            # transaction span, so this retry needs a fresh one -- otherwise it
            # would run with no transaction span and its command span would
            # have no parent.
            if self._transaction.span is None:
                self._transaction.span = _otel.start_transaction_span(
                    self._transaction.client.options.tracing
                )
```

`_otel` is already imported in this file (used by `start_transaction`); confirm with grep.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py -m otel -v`
Expected: PASS, including the vendored `transaction/convenient.json` and `transaction/core_api.json` fixtures. `convenient.json` covers `withTransaction` — if it now fails, the fixture is the authority on the expected span shape; report the mismatch rather than editing the fixture.

- [ ] **Step 6: Run synchro, typing, lint, and a transactions regression slice**

```bash
just synchro
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_transactions.py test/asynchronous/test_session.py -q
```
Expected: all clean. This task edits core transaction-lifecycle code, so the transactions suite passing matters.

- [ ] **Step 7: Commit**

```bash
git add pymongo/asynchronous/client_session.py pymongo/synchronous/client_session.py test/asynchronous/test_otel.py test/test_otel.py
git commit -m "PYTHON-5947 Add withTransaction span and fix retried-commit transaction span"
```

---

### Task 8: Cross-version verification, changelog, and docstring update

**Files:**
- Modify: `doc/changelog.rst`, `pymongo/asynchronous/mongo_client.py` (the `tracing` option docstring), `pymongo/synchronous/mongo_client.py` (same, via synchro)
- Test: full otel suite on two Python versions

**Interfaces:**
- Consumes: everything from Tasks 1-7.
- Produces: nothing new.

- [ ] **Step 1: Run the full otel suite on Python 3.10 (the default venv)**

```bash
source .venv/bin/activate
python -m pytest test/asynchronous/test_otel.py test/test_otel.py -m otel -q
python -m pytest test/asynchronous/test_open_telemetry_unified.py test/test_open_telemetry_unified.py -m otel -q
```
Expected: all pass, no skips other than the two pre-existing documented ones (`map_reduce` for the removed API, and the `update` fixture's `$$matchAsRoot` divergence).

- [ ] **Step 2: Run the same suites on Python 3.13**

```bash
uv run --python 3.13 --extra opentelemetry --extra test --with opentelemetry-sdk python -m pytest test/asynchronous/test_otel.py test/test_otel.py test/asynchronous/test_open_telemetry_unified.py test/test_open_telemetry_unified.py -m otel -q
```
Expected: identical results to Step 1. A prior Critical bug in this feature (enum formatting in span names) reproduced only on 3.11+, so a 3.10-only run is not sufficient evidence.

- [ ] **Step 3: Update the changelog**

In `doc/changelog.rst`, extend the existing OpenTelemetry bullet in the in-progress `4.18` section (added by PYTHON-5945/5947) to cover the new nesting behavior. Read the current bullet first and append to it rather than adding a second one:

```rst
  Operation spans now cover a cursor's whole lifetime, so every ``getMore``
  nests under the ``find``/``aggregate`` that created the cursor, and
  ``killCursors``/``endSessions`` now get operation spans of their own. A
  single ``withTransaction`` span wraps all retries of one
  ``with_transaction()`` call.
```

- [ ] **Step 4: Update the `tracing` option docstring**

In `pymongo/asynchronous/mongo_client.py`'s `tracing` option docstring, extend the existing `.. versionchanged:: 4.18` block (do not add a second block for the same version) so it mentions cursor and transaction span nesting:

```
        .. versionchanged:: 4.18
           Added the ``tracing`` keyword argument. The ``tracing`` option
           creates one span per public API call (nesting each call's command
           spans, including a cursor's ``getMore`` commands, underneath), a
           ``"transaction"`` pseudo-span wrapping ``start_transaction()``
           through ``commit_transaction()``/``abort_transaction()``, and a
           ``"withTransaction"`` span wrapping all retries of one
           ``with_transaction()`` call.
```

- [ ] **Step 5: Run synchro, typing, and lint**

```bash
just synchro
just typing
just lint-manual
```
Expected: all clean; `git status` shows `pymongo/synchronous/mongo_client.py` regenerated with the same docstring.

- [ ] **Step 6: Commit**

```bash
git add doc/changelog.rst pymongo/asynchronous/mongo_client.py pymongo/synchronous/mongo_client.py
git commit -m "PYTHON-5947 Document expanded OTel span coverage"
```

---

## Final verification

After all 8 tasks, from the worktree root:

```bash
just typing
just lint-manual
source .venv/bin/activate && python -m pytest test/asynchronous/test_otel.py test/test_otel.py test/asynchronous/test_open_telemetry_unified.py test/test_open_telemetry_unified.py -m otel -q
source .venv/bin/activate && python -m pytest test/asynchronous/test_collection.py test/asynchronous/test_database.py test/asynchronous/test_cursor.py test/asynchronous/test_session.py test/asynchronous/test_transactions.py test/asynchronous/test_change_stream.py test/asynchronous/test_client.py test/asynchronous/test_client_bulk_write.py test/asynchronous/test_bulk.py -q
uv run --python 3.13 --extra opentelemetry --extra test --with opentelemetry-sdk python -m pytest test/asynchronous/test_otel.py test/asynchronous/test_open_telemetry_unified.py -m otel -q
```

All must pass. The vendored spec fixtures under `test/open_telemetry/` are the authority on expected span shapes — if one fails, fix the driver, not the fixture.

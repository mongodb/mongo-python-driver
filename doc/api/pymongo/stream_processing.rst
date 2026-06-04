:mod:`stream_processing` -- Atlas Stream Processing
====================================================

.. warning::

   The Atlas Stream Processing API is **experimental**. The driver
   specification is in Draft status and the API surface — including
   retryability behavior, error code mappings, and method signatures —
   may change in a backward-incompatible way before the spec is
   finalized.

Overview
--------

Atlas Stream Processing (ASP) lets you build continuous, stateful pipelines
that process data from one or more sources in real time. A *stream processing
workspace* is a dedicated Atlas endpoint that hosts one or more named stream
processors; it is distinct from a standard MongoDB cluster and is accessed
through :class:`~pymongo.asynchronous.stream_processing.AsyncStreamProcessingClient`
(or its sync twin :class:`~pymongo.synchronous.stream_processing.StreamProcessingClient`)
rather than ``MongoClient``.

Workspace connection strings use the standard ``mongodb://`` scheme — ``mongodb+srv://``
is not supported. TLS is always required for workspace connections and cannot be
disabled. ``authSource`` defaults to ``"admin"`` if not explicitly set. Users must
hold the ``atlasAdmin`` role to execute ASP commands.

Quickstart
----------

Async
~~~~~

.. code-block:: python

   import asyncio
   from pymongo import AsyncStreamProcessingClient

   async def main():
       uri = (
           "mongodb://user:pass@"
           "atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net/"
       )
       async with AsyncStreamProcessingClient(uri) as client:
           sps = client.stream_processors()
           await sps.create("demo", pipeline=[
               {"$source": {"connectionName": "<conn>", "topic": "events"}},
               {"$match": {"value": {"$gt": 100}}},
               {"$emit": {"connectionName": "<conn>", "db": "out", "coll": "high"}},
           ])
           proc = sps.get("demo")
           await proc.start()
           info = await sps.get_info("demo")
           print(info.state, info.pipeline_version)
           async for doc in proc.sample(limit=10):
               print(doc)
           await proc.stop()
           await proc.drop()

   asyncio.run(main())

Sync
~~~~

.. code-block:: python

   from pymongo import StreamProcessingClient

   uri = (
       "mongodb://user:pass@"
       "atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net/"
   )
   with StreamProcessingClient(uri) as client:
       sps = client.stream_processors()
       sps.create("demo", pipeline=[
           {"$source": {"connectionName": "<conn>", "topic": "events"}},
           {"$match": {"value": {"$gt": 100}}},
           {"$emit": {"connectionName": "<conn>", "db": "out", "coll": "high"}},
       ])
       proc = sps.get("demo")
       proc.start()
       info = sps.get_info("demo")
       print(info.state, info.pipeline_version)
       for doc in proc.sample(limit=10):
           print(doc)
       proc.stop()
       proc.drop()

Sample cursor semantics
-----------------------

The sample cursor is a custom two-phase protocol distinct from the standard
MongoDB ``getMore`` mechanism. It MUST NOT be confused with standard
:class:`~pymongo.asynchronous.cursor.AsyncCursor` objects.

For most use cases, call :meth:`~pymongo.asynchronous.stream_processing.AsyncStreamProcessor.sample`
to obtain an :class:`~pymongo.asynchronous.stream_processing.AsyncSampleCursor` and iterate it
with ``async for``. The cursor drives the underlying protocol automatically:

- The first iteration sends ``startSampleStreamProcessor``, optionally with a ``limit``.
- Subsequent iterations send ``getMoreSampleStreamProcessor`` with the ``cursorId``
  returned by the previous call, optionally with a ``batchSize``.
- When the server returns ``cursorId: 0`` the cursor is exhausted and no further
  wire calls are made.

For fine-grained control — tracking ``cursorId`` across calls yourself — use
:meth:`~pymongo.asynchronous.stream_processing.AsyncStreamProcessor.get_stream_processor_samples`
directly. Pass ``cursor_id=0`` and an :exc:`~pymongo.errors.InvalidOperation` is raised
immediately, before any wire call is sent.

Commands not yet supported
--------------------------

The following commands from the ASP server specification are intentionally
deferred and not yet wrapped by this API:

- ``modifyStreamProcessor`` — rename, pipeline replacement, and DLQ reconfiguration
- ``listStreamProcessors`` — enumerate processors in a workspace
- ``listStreamConnections`` — enumerate available connections
- ``processStreamProcessor`` — one-shot ad-hoc pipeline execution
- ``listWorkspaceDefaults`` — fetch workspace tier defaults

Users can still call any of these directly via ``run_command`` on a plain
:class:`~pymongo.asynchronous.mongo_client.AsyncMongoClient` connected to the
workspace endpoint.

Async classes
-------------

.. autoclass:: pymongo.asynchronous.stream_processing.AsyncStreamProcessingClient
   :members:
   :show-inheritance:

.. autoclass:: pymongo.asynchronous.stream_processing.AsyncStreamProcessors
   :members:
   :show-inheritance:

.. autoclass:: pymongo.asynchronous.stream_processing.AsyncStreamProcessor
   :members:
   :show-inheritance:

.. autoclass:: pymongo.asynchronous.stream_processing.AsyncSampleCursor
   :members:
   :show-inheritance:

Sync classes
------------

.. autoclass:: pymongo.synchronous.stream_processing.StreamProcessingClient
   :members:
   :show-inheritance:

.. autoclass:: pymongo.synchronous.stream_processing.StreamProcessors
   :members:
   :show-inheritance:

.. autoclass:: pymongo.synchronous.stream_processing.StreamProcessor
   :members:
   :show-inheritance:

.. autoclass:: pymongo.synchronous.stream_processing.SampleCursor
   :members:
   :show-inheritance:

Options and result types
------------------------

.. autoclass:: pymongo.stream_processing_options.CreateStreamProcessorOptions
   :members:
   :show-inheritance:

.. autoclass:: pymongo.stream_processing_options.StartStreamProcessorOptions
   :members:
   :show-inheritance:

.. autoclass:: pymongo.stream_processing_options.GetStreamProcessorStatsOptions
   :members:
   :show-inheritance:

.. autoclass:: pymongo.stream_processing_options.GetStreamProcessorSamplesOptions
   :members:
   :show-inheritance:

.. autoclass:: pymongo.stream_processing_options.GetStreamProcessorSamplesResult
   :members:
   :show-inheritance:

.. autoclass:: pymongo.stream_processing_options.StreamProcessorInfo
   :members:
   :show-inheritance:

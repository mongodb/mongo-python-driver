# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Options and result classes for Atlas Stream Processing commands.

These classes are shared between the synchronous and asynchronous APIs —
no async/sync split is needed for plain dataclasses.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping, Optional

if TYPE_CHECKING:
    from bson import Timestamp

from pymongo.errors import InvalidOperation

_VALID_TIERS = {"SP2", "SP5", "SP10", "SP30", "SP50"}


@dataclass
class CreateStreamProcessorOptions:
    """Options for :meth:`AsyncStreamProcessors.create`.

    All fields are optional.
    """

    dlq: Optional[Mapping[str, Any]] = None
    stream_meta_field_name: Optional[str] = None
    tier: Optional[str] = None
    failover: Optional[bool] = None


@dataclass
class StartStreamProcessorOptions:
    """Options for :meth:`AsyncStreamProcessor.start`.

    ``start_after`` and ``start_at_operation_time`` are mutually exclusive.
    ``tier``, when provided, must be one of ``"SP2"``, ``"SP5"``, ``"SP10"``,
    ``"SP30"``, or ``"SP50"``.
    """

    workers: Optional[int] = None
    clear_checkpoints: Optional[bool] = None
    start_at_operation_time: Optional[Timestamp] = None
    start_after: Optional[Mapping[str, Any]] = None
    tier: Optional[str] = None
    enable_auto_scaling: Optional[bool] = None
    failover: Optional[Mapping[str, Any]] = None

    def __post_init__(self) -> None:
        if self.start_after is not None and self.start_at_operation_time is not None:
            raise InvalidOperation(
                "start_after and start_at_operation_time are mutually exclusive."
            )
        if self.tier is not None and self.tier not in _VALID_TIERS:
            raise InvalidOperation(
                f"Invalid tier {self.tier!r}. Must be one of: {sorted(_VALID_TIERS)}."
            )
        if self.workers is not None and self.workers <= 0:
            raise InvalidOperation("workers must be a positive integer.")


@dataclass
class GetStreamProcessorStatsOptions:
    """Options for :meth:`AsyncStreamProcessor.stats`.

    :param scale: Size unit for byte-valued fields. ``1`` = bytes (default),
        ``1024`` = kibibytes.
    :param verbose: If ``True``, include per-operator statistics.
    """

    scale: Optional[int] = None
    verbose: Optional[bool] = None

    def __post_init__(self) -> None:
        if self.scale is not None and self.scale <= 0:
            raise InvalidOperation("scale must be a positive integer.")


@dataclass
class GetStreamProcessorSamplesOptions:
    """Options for :meth:`AsyncStreamProcessor.get_stream_processor_samples`.

    When ``cursor_id`` is absent or zero a new sample cursor is opened via
    ``startSampleStreamProcessor``; ``limit`` is only sent on that initial
    call.  When ``cursor_id`` is non-zero the next batch is fetched via
    ``getMoreSampleStreamProcessor``; ``batch_size`` is only sent on
    subsequent calls.
    """

    cursor_id: Optional[int] = None
    limit: Optional[int] = None
    batch_size: Optional[int] = None

    def __post_init__(self) -> None:
        if self.cursor_id is not None and self.cursor_id < 0:
            raise InvalidOperation("cursor_id must be non-negative (0 means exhausted).")
        if self.limit is not None and self.limit < 0:
            raise InvalidOperation("limit must be non-negative.")
        if self.batch_size is not None and self.batch_size < 0:
            raise InvalidOperation("batch_size must be non-negative.")


@dataclass
class GetStreamProcessorSamplesResult:
    """Result from :meth:`AsyncStreamProcessor.get_stream_processor_samples`.

    A ``cursor_id`` of ``0`` means the cursor is exhausted; no further calls
    should be made.
    """

    cursor_id: int
    documents: list[Mapping[str, Any]]


@dataclass
class StreamProcessorInfo:
    """Information about a single stream processor.

    Returned by :meth:`AsyncStreamProcessors.get_info`.

    All fields from the ``getStreamProcessor`` server response are mapped to
    snake_case.  The complete raw server document is preserved in :attr:`raw`
    so that unknown or future fields are not silently discarded.
    """

    id: Optional[str]
    name: str
    state: str  # plain str — drivers MUST NOT hard-code this as an Enum
    pipeline: list[Mapping[str, Any]]
    pipeline_version: Optional[int]
    tier: Optional[str] = None
    dlq: Optional[Mapping[str, Any]] = None
    stream_meta_field_name: Optional[str] = None
    enable_auto_scaling: bool = False
    failover_enabled: bool = False
    active_region: str = ""
    last_modified_at: Optional[datetime] = None
    modified_by: Optional[str] = None
    last_state_change: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    has_started: bool = False
    stats: Optional[Mapping[str, Any]] = None
    error_msg: Optional[str] = None
    error_code: Optional[int] = None
    error_retryable: Optional[bool] = None
    raw: Mapping[str, Any] = field(default_factory=dict)

    @classmethod
    def from_response(cls, doc: Mapping[str, Any]) -> StreamProcessorInfo:
        """Construct a :class:`StreamProcessorInfo` from a server response document.

        Maps camelCase server keys to Python snake_case fields and stashes the
        full *doc* in :attr:`raw` so no fields are silently dropped.
        """
        return cls(
            id=doc.get("id"),
            name=doc["name"],
            state=doc["state"],
            pipeline=doc.get("pipeline", []),
            pipeline_version=doc.get("pipelineVersion"),
            tier=doc.get("tier"),
            dlq=doc.get("dlq"),
            stream_meta_field_name=doc.get("streamMetaFieldName"),
            enable_auto_scaling=doc.get("enableAutoScaling", False),
            failover_enabled=doc.get("failoverEnabled", False),
            active_region=doc.get("activeRegion", ""),
            last_modified_at=doc.get("lastModifiedAt"),
            modified_by=doc.get("modifiedBy"),
            last_state_change=doc.get("lastStateChange"),
            last_heartbeat=doc.get("lastHeartbeat"),
            has_started=doc.get("hasStarted", False),
            stats=doc.get("stats"),
            error_msg=doc.get("errorMsg"),
            error_code=doc.get("errorCode"),
            error_retryable=doc.get("errorRetryable"),
            raw=dict(doc),
        )

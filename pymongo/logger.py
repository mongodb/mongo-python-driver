# Copyright 2023-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import enum
import logging
import os
from typing import Any

from bson import UuidRepresentation, json_util
from bson.json_util import JSONOptions, _truncate_documents


class _CommandStatusMessage(str, enum.Enum):
    STARTED = "Command started"
    SUCCEEDED = "Command succeeded"
    FAILED = "Command failed"


_DEFAULT_DOCUMENT_LENGTH = 1000
_SENSITIVE_COMMANDS = [
    "authenticate",
    "saslStart",
    "saslContinue",
    "getnonce",
    "createUser",
    "updateUser",
    "copydbgetnonce",
    "copydbsaslstart",
    "copydb",
]
_HELLO_COMMANDS = ["hello", "ismaster", "isMaster"]
_REDACTED_FAILURE_FIELDS = ["code", "codeName", "errorLabels"]
_DOCUMENT_NAMES = ["command", "reply", "failure"]
_JSON_OPTIONS = JSONOptions(uuid_representation=UuidRepresentation.STANDARD)
_COMMAND_LOGGER = logging.getLogger("pymongo.command")


def _debug_log(logger: logging.Logger, **fields):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(LogMessage(**fields))


class LogMessage:
    __slots__ = ["_kwargs"]

    def __init__(self, **kwargs: Any):
        self._kwargs = kwargs

        if "durationMS" in self._kwargs:
            self._kwargs["durationMS"] = self._kwargs["durationMS"].total_seconds() * 1000
        if "serviceId" in self._kwargs and self._kwargs["serviceId"] is None:
            del self._kwargs["serviceId"]

    def __str__(self) -> str:
        self._redact()
        return "%s" % (
            json_util.dumps(
                self._kwargs, json_options=_JSON_OPTIONS, default=lambda o: o.__repr__()
            )
        )

    def _is_sensitive(self, doc_name: str) -> bool:
        is_speculative_authenticate = (
            self._kwargs.pop("speculative_authenticate", False)
            or "speculativeAuthenticate" in self._kwargs[doc_name]
        )
        is_sensitive_command = (
            "commandName" in self._kwargs and self._kwargs["commandName"] in _SENSITIVE_COMMANDS
        )

        is_sensitive_hello = (
            self._kwargs["commandName"] in _HELLO_COMMANDS and is_speculative_authenticate
        )

        return is_sensitive_command or is_sensitive_hello

    def _redact(self) -> None:
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", _DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = _DEFAULT_DOCUMENT_LENGTH
        is_server_side_error = self._kwargs.pop("isServerSideError", False)

        for doc_name in _DOCUMENT_NAMES:
            doc = self._kwargs.get(doc_name)
            if doc:
                if doc_name == "failure" and is_server_side_error:
                    doc = {k: v for k, v in doc.items() if k in _REDACTED_FAILURE_FIELDS}
                if doc_name != "failure" and self._is_sensitive(doc_name):
                    doc = json_util.dumps({})
                else:
                    truncated_doc = _truncate_documents(doc, document_length)[0]
                    doc = json_util.dumps(
                        truncated_doc,
                        json_options=_JSON_OPTIONS,
                        default=lambda o: o.__repr__(),
                    )
                if len(doc) > document_length:
                    doc = (
                        doc.encode()[:document_length].decode("unicode-escape", "ignore")
                    ) + "..."
                self._kwargs[doc_name] = doc

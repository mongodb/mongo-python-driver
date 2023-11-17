from __future__ import annotations

import enum
import os
from typing import Any

from bson import UuidRepresentation, json_util
from bson.json_util import JSONOptions


class LogMessageStatus(str, enum.Enum):
    STARTED = "Command started"
    SUCCEEDED = "Command succeeded"
    FAILED = "Command failed"


DEFAULT_DOCUMENT_LENGTH = 1000
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
_DOCUMENTS = ["command", "reply", "failure"]
_JSON_OPTIONS = JSONOptions(uuid_representation=UuidRepresentation.STANDARD)


class LogMessage:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs

        self._process_documents()

        if "durationMS" in self.kwargs:
            self.kwargs["durationMS"] = self.kwargs["durationMS"].total_seconds() * 1000
        if "serviceId" in self.kwargs and self.kwargs["serviceId"] is None:
            del self.kwargs["serviceId"]

    def __str__(self) -> str:
        return "%s" % (
            json_util.dumps(self.kwargs, json_options=_JSON_OPTIONS, default=lambda o: o.__repr__())
        )

    def _is_sensitive(self, doc: str) -> bool:
        is_speculative_authenticate = (
            self.kwargs.pop("speculative_authenticate", False)
            or "speculativeAuthenticate" in self.kwargs[doc]
        )
        is_sensitive_command = (
            "commandName" in self.kwargs and self.kwargs["commandName"] in _SENSITIVE_COMMANDS
        )

        is_sensitive_hello = (
            self.kwargs["commandName"] in _HELLO_COMMANDS and is_speculative_authenticate
        )

        return is_sensitive_command or is_sensitive_hello

    def _process_documents(self) -> None:
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = DEFAULT_DOCUMENT_LENGTH
        is_server_side_error = self.kwargs.pop("isServerSideError", False)

        for doc in _DOCUMENTS:
            if doc in self.kwargs:
                if doc == "failure" and is_server_side_error:
                    self.kwargs[doc] = {
                        k: v for k, v in self.kwargs[doc].items() if k in _REDACTED_FAILURE_FIELDS
                    }
                if doc != "failure" and self._is_sensitive(doc):
                    self.kwargs[doc] = json_util.dumps({})
                else:
                    self.kwargs[doc] = json_util.dumps(
                        self.kwargs[doc],
                        json_options=_JSON_OPTIONS,
                        default=lambda o: o.__repr__(),
                    )
                if len(self.kwargs[doc]) > document_length:
                    self.kwargs[doc] = self.kwargs[doc][:document_length] + "..."

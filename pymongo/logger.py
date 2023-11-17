from __future__ import annotations

import os
from typing import Any

from bson import UuidRepresentation, json_util
from bson.json_util import JSONOptions

DEFAULT_DOCUMENT_LENGTH = 1000
SENSITIVE_COMMANDS = [
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
HELLO_COMMANDS = ["hello", "ismaster", "isMaster"]
REDACTED_FAILURE_FIELDS = ["code", "codeName", "errorLabels"]
DOCUMENTS = ["command", "reply", "failure"]


class LogMessage:
    def __init__(self, **kwargs: Any):
        self.kwargs = kwargs
        self.json_options = JSONOptions(uuid_representation=UuidRepresentation.STANDARD)

        self._process_documents()

        if "durationMS" in self.kwargs:
            self.kwargs["durationMS"] = self.kwargs["durationMS"].total_seconds() * 1000
        if "serviceId" in self.kwargs and self.kwargs["serviceId"] is None:
            del self.kwargs["serviceId"]

    def __str__(self) -> str:
        return "%s" % (
            json_util.dumps(
                self.kwargs, json_options=self.json_options, default=lambda o: o.__repr__()
            )
        )

    def _process_documents(self):
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = DEFAULT_DOCUMENT_LENGTH

        is_server_side_error = self.kwargs.pop("isServerSideError", False)
        is_speculative_authenticate = self.kwargs.pop("speculative_authenticate", False)
        is_sensitive_command = (
            "commandName" in self.kwargs and self.kwargs["commandName"] in SENSITIVE_COMMANDS
        )
        for doc in DOCUMENTS:
            if doc in self.kwargs:
                if doc == "failure" and is_server_side_error:
                    self.kwargs[doc] = {
                        k: v for k, v in self.kwargs[doc].items() if k in REDACTED_FAILURE_FIELDS
                    }
                is_speculative_authenticate = (
                    is_speculative_authenticate or "speculativeAuthenticate" in self.kwargs[doc]
                )
                is_sensitive_hello = (
                    self.kwargs["commandName"] in HELLO_COMMANDS and is_speculative_authenticate
                )
                if doc != "failure" and (is_sensitive_command or is_sensitive_hello):
                    self.kwargs[doc] = json_util.dumps({})
                else:
                    self.kwargs[doc] = json_util.dumps(
                        self.kwargs[doc],
                        json_options=self.json_options,
                        default=lambda o: o.__repr__(),
                    )
                if len(self.kwargs[doc]) > document_length:
                    self.kwargs[doc] = self.kwargs[doc][:document_length] + "..."

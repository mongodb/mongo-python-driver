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


class StructuredMessage:
    def __init__(self, **kwargs: Any):
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = DEFAULT_DOCUMENT_LENGTH
        self.kwargs = kwargs
        _documents = ["command", "reply", "failure"]
        json_options = JSONOptions(uuid_representation=UuidRepresentation.STANDARD)
        for doc in _documents:
            if doc in kwargs:
                if doc == "failure" and kwargs.pop("isServerSideError", False):
                    kwargs[doc] = {
                        k: v for k, v in kwargs[doc].items() if k in REDACTED_FAILURE_FIELDS
                    }
                is_speculative_authenticate = (
                    kwargs.pop("speculative_authenticate", False)
                    or "speculativeAuthenticate" in kwargs[doc]
                )
                if (
                    doc != "failure"
                    and ("commandName" in kwargs and kwargs["commandName"] in SENSITIVE_COMMANDS)
                    or (kwargs["commandName"] in HELLO_COMMANDS and is_speculative_authenticate)
                ):
                    kwargs[doc] = json_util.dumps({})
                else:
                    kwargs[doc] = json_util.dumps(
                        kwargs[doc], json_options=json_options, default=lambda o: o.__repr__()
                    )
                if len(kwargs[doc]) > document_length:
                    kwargs[doc] = kwargs[doc][:document_length] + "..."

        if "durationMS" in kwargs:
            kwargs["durationMS"] = kwargs["durationMS"].total_seconds() * 1000
        if "serviceId" in kwargs and kwargs["serviceId"] is None:
            del kwargs["serviceId"]

    def __str__(self) -> str:
        return "%s" % (json_util.dumps(self.kwargs))

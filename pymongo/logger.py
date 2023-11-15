from __future__ import annotations

import os

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


class StructuredMessage:
    def __init__(self, **kwargs):
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = DEFAULT_DOCUMENT_LENGTH
        self.kwargs = kwargs
        _documents = ["command", "reply", "failure"]
        json_options = JSONOptions(uuid_representation=UuidRepresentation.STANDARD)
        for doc in _documents:
            if doc in kwargs:
                is_speculative_authenticate = (
                    kwargs.pop("speculative_authenticate", False)
                    or "speculativeAuthenticate" in kwargs[doc]
                )
                if ("commandName" in kwargs and kwargs["commandName"] in SENSITIVE_COMMANDS) or (
                    kwargs["commandName"] in HELLO_COMMANDS and is_speculative_authenticate
                ):
                    kwargs[doc] = json_util.dumps({})
                else:
                    kwargs[doc] = json_util.dumps(kwargs[doc], json_options=json_options)
                if len(kwargs[doc]) > document_length:
                    kwargs[doc] = kwargs[doc][:document_length] + "..."

        if "durationMS" in kwargs:
            kwargs["durationMS"] = kwargs["durationMS"].total_seconds() * 1000

    def __str__(self):
        return "%s" % (json_util.dumps(self.kwargs))

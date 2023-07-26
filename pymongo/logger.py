import os

from bson import json_util

DEFAULT_DOCUMENT_LENGTH = 1000


class StructuredMessage:
    def __init__(self, **kwargs):
        document_length = int(os.getenv("MONGOB_LOG_MAX_DOCUMENT_LENGTH", DEFAULT_DOCUMENT_LENGTH))
        if document_length < 0:
            document_length = DEFAULT_DOCUMENT_LENGTH
        self.kwargs = kwargs
        _documents = ["command", "reply"]
        for doc in _documents:
            if doc in kwargs:
                kwargs[doc] = json_util.dumps(kwargs[doc])
                if len(kwargs[doc]) > document_length:
                    kwargs[doc] = kwargs[doc][:document_length] + "..."

        if "durationMS" in kwargs:
            kwargs["durationMS"] = kwargs["durationMS"].total_seconds() * 1000

    def __str__(self):
        return "%s" % (json_util.dumps(self.kwargs))

# Copyright 2017-present MongoDB, Inc.
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

"""Logical sessions for ordering sequential operations.

Requires MongoDB 3.6.

.. versionadded:: 3.6

Causally Consistent Reads
=========================

.. code-block:: python

  with client.start_session(causally_consistent_reads=True) as session:
      collection = client.db.collection
      collection.update_one({'_id': 1}, {'$set': {'x': 10}}, session=session)
      secondary_c = collection.with_options(
          read_preference=ReadPreference.SECONDARY)

      # A secondary read waits for replication of the write.
      secondary_c.find_one({'_id': 1}, session=session)

If `causally_consistent_reads` is True, read operations that use the session are
causally after previous read and write operations. Using a causally consistent
session, an application can read its own writes and is guaranteed monotonic
reads, even when reading from replica set secondaries.

Classes
=======
"""

import uuid

from bson.binary import Binary
from pymongo import monotonic


class SessionOptions(object):
    """Options for a new :class:`ClientSession`.

    :Parameters:
      - `causally_consistent_reads` (optional): If True, read operations are
        causally ordered within the session.
    """
    def __init__(self, causally_consistent_reads=False):
        self._causally_consistent_reads = causally_consistent_reads

    @property
    def causally_consistent_reads(self):
        """Whether causally consistent reads are configured."""
        return self._causally_consistent_reads


class ClientSession(object):
    """A session for ordering sequential operations.

    :Parameters:
      - `client`: A :class:`~pymongo.mongo_client.MongoClient`.
      - `options` (optional): A :class:`SessionOptions` instance.
    """
    def __init__(self, client, options=None):
        self._client = client
        self._has_ended = False

        if options is not None:
            self._options = options
        else:
            self._options = SessionOptions()

        self._server_session = _ServerSession()

    def end_session(self):
        """Finish this session.

        It is an error to use the session or any derived
        :class:`~pymongo.database.Database`,
        :class:`~pymongo.collection.Collection`, or
        :class:`~pymongo.cursor.Cursor` after the session has ended.
        """
        self._has_ended = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_session()

    @property
    def client(self):
        """The :class:`~pymongo.mongo_client.MongoClient` this session was
        created from.
        """
        return self._client

    @property
    def options(self):
        """The :class:`SessionOptions` this session was created with."""
        return self._options

    @property
    def session_id(self):
        """A BSON document, the opaque server session identifier."""
        return self._server_session.session_id

    @property
    def has_ended(self):
        """True if this session is finished."""
        return self._has_ended


class _ServerSession(object):
    def __init__(self):
        # Ensure id is type 4, regardless of CodecOptions.uuid_representation.
        self.session_id = {'id': Binary(uuid.uuid4().bytes, 4)}
        self.last_use = monotonic.time()

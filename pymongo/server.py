# Copyright 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

"""Communicate with one MongoDB server in a topology."""

import contextlib
import socket

from pymongo.errors import AutoReconnect, DocumentTooLarge, NetworkTimeout
from pymongo.response import Response, ExhaustResponse
from pymongo.server_type import SERVER_TYPE


class Server(object):
    def __init__(self, server_description, pool, monitor):
        """Represent one MongoDB server."""
        self._description = server_description
        self._pool = pool
        self._monitor = monitor

    def open(self):
        """Start monitoring, or restart after a fork.

        Multiple calls have no effect.
        """
        self._monitor.open()

    def reset(self):
        """Clear the connection pool."""
        self.pool.reset()

    def close(self):
        """Clear the connection pool and stop the monitor.

        Reconnect with open().
        """
        self._monitor.close()
        self._pool.reset()

    def request_check(self):
        """Check the server's state soon."""
        self._monitor.request_check()

    def send_message(self, message, all_credentials):
        """Send an unacknowledged message to MongoDB.

        Can raise ConnectionFailure.

        :Parameters:
          - `message`: (request_id, data).
          - `all_credentials`: dict, maps auth source to MongoCredential.
        """
        request_id, data = self._check_bson_size(message)
        try:
            with self.get_socket(all_credentials) as sock_info:
                sock_info.send_message(data)
        except socket.error as exc:
            self._raise_connection_failure(exc)

    def send_message_with_response(
            self,
            message,
            all_credentials,
            exhaust=False):
        """Send a message to MongoDB and return a Response object.

        Can raise ConnectionFailure.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data).
          - `all_credentials`: dict, maps auth source to MongoCredential.
          - `exhaust` (optional): If True, the socket used stays checked out.
            It is returned along with its Pool in the Response.
        """
        request_id, data = self._check_bson_size(message)
        try:
            with self.get_socket(all_credentials, exhaust) as sock_info:
                sock_info.send_message(data)
                response_data = sock_info.receive_message(1, request_id)
                if exhaust:
                    return ExhaustResponse(
                        data=response_data,
                        address=self._description.address,
                        socket_info=sock_info,
                        pool=self._pool)
                else:
                    return Response(
                        data=response_data,
                        address=self._description.address)
        except socket.error as exc:
            self._raise_connection_failure(exc)

    @contextlib.contextmanager
    def get_socket(self, all_credentials, checkout=False):
        with self.pool.get_socket(all_credentials, checkout) as sock_info:
            yield sock_info

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, server_description):
        assert server_description.address == self._description.address
        self._description = server_description

    @property
    def pool(self):
        return self._pool

    def _check_bson_size(self, message):
        """Make sure the message doesn't include BSON documents larger
        than the server will accept.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data)

        Returns request_id, data.
        """
        if len(message) == 3:
            request_id, data, max_doc_size = message
            if max_doc_size > self.description.max_bson_size:
                raise DocumentTooLarge(
                    "BSON document too large (%d bytes) - the connected server"
                    "supports BSON document sizes up to %d bytes." %
                    (max_doc_size, self.description.max_bson_size))
            return request_id, data
        else:
            # get_more and kill_cursors messages don't include BSON documents.
            return message

    def _raise_connection_failure(self, exc):
        host, port = self._description.address
        msg = '%s:%d: %s' % (host, port, exc)

        if isinstance(exc, socket.timeout):
            raise NetworkTimeout(msg)
        else:
            raise AutoReconnect(msg)

    def __str__(self):
        d = self._description
        return '<Server "%s:%s" %s>' % (
            d.address[0], d.address[1],
            SERVER_TYPE._fields[d.server_type])

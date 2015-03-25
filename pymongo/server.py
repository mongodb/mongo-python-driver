# Copyright 2009-2015 MongoDB, Inc.
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
        request_id, data, max_doc_size = self._split_message(message)
        with self.get_socket(all_credentials) as sock_info:
            sock_info.send_message(data, max_doc_size)

    def send_message_with_response(
            self,
            operation,
            set_slave_okay,
            all_credentials,
            exhaust=False):
        """Send a message to MongoDB and return a Response object.

        Can raise ConnectionFailure.

        :Parameters:
          - `operation`: A _Query or _GetMore object.
          - `set_slave_okay`: Pass to operation.get_message.
          - `all_credentials`: dict, maps auth source to MongoCredential.
          - `exhaust` (optional): If True, the socket used stays checked out.
            It is returned along with its Pool in the Response.
        """
        with self.get_socket(all_credentials, exhaust) as sock_info:
            message = operation.get_message(
                set_slave_okay, sock_info.is_mongos)
            request_id, data, max_doc_size = self._split_message(message)
            sock_info.send_message(data, max_doc_size)
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

    def _split_message(self, message):
        """Return request_id, data, max_doc_size.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data)
        """
        if len(message) == 3:
            return message
        else:
            # get_more and kill_cursors messages don't include BSON documents.
            request_id, data = message
            return request_id, data, 0

    def __str__(self):
        d = self._description
        return '<Server "%s:%s" %s>' % (
            d.address[0], d.address[1],
            SERVER_TYPE._fields[d.server_type])

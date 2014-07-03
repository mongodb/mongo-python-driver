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

"""Communicate with one MongoDB server in a cluster."""

from pymongo.ismaster import SERVER_TYPE


class Server(object):
    def __init__(self, server_description, pool, monitor):
        """Represent one MongoDB server."""
        self._description = server_description
        self._pool = pool
        self._monitor = monitor

    def open(self):
        self._monitor.start()

    def close(self):
        self._monitor.close()

        # TODO: Add a close() method for consistency.
        self._pool.reset()

    def request_check(self):
        """Check the server's state soon."""
        self._monitor.request_check()

    def send_message_with_response(self, message, request_id):
        # TODO: make a context manager to use in a "with" statement.
        sock_info = self._pool.get_socket()
        try:
            sock_info.send_message(message)
            response = sock_info.receive_message(1, request_id)
        except:
            sock_info.close()
            raise
        finally:
            self._pool.maybe_return_socket(sock_info)
        
        return response

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

    def __str__(self):
        d = self._description
        return '<Server "%s:%s" %s>' % (
            d.address[0], d.address[1],
            SERVER_TYPE._fields[d.server_type])

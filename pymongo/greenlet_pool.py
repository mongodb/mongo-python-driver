# Copyright 2009-2011 10gen, Inc.
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

"""
Like pool.Pool, but start_request() binds a socket to this greenlet, as well
as this thread, until end_request().
"""

import greenlet

from pymongo.pool import BasePool, NO_REQUEST, NO_SOCKET_YET, SocketInfo
import weakref

_refs = {}

class GreenletPool(BasePool):
    """A simple connection pool.

    Calling start_request() acquires a greenlet-local socket, which is returned
    to the pool when the greenlet calls end_request() or dies.
    """
    def __init__(self, *args, **kwargs):
        self._gr_id_to_sock = {}
        self._refs = {}
        super(GreenletPool, self).__init__(*args, **kwargs)

    # Overrides
    def _set_request_socket(self, sock_info):
        current = greenlet.getcurrent()
        gr_id = id(current)

        # TODO: comment, rename
        def on_gr_died(ref):
            self._refs.pop(gr_id, None)
            self._gr_id_to_sock.pop(gr_id, None)

        if sock_info == NO_REQUEST:
            on_gr_died(None)
        else:
            self._refs[gr_id] = weakref.ref(current, on_gr_died)
            self._gr_id_to_sock[gr_id] = sock_info

    def _get_request_socket(self):
        gr_id = id(greenlet.getcurrent())
        return self._gr_id_to_sock.get(gr_id, NO_REQUEST)

    def _reset(self):
        self._gr_id_to_sock.clear()
        self._refs.clear()

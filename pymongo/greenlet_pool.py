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

from pymongo.pool import Pool, NO_REQUEST, NO_SOCKET_YET, SocketInfo
import weakref

_refs = {}

class GreenletPool(Pool):
    """A simple connection pool.

    Calling start_request() acquires a greenlet-local socket, which is returned
    to the pool when the greenlet calls end_request() or dies.
    """
    # Override
    def _current_thread_id(self):
        return id(greenlet.getcurrent())

    # Override
    def _watch_current_thread(self, callback):
        gr_id = self._current_thread_id()
        if callback:
            def _callback(ref):
                callback(gr_id)

            _refs[gr_id] = weakref.ref(greenlet.getcurrent(), _callback)
        else:
            # This ref will be garbage-collected soon, so its callback probably
            # won't fire, but set "canceled" to be certain.
            _refs.pop(gr_id, None)

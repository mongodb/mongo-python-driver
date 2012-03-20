# Copyright 2012 10gen, Inc.
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

"""Utilities for testing pymongo
"""

import gc
import sys
import time

from nose.plugins.skip import SkipTest

from pymongo import pool

def delay(sec):
    return '''function() {
        var d = new Date((new Date()).getTime() + %s * 1000);
        while (d > (new Date())) { }; return true;
    }''' % sec

def get_command_line(connection):
    command_line = connection.admin.command('getCmdLineOpts')
    assert command_line['ok'] == 1, "getCmdLineOpts() failed"
    return command_line['argv']

def server_started_with_auth(connection):
    argv = get_command_line(connection)
    return '--auth' in argv or '--keyFile' in argv

def server_is_master_with_slave(connection):
    return '--master' in get_command_line(connection)

def drop_collections(db):
    for coll in db.collection_names():
        if not coll.startswith('system'):
            db.drop_collection(coll)

def force_reclaim_sockets(cx_pool, n_expected):
    # When a thread dies without ending its request, the SocketInfo it was
    # using is deleted, and in its __del__ it returns the socket to the
    # pool. However, when exactly that happens is unpredictable. Try
    # various ways of forcing the issue.

    if sys.platform.startswith('java'):
        raise SkipTest("Jython can't reclaim sockets")

    # Bizarre behavior in CPython 2.4, and possibly other CPython versions
    # less than 2.7: the last dead thread's locals aren't cleaned up until
    # the local attribute with the same name is accessed from a different
    # thread. This assert checks that the thread-local is indeed local, and
    # also triggers the cleanup so the socket is reclaimed.
    if isinstance(cx_pool, pool.Pool):
        assert cx_pool.local.sock_info is None

    # In PyPy, we need to try for a while to make garbage-collection call
    # SocketInfo.__del__
    start = time.time()
    while len(cx_pool.sockets) < n_expected and time.time() - start < 5:
        try:
            gc.collect(2)
        except TypeError:
            # collect() didn't support 'generation' arg until 2.5
            gc.collect()

        time.sleep(0.5)

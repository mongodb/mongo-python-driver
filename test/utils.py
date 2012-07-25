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

from pymongo.errors import AutoReconnect


def delay(sec):
    # Javascript sleep() only available in MongoDB since version ~1.9
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

def joinall(threads):
    """Join threads with a 5-minute timeout, assert joins succeeded"""
    for t in threads:
        t.join(300)
        assert not t.isAlive(), "Thread %s hung" % t

def is_mongos(conn):
    res = conn.admin.command('ismaster')
    return res.get('msg', '') == 'isdbgrid'

def read_from_which_host(
    rsc,
    mode,
    tag_sets=None,
    secondary_acceptable_latency_ms=15
):
    """Read from a ReplicaSetConnection with the given Read Preference mode,
       tags, and acceptable latency. Return the 'host:port' which was read from.

    :Parameters:
      - `rsc`: A ReplicaSetConnection
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms`: a float
    """
    db = rsc.pymongo_test
    db.read_preference = mode
    if isinstance(tag_sets, dict):
        tag_sets = [tag_sets]
    db.tag_sets = tag_sets or [{}]
    db.secondary_acceptable_latency_ms = secondary_acceptable_latency_ms

    cursor = db.test.find()
    try:
        try:
            cursor.next()
        except StopIteration:
            # No documents in collection, that's fine
            pass

        return cursor._Cursor__connection_id
    except AutoReconnect:
        return None

def assertReadFrom(testcase, rsc, member, *args, **kwargs):
    """Check that a query with the given mode, tag_sets, and
       secondary_acceptable_latency_ms reads from the expected replica-set
       member

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `rsc`: A ReplicaSetConnection
      - `member`: replica_set_connection.Member expected to be used
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms`: a float
    """
    for _ in range(10):
        testcase.assertEqual(member, read_from_which_host(rsc, *args, **kwargs))

def assertReadFromAll(testcase, rsc, members, *args, **kwargs):
    """Check that a query with the given mode, tag_sets, and
       secondary_acceptable_latency_ms can read from any of a set of
       replica-set members.

    :Parameters:
      - `testcase`: A unittest.TestCase
      - `rsc`: A ReplicaSetConnection
      - `members`: Sequence of replica_set_connection.Member expected to be used
      - `mode`: A ReadPreference
      - `tag_sets`: List of dicts of tags for data-center-aware reads
      - `secondary_acceptable_latency_ms`: a float
    """
    members = set(members)
    used = set()
    for _ in range(100):
        used.add(read_from_which_host(rsc, *args, **kwargs))

    testcase.assertEqual(members, used)

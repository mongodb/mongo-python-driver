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

"""TODO: help string."""

import random
import threading

from bson.py3compat import (string_type)
from pymongo import (database,
                     monitor,
                     pool,
                     uri_parser, ReadPreference)
from pymongo.cluster import Cluster
from pymongo.cluster_description import CLUSTER_TYPE
from pymongo.errors import (ConfigurationError)
from pymongo.server_selectors import (writable_server_selector,
                                      secondary_server_selector)
from pymongo.settings import ClusterSettings


class MongoClientNew(object):
    """Connection to one or more MongoDB servers.
    """

    def __init__(
        self,
        host='localhost',
        port=27017,
        replicaSet=None,
    ):
        """TODO: docstring"""
        if isinstance(host, string_type):
            host = [host]

        if not isinstance(port, int):
            raise TypeError("port must be an instance of int")

        seeds = set()

        for entity in host:
            seeds.update(uri_parser.split_hosts(entity, port))
        if not seeds:
            raise ConfigurationError("need to specify at least one host")

        cluster_settings = ClusterSettings(
            seeds=seeds,
            set_name=replicaSet,
            pool_class=pool.Pool,
            monitor_class=monitor.Monitor,
            condition_class=threading.Condition)

        # TODO: parse URI, socket timeouts, ssl args, auth,
        # pool_class, document_class, pool options, condition_class,
        # default database.

        self._cluster = Cluster(cluster_settings)
        self._cluster.open()

        # TODO: these are here to fake the old MongoClient's API for the sake
        # of existing Database, Collection, and Cursor.
        self.read_preference = ReadPreference.PRIMARY
        self.uuid_subtype = 4
        self.write_concern = {}
        self.document_class = dict
        self.tz_aware = False

    @property
    def is_mongos(self):
        return self._cluster.description.cluster_type == CLUSTER_TYPE.Mongos

    # TODO: Remove. Database, Collection, etc. should use Cluster.
    def _send_message_with_response(
            self,
            msg,
            read_preference=ReadPreference.PRIMARY,
            exhaust=False):
        """Send a message to MongoDB and return a Response object.

        :Parameters:
          - `msg`: (request_id, data) pair making up the message to send.
          - `read_preference`: A ReadPreference.
          - `exhaust`: True for an exhaust cursor's initial query.
        """
        request_id, data, max_doc_size = msg

        # TODO: real read preferences.
        if read_preference == ReadPreference.PRIMARY:
            servers = self._cluster.select_servers(writable_server_selector)
        else:
            servers = self._cluster.select_servers(secondary_server_selector)

        server = random.choice(servers)
        return server.send_message_with_response(data, request_id, exhaust)

    def __getattr__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return database.Database(self, name)

    def __getitem__(self, name):
        """Get a database by name.

        Raises :class:`~pymongo.errors.InvalidName` if an invalid
        database name is used.

        :Parameters:
          - `name`: the name of the database to get
        """
        return self.__getattr__(name)

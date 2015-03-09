# Copyright 2014 MongoDB, Inc.
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

"""Tools to parse mongo client options."""

from bson.codec_options import _parse_codec_options
from bson.py3compat import iteritems
from pymongo.auth import _build_credentials_tuple
from pymongo.common import validate, validate_boolean
from pymongo import common
from pymongo.errors import ConfigurationError
from pymongo.pool import PoolOptions
from pymongo.read_preferences import make_read_preference
from pymongo.ssl_support import get_ssl_context
from pymongo.write_concern import WriteConcern


def _parse_credentials(username, password, database, options):
    """Parse authentication credentials."""
    if username is None:
        return None
    mechanism = options.get('authmechanism', 'DEFAULT')
    source = options.get('authsource', database or 'admin')
    return _build_credentials_tuple(
        mechanism, source, username, password, options)


def _parse_read_preference(options):
    """Parse read preference options."""
    if 'read_preference' in options:
        return options['read_preference']

    mode = options.get('readpreference', 0)
    tags = options.get('readpreferencetags')
    return make_read_preference(mode, tags)


def _parse_write_concern(options):
    """Parse write concern options."""
    concern = options.get('w')
    wtimeout = options.get('wtimeout')
    j = options.get('j', options.get('journal'))
    fsync = options.get('fsync')
    return WriteConcern(concern, wtimeout, j, fsync)


def _parse_ssl_options(options):
    """Parse ssl options."""
    use_ssl = options.get('ssl')
    if use_ssl is not None:
        validate_boolean('ssl', use_ssl)

    certfile = options.get('ssl_certfile')
    keyfile = options.get('ssl_keyfile')
    ca_certs = options.get('ssl_ca_certs')
    cert_reqs = options.get('ssl_cert_reqs')
    match_hostname = options.get('ssl_match_hostname', True)

    ssl_kwarg_keys = [k for k in options
                      if k.startswith('ssl_') and options[k]]
    if use_ssl == False and ssl_kwarg_keys:
        raise ConfigurationError("ssl has not been enabled but the "
                                 "following ssl parameters have been set: "
                                 "%s. Please set `ssl=True` or remove."
                                 % ', '.join(ssl_kwarg_keys))

    if cert_reqs and not ca_certs:
        raise ConfigurationError("If `ssl_cert_reqs` is not "
                                 "`ssl.CERT_NONE` then you must "
                                 "include `ssl_ca_certs` to be able "
                                 "to validate the server.")

    if ssl_kwarg_keys and use_ssl is None:
        # ssl options imply ssl = True
        use_ssl = True

    if use_ssl is True:
        ctx = get_ssl_context(certfile, keyfile, ca_certs, cert_reqs)
        return ctx, match_hostname
    return None, match_hostname


def _parse_pool_options(options):
    """Parse connection pool options."""
    max_pool_size = options.get('max_pool_size')
    connect_timeout = options.get('connecttimeoutms', 20.0)
    socket_keepalive = options.get('socketkeepalive', False)
    socket_timeout = options.get('sockettimeoutms')
    wait_queue_timeout = options.get('waitqueuetimeoutms')
    wait_queue_multiple = options.get('waitqueuemultiple')
    ssl_context, ssl_match_hostname = _parse_ssl_options(options)
    return PoolOptions(max_pool_size,
                       connect_timeout, socket_timeout,
                       wait_queue_timeout, wait_queue_multiple,
                       ssl_context, ssl_match_hostname, socket_keepalive)


class ClientOptions(object):

    """ClientOptions"""

    def __init__(self, username, password, database, options):
        options = dict([validate(opt, val) for opt, val in iteritems(options)])

        self.__codec_options = _parse_codec_options(options)
        self.__credentials = _parse_credentials(
            username, password, database, options)
        self.__local_threshold_ms = options.get('localthresholdms', 15)
        # self.__server_selection_timeout is in seconds. Must use full name for
        # common.SERVER_SELECTION_TIMEOUT because it is set directly by tests.
        self.__server_selection_timeout = options.get(
            'serverselectiontimeoutms', common.SERVER_SELECTION_TIMEOUT)
        self.__pool_options = _parse_pool_options(options)
        self.__read_preference = _parse_read_preference(options)
        self.__replica_set_name = options.get('replicaset')
        self.__write_concern = _parse_write_concern(options)

    @property
    def codec_options(self):
        """A :class:`~bson.codec_options.CodecOptions` instance."""
        return self.__codec_options

    @property
    def credentials(self):
        """A :class:`~pymongo.auth.MongoCredentials` instance or None."""
        return self.__credentials

    @property
    def local_threshold_ms(self):
        """The local threshold for this instance."""
        return self.__local_threshold_ms

    @property
    def server_selection_timeout(self):
        """The server selection timeout for this instance in seconds."""
        return self.__server_selection_timeout

    @property
    def pool_options(self):
        """A :class:`~pymongo.pool.PoolOptions` instance."""
        return self.__pool_options

    @property
    def read_preference(self):
        """A read preference instance."""
        return self.__read_preference

    @property
    def replica_set_name(self):
        """Replica set name or None."""
        return self.__replica_set_name

    @property
    def write_concern(self):
        """A :class:`~pymongo.write_concern.WriteConcern` instance."""
        return self.__write_concern

# Copyright 2011-2014 MongoDB, Inc.
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


"""Functions and classes common to multiple pymongo modules."""

import collections
import warnings

from pymongo.auth import MECHANISMS
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import (make_read_preference,
                                      read_pref_mode_from_name,
                                      ServerMode)
from pymongo.ssl_support import validate_cert_reqs
from pymongo.write_concern import WriteConcern
from bson.binary import (OLD_UUID_SUBTYPE, UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY)
from bson.py3compat import string_type, integer_types

# Defaults until we connect to a server and get updated limits.
MAX_BSON_SIZE = 16 * (1024 ** 2)
MAX_MESSAGE_SIZE = 2 * MAX_BSON_SIZE  # TODO: remove.
MIN_WIRE_VERSION = 0
MAX_WIRE_VERSION = 0
MAX_WRITE_BATCH_SIZE = 1000

# What this version of PyMongo supports.
MIN_SUPPORTED_WIRE_VERSION = 0
MAX_SUPPORTED_WIRE_VERSION = 2

# Frequency to call ismaster on servers, in seconds.
HEARTBEAT_FREQUENCY = 10

# Spec requires at least 10ms between ismaster calls.
MIN_HEARTBEAT_INTERVAL = 0.01

# mongod/s 2.6 and above return code 59 when a
# command doesn't exist. mongod versions previous
# to 2.6 and mongos 2.4.x return no error code
# when a command does exist. mongos versions previous
# to 2.4.0 return code 13390 when a command does not
# exist.
COMMAND_NOT_FOUND_CODES = (59, 13390, None)


def partition_node(node):
    """Split a host:port string into (host, int(port)) pair."""
    host = node
    port = 27017
    idx = node.rfind(':')
    if idx != -1:
        host, port = node[:idx], int(node[idx + 1:])
    if host.startswith('['):
        host = host[1:-1]
    return host, port


def raise_config_error(key, dummy):
    """Raise ConfigurationError with the given key name."""
    raise ConfigurationError("Unknown option %s" % (key,))


# Mapping of URI uuid representation options to valid subtypes.
_UUID_SUBTYPES = {
    'standard': UUID_SUBTYPE,
    'pythonLegacy': OLD_UUID_SUBTYPE,
    'javaLegacy': JAVA_LEGACY,
    'csharpLegacy': CSHARP_LEGACY
}


def validate_boolean(option, value):
    """Validates that 'value' is 'true' or 'false'.
    """
    if isinstance(value, bool):
        return value
    elif isinstance(value, string_type):
        if value not in ('true', 'false'):
            raise ConfigurationError("The value of %s must be "
                                     "'true' or 'false'" % (option,))
        return value == 'true'
    raise TypeError("Wrong type for %s, value must be a boolean" % (option,))


def validate_integer(option, value):
    """Validates that 'value' is an integer (or basestring representation).
    """
    if isinstance(value, integer_types):
        return value
    elif isinstance(value, string_type):
        if not value.isdigit():
            raise ConfigurationError("The value of %s must be "
                                     "an integer" % (option,))
        return int(value)
    raise TypeError("Wrong type for %s, value must be an integer" % (option,))


def validate_positive_integer(option, value):
    """Validate that 'value' is a positive integer.
    """
    val = validate_integer(option, value)
    if val < 0:
        raise ConfigurationError("The value of %s must be "
                                 "a positive integer" % (option,))
    return val


def validate_readable(option, value):
    """Validates that 'value' is file-like and readable.
    """
    # First make sure its a string py3.3 open(True, 'r') succeeds
    # Used in ssl cert checking due to poor ssl module error reporting
    value = validate_string(option, value)
    open(value, 'r').close()
    return value


def validate_positive_integer_or_none(option, value):
    """Validate that 'value' is a positive integer or None.
    """
    if value is None:
        return value
    return validate_positive_integer(option, value)


def validate_string(option, value):
    """Validates that 'value' is an instance of `basestring` for Python 2
    or `str` for Python 3.
    """
    if isinstance(value, string_type):
        return value
    raise TypeError("Wrong type for %s, value must be "
                    "an instance of %s" % (option, string_type.__name__))


def validate_int_or_basestring(option, value):
    """Validates that 'value' is an integer or string.
    """
    if isinstance(value, integer_types):
        return value
    elif isinstance(value, string_type):
        if value.isdigit():
            return int(value)
        return value
    raise TypeError("Wrong type for %s, value must be an "
                    "integer or a string" % (option,))


def validate_positive_float(option, value):
    """Validates that 'value' is a float, or can be converted to one, and is
       positive.
    """
    err = ConfigurationError("%s must be a positive int or float" % (option,))
    try:
        value = float(value)
    except (ValueError, TypeError):
        raise err

    # float('inf') doesn't work in 2.4 or 2.5 on Windows, so just cap floats at
    # one billion - this is a reasonable approximation for infinity
    if not 0 < value < 1e9:
        raise err

    return value


def validate_timeout_or_none(option, value):
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds.
    """
    if value is None:
        return value
    return validate_positive_float(option, value) / 1000.0


def validate_read_preference(dummy, value):
    """Validate a read preference.
    """
    if not isinstance(value, ServerMode):
        raise ConfigurationError("%r is not a "
                                 "valid read preference." % (value,))
    return value


def validate_read_preference_mode(dummy, name):
    """Validate read preference mode for a MongoReplicaSetClient.
    """
    try:
        return read_pref_mode_from_name(name)
    except ValueError:
        raise ConfigurationError("Not a valid read preference")


def validate_auth_mechanism(option, value):
    """Validate the authMechanism URI option.
    """
    # CRAM-MD5 is for server testing only. Undocumented,
    # unsupported, may be removed at any time. You have
    # been warned.
    if value not in MECHANISMS and value != 'CRAM-MD5':
        raise ConfigurationError("%s must be in "
                                 "%s" % (option, MECHANISMS))
    return value


def validate_uuid_representation(dummy, value):
    """Validate the uuid representation option selected in the URI.
    """
    if value not in _UUID_SUBTYPES:
        raise ConfigurationError("%s is an invalid UUID representation. "
                                 "Must be one of "
                                 "%s" % (value, list(_UUID_SUBTYPES)))
    return _UUID_SUBTYPES[value]


def validate_uuid_subtype(dummy, value):
    """Validate the uuid subtype option, a numerical value whose acceptable
    values are defined in bson.binary."""
    if value not in _UUID_SUBTYPES.values():
        raise ConfigurationError("Not a valid setting for uuid_subtype.")
    return value


def validate_read_preference_tags(name, value):
    """Parse readPreferenceTags if passed as a client kwarg.
    """
    if not isinstance(value, list):
        value = [value]

    tag_sets = []
    for tag_set in value:
        if tag_set == '':
            tag_sets.append({})
            continue
        try:
            tag_sets.append(dict([tag.split(":")
                                  for tag in tag_set.split(",")]))
        except Exception:
            raise ConfigurationError("%r not a valid "
                                     "value for %s" % (tag_set, name))
    return tag_sets


# journal is an alias for j,
# wtimeoutms is an alias for wtimeout,
VALIDATORS = {
    'replicaset': validate_string,
    'w': validate_int_or_basestring,
    'wtimeout': validate_integer,
    'wtimeoutms': validate_integer,
    'fsync': validate_boolean,
    'j': validate_boolean,
    'journal': validate_boolean,
    'connecttimeoutms': validate_timeout_or_none,
    'max_pool_size': validate_positive_integer_or_none,
    'socketkeepalive': validate_boolean,
    'sockettimeoutms': validate_timeout_or_none,
    'waitqueuetimeoutms': validate_timeout_or_none,
    'waitqueuemultiple': validate_positive_integer_or_none,
    'ssl': validate_boolean,
    'ssl_keyfile': validate_readable,
    'ssl_certfile': validate_readable,
    'ssl_cert_reqs': validate_cert_reqs,
    'ssl_ca_certs': validate_readable,
    'read_preference': validate_read_preference,
    'readpreference': validate_read_preference_mode,
    'readpreferencetags': validate_read_preference_tags,
    'latencythresholdms': validate_positive_float,
    'secondaryacceptablelatencyms': validate_positive_float,
    'auto_start_request': validate_boolean,
    'authmechanism': validate_auth_mechanism,
    'authsource': validate_string,
    'gssapiservicename': validate_string,
    'uuidrepresentation': validate_uuid_representation,
}


_AUTH_OPTIONS = frozenset(['gssapiservicename'])


def validate_auth_option(option, value):
    """Validate optional authentication parameters.
    """
    lower, value = validate(option, value)
    if lower not in _AUTH_OPTIONS:
        raise ConfigurationError('Unknown '
                                 'authentication option: %s' % (option,))
    return lower, value


def validate(option, value):
    """Generic validation function.
    """
    lower = option.lower()
    validator = VALIDATORS.get(lower, raise_config_error)
    value = validator(option, value)
    return lower, value


WRITE_CONCERN_OPTIONS = frozenset([
    'w',
    'wtimeout',
    'wtimeoutms',
    'fsync',
    'j',
    'journal'
])


class BaseObject(object):
    """A base class that provides attributes and methods common
    to multiple pymongo classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO MONGODB.
    """

    def __init__(self, read_preference, uuid_subtype, write_concern):

        self.__read_pref = read_preference
        self.__uuid_subtype = uuid_subtype or OLD_UUID_SUBTYPE
        self.__write_concern = WriteConcern(**write_concern)

    def __set_write_concern(self, value):
        """Property setter for write_concern."""
        if not isinstance(value, collections.Mapping):
            raise ConfigurationError("write_concern must be an "
                                     "instance of dict or a subclass.")
        self.__write_concern = WriteConcern(**value)

    def __get_write_concern(self):
        """The default write concern for this instance.

        Supports dict style access for getting/setting write concern
        options. Valid options include:

        - `w`: (integer or string) If this is a replica set, write operations
          will block until they have been replicated to the specified number
          or tagged set of servers. `w=<int>` always includes the replica set
          primary (e.g. w=3 means write to the primary and wait until
          replicated to **two** secondaries). **Setting w=0 disables write
          acknowledgement and all other write concern options.**
        - `wtimeout`: (integer) Used in conjunction with `w`. Specify a value
          in milliseconds to control how long to wait for write propagation
          to complete. If replication does not complete in the given
          timeframe, a timeout exception is raised.
        - `j`: If ``True`` block until write operations have been committed
          to the journal. Cannot be used in combination with `fsync`. Prior
          to MongoDB 2.6 this option was ignored if the server was running
          without journaling. Starting with MongoDB 2.6 write operations will
          fail with an exception if this option is used when the server is
          running without journaling.
        - `fsync`: If ``True`` and the server is running without journaling,
          blocks until the server has synced all data files to disk. If the
          server is running with journaling, this acts the same as the `j`
          option, blocking until write operations have been committed to the
          journal. Cannot be used in combination with `j`.

        >>> m = pymongo.MongoClient()
        >>> m.write_concern
        {}
        >>> m.write_concern = {'w': 2, 'wtimeout': 1000}
        >>> m.write_concern
        {'wtimeout': 1000, 'w': 2}
        >>> m.write_concern['j'] = True
        >>> m.write_concern
        {'wtimeout': 1000, 'j': True, 'w': 2}
        >>> m.write_concern = {'j': True}
        >>> m.write_concern
        {'j': True}
        >>> # Disable write acknowledgement and write concern
        ...
        >>> m.write_concern['w'] = 0


        .. note:: Accessing :attr:`write_concern` returns its value
           (a subclass of :class:`dict`), not a copy.
        """
        # To support dict style access we have to return the actual
        # WriteConcern here, not a copy.
        return self.__write_concern.document

    write_concern = property(__get_write_concern, __set_write_concern)

    def __get_read_pref(self):
        """The read preference mode for this instance.

        See :class:`~pymongo.read_preferences.ReadPreference` for
        available options.

        .. versionadded:: 2.1
        """
        return self.__read_pref

    def __set_read_pref(self, value):
        """Property setter for read_preference"""
        self.__read_pref = validate_read_preference(None, value)

    read_preference = property(__get_read_pref, __set_read_pref)

    def __get_latency(self):
        return self.__read_pref.latency_threshold_ms

    def __set_latency(self, latency):
        warnings.warn("The secondary_acceptable_latency_ms attribute is "
                      "deprecated", DeprecationWarning, stacklevel=2)
        mode = self.__read_pref.mode
        tag_sets = self.__read_pref.tag_sets
        self.__read_pref = make_read_preference(mode, latency, tag_sets)

    secondary_acceptable_latency_ms = property(__get_latency, __set_latency)

    def __get_tags(self):
        return self.__read_pref.tag_sets

    def __set_tags(self, tag_sets):
        warnings.warn("The tag_sets attribute is deprecated",
                      DeprecationWarning, stacklevel=2)
        mode = self.__read_pref.mode
        latency = self.__read_pref.latency_threshold_ms
        self.__read_pref = make_read_preference(mode, latency, tag_sets)

    tag_sets = property(__get_tags, __set_tags)

    def __get_uuid_subtype(self):
        """This attribute specifies which BSON Binary subtype is used when
        storing UUIDs. Historically UUIDs have been stored as BSON Binary
        subtype 3. This attribute is used to switch to the newer BSON Binary
        subtype 4. It can also be used to force legacy byte order and subtype
        compatibility with the Java and C# drivers. See the :mod:`bson.binary`
        module for all options."""
        return self.__uuid_subtype

    def __set_uuid_subtype(self, value):
        """Sets the BSON Binary subtype to be used when storing UUIDs."""
        self.__uuid_subtype = validate_uuid_subtype("uuid_subtype", value)

    uuid_subtype = property(__get_uuid_subtype, __set_uuid_subtype)

    def _get_wc_override(self):
        """Get write concern override.

        Used in internal methods that **must** do acknowledged write ops.
        We don't want to override user write concern options if write concern
        is already enabled.
        """
        if self.__write_concern.acknowledged:
            return {}
        return {'w': 1}


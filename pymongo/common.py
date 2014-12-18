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
from pymongo.codec_options import CodecOptions
from pymongo.errors import ConfigurationError
from pymongo.read_preferences import (make_read_preference,
                                      read_pref_mode_from_name,
                                      ServerMode)
from pymongo.ssl_support import validate_cert_reqs
from pymongo.write_concern import WriteConcern
from bson.binary import (STANDARD, PYTHON_LEGACY,
                         JAVA_LEGACY, CSHARP_LEGACY)
from bson.py3compat import string_type, integer_types

# Defaults until we connect to a server and get updated limits.
MAX_BSON_SIZE = 16 * (1024 ** 2)
MAX_MESSAGE_SIZE = 2 * MAX_BSON_SIZE
MIN_WIRE_VERSION = 0
MAX_WIRE_VERSION = 0
MAX_WRITE_BATCH_SIZE = 1000

# What this version of PyMongo supports.
MIN_SUPPORTED_WIRE_VERSION = 0
MAX_SUPPORTED_WIRE_VERSION = 3

# Frequency to call ismaster on servers, in seconds.
HEARTBEAT_FREQUENCY = 10

# Frequency to process kill-cursors, in seconds. See MongoClient.close_cursor.
KILL_CURSOR_FREQUENCY = 1

# How long to wait, in seconds, for a suitable server to be found before
# aborting an operation. For example, if the client attempts an insert
# during a replica set election, SERVER_WAIT_TIME governs the longest it
# is willing to wait for a new primary to be found.
SERVER_WAIT_TIME = 5

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


def clean_node(node):
    """Split and normalize a node name from an ismaster response."""
    host, port = partition_node(node)

    # Normalize hostname to lowercase, since DNS is case-insensitive:
    # http://tools.ietf.org/html/rfc4343
    # This prevents useless rediscovery if "foo.com" is in the seed list but
    # "FOO.com" is in the ismaster response.
    return host.lower(), port


def raise_config_error(key, dummy):
    """Raise ConfigurationError with the given key name."""
    raise ConfigurationError("Unknown option %s" % (key,))


# Mapping of URI uuid representation options to valid subtypes.
_UUID_REPRESENTATIONS = {
    'standard': STANDARD,
    'pythonLegacy': PYTHON_LEGACY,
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
    if value is None:
        return value
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


def validate_string_or_none(option, value):
    """Validates that 'value' is an instance of `basestring` or `None`.
    """
    if value is None:
        return value
    return validate_string(option, value)


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
    try:
        return _UUID_REPRESENTATIONS[value]
    except KeyError:
        raise ConfigurationError("%s is an invalid UUID representation. "
                                 "Must be one of "
                                 "%s" % (value, tuple(_UUID_REPRESENTATIONS)))


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


_MECHANISM_PROPS = frozenset(['SERVICE_NAME'])


def validate_auth_mechanism_properties(option, value):
    """Validate authMechanismProperties."""
    value = validate_string(option, value)
    props = {}
    for opt in value.split(','):
        try:
            key, val = opt.split(':')
            if key not in _MECHANISM_PROPS:
                raise ConfigurationError("%s is not a supported auth "
                                         "mechanism property. Must be one of "
                                         "%s." % (key, tuple(_MECHANISM_PROPS)))
            props[key] = val
        except ValueError:
            raise ConfigurationError("auth mechanism properties must be "
                                     "key:value pairs like SERVICE_NAME:"
                                     "mongodb, not %s." % (opt,))
    return props


def validate_document_class(option, value):
    if not issubclass(value, collections.MutableMapping):
        raise ConfigurationError("%s must be a sublass of "
                                 "collections.MutableMapping" % (option,))
    return value


# journal is an alias for j,
# wtimeoutms is an alias for wtimeout,
VALIDATORS = {
    'replicaset': validate_string_or_none,
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
    'authmechanism': validate_auth_mechanism,
    'authsource': validate_string,
    'authmechanismproperties': validate_auth_mechanism_properties,
    'document_class': validate_document_class,
    'tz_aware': validate_boolean,
    'uuidrepresentation': validate_uuid_representation,
}


_AUTH_OPTIONS = frozenset(['authmechanismproperties'])


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

    def __init__(self, codec_options, read_preference, write_concern):

        if not isinstance(codec_options, CodecOptions):
            raise TypeError("codec_options must be an instance of "
                            "pymongo.codec_options.CodecOptions")
        self.__codec_options = codec_options

        # TODO: Better error reporting for read preference.
        if not isinstance(read_preference, ServerMode):
            raise TypeError("read_preference is invalid")
        self.__read_pref = read_preference

        if not isinstance(write_concern, WriteConcern):
            raise TypeError("write_concern must be an instance of "
                            "pymongo.write_concern.WriteConcern")
        self.__write_concern = write_concern

    @property
    def codec_options(self):
        """An instance of :class:`~pymongo.codec_options.CodecOptions`."""
        return self.__codec_options

    def __set_write_concern(self, value):
        """Property setter for write_concern."""
        if not isinstance(value, WriteConcern):
            raise TypeError("write_concern must be an instance "
                            "of pymongo.write_concern.WriteConcern.")
        self.__write_concern = value

    def __get_write_concern(self):
        """The :class:`~pymongo.write_concern.WriteConcern` for this instance.
        """
        return self.__write_concern

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
        """Deprecated. Use ``client.read_preference.latency_threshold_ms``.

        See :class:`~pymongo.read_preferences.ReadPreference`.

        .. note:: ``secondary_acceptable_latency_ms`` is ignored when talking
          to a replica set *through* a mongos. The equivalent is the
          localThreshold_ command line option.

        .. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption-mongos--localThreshold
        """
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

    def _get_wc_override(self):
        """Get write concern override.

        Used in internal methods that **must** do acknowledged write ops.
        We don't want to override user write concern options if write concern
        is already enabled.
        """
        if self.__write_concern.acknowledged:
            return {}
        return {'w': 1}


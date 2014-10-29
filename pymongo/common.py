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
import sys
import warnings
from pymongo import read_preferences

from pymongo.auth import MECHANISMS
from pymongo.read_preferences import ReadPreference
from pymongo.errors import ConfigurationError
from bson.binary import (OLD_UUID_SUBTYPE, UUID_SUBTYPE,
                         JAVA_LEGACY, CSHARP_LEGACY)

HAS_SSL = True
try:
    import ssl
except ImportError:
    HAS_SSL = False


# Jython 2.7 includes an incomplete ssl module. See PYTHON-498.
if sys.platform.startswith('java'):
    HAS_SSL = False


# Defaults until we connect to a server and get updated limits.
MAX_BSON_SIZE = 16 * (1024 ** 2)
MAX_MESSAGE_SIZE = 2 * MAX_BSON_SIZE
MIN_WIRE_VERSION = 0
MAX_WIRE_VERSION = 0
MAX_WRITE_BATCH_SIZE = 1000

# What this version of PyMongo supports.
MIN_SUPPORTED_WIRE_VERSION = 0
MAX_SUPPORTED_WIRE_VERSION = 3

# mongod/s 2.6 and above return code 59 when a
# command doesn't exist. mongod versions previous
# to 2.6 and mongos 2.4.x return no error code
# when a command does exist. mongos versions previous
# to 2.4.0 return code 13390 when a command does not
# exist.
COMMAND_NOT_FOUND_CODES = (59, 13390, None)


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
    elif isinstance(value, basestring):
        if value not in ('true', 'false'):
            raise ConfigurationError("The value of %s must be "
                                     "'true' or 'false'" % (option,))
        return value == 'true'
    raise TypeError("Wrong type for %s, value must be a boolean" % (option,))


def validate_integer(option, value):
    """Validates that 'value' is an integer (or basestring representation).
    """
    if isinstance(value, (int, long)):
        return value
    elif isinstance(value, basestring):
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
    value = validate_basestring(option, value)
    open(value, 'r').close()
    return value


def validate_cert_reqs(option, value):
    """Validate the cert reqs are valid. It must be None or one of the three
    values ``ssl.CERT_NONE``, ``ssl.CERT_OPTIONAL`` or ``ssl.CERT_REQUIRED``"""
    if value is None:
        return value
    if HAS_SSL:
        if value in (ssl.CERT_NONE, ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED):
            return value
        raise ConfigurationError("The value of %s must be one of: "
                                 "`ssl.CERT_NONE`, `ssl.CERT_OPTIONAL` or "
                                 "`ssl.CERT_REQUIRED" % (option,))
    else:
        raise ConfigurationError("The value of %s is set but can't be "
                                 "validated. The ssl module is not available"
                                 % (option,))


def validate_positive_integer_or_none(option, value):
    """Validate that 'value' is a positive integer or None.
    """
    if value is None:
        return value
    return validate_positive_integer(option, value)


def validate_basestring(option, value):
    """Validates that 'value' is an instance of `basestring`.
    """
    if isinstance(value, basestring):
        return value
    raise TypeError("Wrong type for %s, value must be an "
                    "instance of %s" % (option, basestring.__name__))


def validate_basestring_or_none(option, value):
    """Validates that 'value' is an instance of `basestring` or `None`.
    """
    if value is None:
        return value
    return validate_basestring(option, value)


def validate_int_or_basestring(option, value):
    """Validates that 'value' is an integer or string.
    """
    if isinstance(value, (int, long)):
        return value
    elif isinstance(value, basestring):
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
    """Validate read preference for a ReplicaSetConnection.
    """
    if value in read_preferences.modes:
        return value

    # Also allow string form of enum for uri_parser
    try:
        return read_preferences.mongos_enum(value)
    except ValueError:
        raise ConfigurationError("Not a valid read preference")


def validate_tag_sets(dummy, value):
    """Validate tag sets for a ReplicaSetConnection.
    """
    if value is None:
        return [{}]

    if not isinstance(value, list):
        raise ConfigurationError((
            "Tag sets %s invalid, must be a list") % repr(value))
    if len(value) == 0:
        raise ConfigurationError((
            "Tag sets %s invalid, must be None or contain at least one set of"
            " tags") % repr(value))

    for tags in value:
        if not isinstance(tags, dict):
            raise ConfigurationError(
                "Tag set %s invalid, must be a dict" % repr(tags))

    return value


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
    if value not in _UUID_SUBTYPES.keys():
        raise ConfigurationError("%s is an invalid UUID representation. "
                                 "Must be one of "
                                 "%s" % (value, _UUID_SUBTYPES.keys()))
    return _UUID_SUBTYPES[value]


def validate_uuid_subtype(dummy, value):
    """Validate the uuid subtype option, a numerical value whose acceptable
    values are defined in bson.binary."""
    if value not in _UUID_SUBTYPES.values():
        raise ConfigurationError("Not a valid setting for uuid_subtype.")
    return value


_MECHANISM_PROPS = frozenset(['SERVICE_NAME'])


def validate_auth_mechanism_properties(option, value):
    """Validate authMechanismProperties."""
    value = validate_basestring(option, value)
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


# jounal is an alias for j,
# wtimeoutms is an alias for wtimeout,
# readpreferencetags is an alias for tag_sets.
VALIDATORS = {
    'replicaset': validate_basestring_or_none,
    'slaveok': validate_boolean,
    'slave_okay': validate_boolean,
    'safe': validate_boolean,
    'w': validate_int_or_basestring,
    'wtimeout': validate_integer,
    'wtimeoutms': validate_integer,
    'fsync': validate_boolean,
    'j': validate_boolean,
    'journal': validate_boolean,
    'connecttimeoutms': validate_timeout_or_none,
    'sockettimeoutms': validate_timeout_or_none,
    'waitqueuetimeoutms': validate_timeout_or_none,
    'waitqueuemultiple': validate_positive_integer_or_none,
    'ssl': validate_boolean,
    'ssl_keyfile': validate_readable,
    'ssl_certfile': validate_readable,
    'ssl_cert_reqs': validate_cert_reqs,
    'ssl_ca_certs': validate_readable,
    'readpreference': validate_read_preference,
    'read_preference': validate_read_preference,
    'readpreferencetags': validate_tag_sets,
    'tag_sets': validate_tag_sets,
    'secondaryacceptablelatencyms': validate_positive_float,
    'secondary_acceptable_latency_ms': validate_positive_float,
    'auto_start_request': validate_boolean,
    'use_greenlets': validate_boolean,
    'authmechanism': validate_auth_mechanism,
    'authsource': validate_basestring,
    'gssapiservicename': validate_basestring,
    'authmechanismproperties': validate_auth_mechanism_properties,
    'uuidrepresentation': validate_uuid_representation,
    'socketkeepalive': validate_boolean
}


_AUTH_OPTIONS = frozenset(['gssapiservicename', 'authmechanismproperties'])


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


SAFE_OPTIONS = frozenset([
    'w',
    'wtimeout',
    'wtimeoutms',
    'fsync',
    'j',
    'journal'
])


class WriteConcern(dict):

    def __init__(self, *args, **kwargs):
        """A subclass of dict that overrides __setitem__ to
        validate write concern options.
        """
        super(WriteConcern, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        if key not in SAFE_OPTIONS:
            raise ConfigurationError("%s is not a valid write "
                                     "concern option." % (key,))
        key, value = validate(key, value)
        super(WriteConcern, self).__setitem__(key, value)


class BaseObject(object):
    """A base class that provides attributes and methods common
    to multiple pymongo classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO MONGODB.
    """

    def __init__(self, **options):

        self.__slave_okay = False
        self.__read_pref = ReadPreference.PRIMARY
        self.__tag_sets = [{}]
        self.__secondary_acceptable_latency_ms = 15
        self.__safe = None
        self.__uuid_subtype = OLD_UUID_SUBTYPE
        self.__write_concern = WriteConcern()
        self.__set_options(options)
        if (self.__read_pref == ReadPreference.PRIMARY
                and self.__tag_sets != [{}]):
            raise ConfigurationError(
                "ReadPreference PRIMARY cannot be combined with tags")

        # If safe hasn't been implicitly set by write concerns then set it.
        if self.__safe is None:
            if options.get("w") == 0:
                self.__safe = False
            else:
                self.__safe = validate_boolean('safe',
                                               options.get("safe", True))
        # Note: 'safe' is always passed by Connection and ReplicaSetConnection
        # Always do the most "safe" thing, but warn about conflicts.
        if self.__safe and options.get('w') == 0:

            warnings.warn("Conflicting write concerns: %s. Write concern "
                          "options were configured, but w=0 disables all "
                          "other options." % self.write_concern,
                          UserWarning)

    def __set_safe_option(self, option, value):
        """Validates and sets getlasterror options for this
        object (Connection, Database, Collection, etc.)
        """
        if value is None:
            self.__write_concern.pop(option, None)
        else:
            self.__write_concern[option] = value
            if option != "w" or value != 0:
                self.__safe = True

    def __set_options(self, options):
        """Validates and sets all options passed to this object."""
        for option, value in options.iteritems():
            if option in ('slave_okay', 'slaveok'):
                self.__slave_okay = validate_boolean(option, value)
            elif option in ('read_preference', "readpreference"):
                self.__read_pref = validate_read_preference(option, value)
            elif option in ('tag_sets', 'readpreferencetags'):
                self.__tag_sets = validate_tag_sets(option, value)
            elif option == 'uuidrepresentation':
                self.__uuid_subtype = validate_uuid_subtype(option, value)
            elif option in (
                'secondaryacceptablelatencyms',
                'secondary_acceptable_latency_ms'
            ):
                self.__secondary_acceptable_latency_ms = \
                    validate_positive_float(option, value)
            elif option in SAFE_OPTIONS:
                if option == 'journal':
                    self.__set_safe_option('j', value)
                elif option == 'wtimeoutms':
                    self.__set_safe_option('wtimeout', value)
                else:
                    self.__set_safe_option(option, value)

    def __set_write_concern(self, value):
        """Property setter for write_concern."""
        if not isinstance(value, dict):
            raise ConfigurationError("write_concern must be an "
                                     "instance of dict or a subclass.")
        # Make a copy here to avoid users accidentally setting the
        # same dict on multiple instances.
        wc = WriteConcern()
        for k, v in value.iteritems():
            # Make sure we validate each option.
            wc[k] = v
        self.__write_concern = wc

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

        .. warning:: If you are using :class:`~pymongo.connection.Connection`
           or :class:`~pymongo.replica_set_connection.ReplicaSetConnection`
           make sure you explicitly set ``w`` to 1 (or a greater value) or
           :attr:`safe` to ``True``. Unlike calling
           :meth:`set_lasterror_options`, setting an option in
           :attr:`write_concern` does not implicitly set :attr:`safe`
           to ``True``.
        """
        # To support dict style access we have to return the actual
        # WriteConcern here, not a copy.
        return self.__write_concern

    write_concern = property(__get_write_concern, __set_write_concern)

    def __get_slave_okay(self):
        """DEPRECATED. Use :attr:`read_preference` instead.

        .. versionchanged:: 2.1
           Deprecated slave_okay.
        .. versionadded:: 2.0
        """
        return self.__slave_okay

    def __set_slave_okay(self, value):
        """Property setter for slave_okay"""
        warnings.warn("slave_okay is deprecated. Please use "
                      "read_preference instead.", DeprecationWarning,
                      stacklevel=2)
        self.__slave_okay = validate_boolean('slave_okay', value)

    slave_okay = property(__get_slave_okay, __set_slave_okay)

    def __get_read_pref(self):
        """The read preference mode for this instance.

        See :class:`~pymongo.read_preferences.ReadPreference` for
        available options.

        .. versionadded:: 2.1
        """
        return self.__read_pref

    def __set_read_pref(self, value):
        """Property setter for read_preference"""
        self.__read_pref = validate_read_preference('read_preference', value)

    read_preference = property(__get_read_pref, __set_read_pref)

    def __get_acceptable_latency(self):
        """Any replica-set member whose ping time is within
        secondary_acceptable_latency_ms of the nearest member may accept
        reads. Defaults to 15 milliseconds.

        See :class:`~pymongo.read_preferences.ReadPreference`.

        .. versionadded:: 2.3

        .. note:: ``secondary_acceptable_latency_ms`` is ignored when talking
          to a replica set *through* a mongos. The equivalent is the
          localThreshold_ command line option.

        .. _localThreshold: http://docs.mongodb.org/manual/reference/mongos/#cmdoption-mongos--localThreshold
        """
        return self.__secondary_acceptable_latency_ms

    def __set_acceptable_latency(self, value):
        """Property setter for secondary_acceptable_latency_ms"""
        self.__secondary_acceptable_latency_ms = (validate_positive_float(
            'secondary_acceptable_latency_ms', value))

    secondary_acceptable_latency_ms = property(
        __get_acceptable_latency, __set_acceptable_latency)

    def __get_tag_sets(self):
        """Set ``tag_sets`` to a list of dictionaries like [{'dc': 'ny'}] to
        read only from members whose ``dc`` tag has the value ``"ny"``.
        To specify a priority-order for tag sets, provide a list of
        tag sets: ``[{'dc': 'ny'}, {'dc': 'la'}, {}]``. A final, empty tag
        set, ``{}``, means "read from any member that matches the mode,
        ignoring tags." ReplicaSetConnection tries each set of tags in turn
        until it finds a set of tags with at least one matching member.

           .. seealso:: `Data-Center Awareness
               <http://www.mongodb.org/display/DOCS/Data+Center+Awareness>`_

        .. versionadded:: 2.3
        """
        return self.__tag_sets

    def __set_tag_sets(self, value):
        """Property setter for tag_sets"""
        self.__tag_sets = validate_tag_sets('tag_sets', value)

    tag_sets = property(__get_tag_sets, __set_tag_sets)

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

    def __get_safe(self):
        """**DEPRECATED:** Use the 'w' :attr:`write_concern` option instead.

        Use getlasterror with every write operation?

        .. versionadded:: 2.0
        """
        return self.__safe

    def __set_safe(self, value):
        """Property setter for safe"""
        warnings.warn("safe is deprecated. Please use the"
                      " 'w' write_concern option instead.",
                      DeprecationWarning, stacklevel=2)
        self.__safe = validate_boolean('safe', value)

    safe = property(__get_safe, __set_safe)

    def get_lasterror_options(self):
        """DEPRECATED: Use :attr:`write_concern` instead.

        Returns a dict of the getlasterror options set on this instance.

        .. versionchanged:: 2.4
           Deprecated get_lasterror_options.
        .. versionadded:: 2.0
        """
        warnings.warn("get_lasterror_options is deprecated. Please use "
                      "write_concern instead.", DeprecationWarning,
                      stacklevel=2)
        return self.__write_concern.copy()

    def set_lasterror_options(self, **kwargs):
        """DEPRECATED: Use :attr:`write_concern` instead.

        Set getlasterror options for this instance.

        Valid options include j=<bool>, w=<int/string>, wtimeout=<int>,
        and fsync=<bool>. Implies safe=True.

        :Parameters:
            - `**kwargs`: Options should be passed as keyword
                          arguments (e.g. w=2, fsync=True)

        .. versionchanged:: 2.4
           Deprecated set_lasterror_options.
        .. versionadded:: 2.0
        """
        warnings.warn("set_lasterror_options is deprecated. Please use "
                      "write_concern instead.", DeprecationWarning,
                      stacklevel=2)
        for key, value in kwargs.iteritems():
            self.__set_safe_option(key, value)

    def unset_lasterror_options(self, *options):
        """DEPRECATED: Use :attr:`write_concern` instead.

        Unset getlasterror options for this instance.

        If no options are passed unsets all getlasterror options.
        This does not set `safe` to False.

        :Parameters:
            - `*options`: The list of options to unset.

        .. versionchanged:: 2.4
           Deprecated unset_lasterror_options.
        .. versionadded:: 2.0
        """
        warnings.warn("unset_lasterror_options is deprecated. Please use "
                      "write_concern instead.", DeprecationWarning,
                      stacklevel=2)
        if len(options):
            for option in options:
                self.__write_concern.pop(option, None)
        else:
            self.__write_concern = WriteConcern()

    def _get_wc_override(self):
        """Get write concern override.

        Used in internal methods that **must** do acknowledged write ops.
        We don't want to override user write concern options if write concern
        is already enabled.
        """
        if self.safe and self.__write_concern.get('w') != 0:
            return {}
        return {'w': 1}

    def _get_write_mode(self, safe=None, **options):
        """Get the current write mode.

        Determines if the current write is safe or not based on the
        passed in or inherited safe value, write_concern values, or
        passed options.

        :Parameters:
            - `safe`: check that the operation succeeded?
            - `**options`: overriding write concern options.

        .. versionadded:: 2.3
        """
        if safe is not None:
            warnings.warn("The safe parameter is deprecated. Please use "
                          "write concern options instead.", DeprecationWarning,
                          stacklevel=3)
            validate_boolean('safe', safe)

        # Passed options override collection level defaults.
        if safe is not None or options:
            if safe or options:
                if not options:
                    options = self.__write_concern.copy()
                    # Backwards compatability edge case. Call getLastError
                    # with no options if safe=True was passed but collection
                    # level defaults have been disabled with w=0.
                    # These should be equivalent:
                    # Connection(w=0).foo.bar.insert({}, safe=True)
                    # MongoClient(w=0).foo.bar.insert({}, w=1)
                    if options.get('w') == 0:
                        return True, {}
                # Passing w=0 overrides passing safe=True.
                return options.get('w') != 0, options
            return False, {}

        # Fall back to collection level defaults.
        # w=0 takes precedence over self.safe = True
        if self.__write_concern.get('w') == 0:
            return False, {}
        elif self.safe or self.__write_concern.get('w', 0) != 0:
            return True, self.__write_concern.copy()

        return False, {}

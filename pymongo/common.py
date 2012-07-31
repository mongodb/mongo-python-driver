# Copyright 2011-2012 10gen, Inc.
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
import warnings
from pymongo import read_preferences

from pymongo.read_preferences import ReadPreference
from pymongo.errors import ConfigurationError


def raise_config_error(key, dummy):
    """Raise ConfigurationError with the given key name."""
    raise ConfigurationError("Unknown option %s" % (key,))


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


def validate_basestring(option, value):
    """Validates that 'value' is an instance of `basestring`.
    """
    if isinstance(value, basestring):
        return value
    raise TypeError("Wrong type for %s, value must be an "
                    "instance of %s" % (option, basestring.__name__))


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
    if value <= 0:
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
    if value not in read_preferences.modes:
        raise ConfigurationError("Not a valid read preference")
    return value


def validate_tag_sets(dummy, value):
    """Validate tag sets for a ReplicaSetConnection.
    """
    if value is None:
        return [{}]

    if not isinstance(value, list):
        raise ConfigurationError((
            "Tag sets %s invalid, must be a list" ) % repr(value))
    if len(value) == 0:
        raise ConfigurationError((
            "Tag sets %s invalid, must be None or contain at least one set of"
            " tags") % repr(value))

    for tags in value:
        if not isinstance(tags, dict):
            raise ConfigurationError(
                "Tag set %s invalid, must be a dict" % repr(tags))

    return value


# jounal is an alias for j,
# wtimeoutms is an alias for wtimeout
VALIDATORS = {
    'replicaset': validate_basestring,
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
    'ssl': validate_boolean,
    'read_preference': validate_read_preference,
    'tag_sets': validate_tag_sets,
    'secondaryacceptablelatencyms': validate_positive_float,
    'secondary_acceptable_latency_ms': validate_positive_float,
    'auto_start_request': validate_boolean,
    'use_greenlets': validate_boolean,
}


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


class BaseObject(object):
    """A base class that provides attributes and methods common
    to multiple pymongo classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO 10GEN
    """

    def __init__(self, **options):

        self.__slave_okay = False
        self.__read_pref = ReadPreference.PRIMARY
        self.__tag_sets = [{}]
        self.__secondary_acceptable_latency_ms = 15
        self.__safe = None
        self.__safe_opts = {}
        self.__set_options(options)
        if (self.__read_pref == ReadPreference.PRIMARY
            and self.__tag_sets != [{}]
        ):
            raise ConfigurationError(
                "ReadPreference PRIMARY cannot be combined with tags")

        # If safe hasn't been implicitly set by write concerns then set it.
        if self.__safe is None:
            self.__safe = validate_boolean('safe', options.get("safe", False))
        if self.__safe and not options.get("safe", True):
            warnings.warn("Conflicting write concerns.  Safe set as False "
                          "but write concerns have been set making safe True. "
                          "Please set safe to True.", UserWarning)

    def __set_safe_option(self, option, value, check=False):
        """Validates and sets getlasterror options for this
        object (Connection, Database, Collection, etc.)
        """
        if value is None:
            self.__safe_opts.pop(option, None)
        else:
            if check:
                option, value = validate(option, value)
            self.__safe_opts[option] = value
            self.__safe = True

    def __set_options(self, options):
        """Validates and sets all options passed to this object."""
        for option, value in options.iteritems():
            if option in ('slave_okay', 'slaveok'):
                self.__slave_okay = validate_boolean(option, value)
            elif option == 'read_preference':
                self.__read_pref = validate_read_preference(option, value)
            elif option == 'tag_sets':
                self.__tag_sets = validate_tag_sets(option, value)
            elif option in (
                'secondaryAcceptableLatencyMS',
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

    def __get_slave_okay(self):
        """DEPRECATED. Use `read_preference` instead.

        .. versionchanged:: 2.1
           Deprecated slave_okay.
        .. versionadded:: 2.0
        """
        return self.__slave_okay

    def __set_slave_okay(self, value):
        """Property setter for slave_okay"""
        warnings.warn("slave_okay is deprecated. Please use "
                      "read_preference instead.", DeprecationWarning)
        self.__slave_okay = validate_boolean('slave_okay', value)

    slave_okay = property(__get_slave_okay, __set_slave_okay)

    def __get_read_pref(self):
        """The read preference mode for this instance.

        See :class:`~pymongo.read_preferences.ReadPreference` for available options.

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

    def __get_safe(self):
        """Use getlasterror with every write operation?

        .. versionadded:: 2.0
        """
        return self.__safe

    def __set_safe(self, value):
        """Property setter for safe"""
        self.__safe = validate_boolean('safe', value)

    safe = property(__get_safe, __set_safe)

    def get_lasterror_options(self):
        """Returns a dict of the getlasterror options set
        on this instance.

        .. versionadded:: 2.0
        """
        return self.__safe_opts.copy()

    def set_lasterror_options(self, **kwargs):
        """Set getlasterror options for this instance.

        Valid options include j=<bool>, w=<int>, wtimeout=<int>,
        and fsync=<bool>. Implies safe=True.

        :Parameters:
            - `**kwargs`: Options should be passed as keyword
                          arguments (e.g. w=2, fsync=True)

        .. versionadded:: 2.0
        """
        for key, value in kwargs.iteritems():
            self.__set_safe_option(key, value, check=True)

    def unset_lasterror_options(self, *options):
        """Unset getlasterror options for this instance.

        If no options are passed unsets all getlasterror options.
        This does not set `safe` to False.

        :Parameters:
            - `*options`: The list of options to unset.

        .. versionadded:: 2.0
        """
        if len(options):
            for option in options:
                self.__safe_opts.pop(option, None)
        else:
            self.__safe_opts = {}

    def _get_safe_and_lasterror_options(self, safe=None, **options):
        """Get the current safe mode and any getLastError options.

        Determines if the current write is safe or not based on the
        passed in or inherited safe value.  Passing any write concerns
        automatically sets safe to True.

        :Parameters:
            - `safe`: check that the operation succeeded?
            - `**options`: overriding getLastError options

        .. versionadded:: 2.3
        """
        if safe is None:
            safe = self.safe
        safe = validate_boolean('safe', safe)
        if safe or options:
            safe = True
            if not options:
                options.update(self.get_lasterror_options())
        return safe, options

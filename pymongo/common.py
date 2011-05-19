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


"""Functions and classes common to multiple pymongo modules."""

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
            raise ConfigurationError("The value of '%s' must be "
                                     "'true' or 'false'" % (option,))
        return value == 'true'
    raise TypeError("Wrong type for %s, value must "
                    "be a boolean or string representation" % (option,))


def validate_integer(option, value):
    """Validates that 'value' is an integer.
    """
    if isinstance(value, (int, long)):
        return value
    elif isinstance(value, basestring):
        if not value.isdigit():
            raise ConfigurationError("The value of '%s' must be "
                                     "an integer." % (option,))
        return int(value)
    raise TypeError("Wrong type for %s, value just be an "
                    "integer or string representation" % (option,))


def noop(dummy, value):
    """Do nothing..."""
    return value


VALIDATORS = { 
    'replicaset': noop,
    'slaveok': validate_boolean,
    'safe': validate_boolean,
    'w': validate_integer,
    'wtimeout': validate_integer,
    'fsync': validate_boolean,
    'j': validate_boolean,
    'maxpoolsize': validate_integer,
}


UNSUPPORTED = frozenset([
    'connect',
    'minpoolsize',
    'waitqueuetimeoutms',
    'waitqueuemultiple',
    'connecttimeoutms',
    'sockettimeoutms'
])


SAFE_OPTIONS = frozenset([
    'w',
    'wtimeout',
    'fsync',
    'j'
])


class BaseObject(object):
    """A base class that provides attributes and methods common
    to multiple pymongo classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO 10GEN
    """

    def __init__(self, **options):

        self._slave_okay = False
        self._safe = False
        self.__safe_opts = {}

        self.__set_options(**options)

    def __set_safe_option(self, option, value):
        """Validates and sets getlasterror options for this
        object (Connection, Database, Collection, etc.)
        """
        if value is None:
            self.__safe_opts.pop(option, None)
        else:
            validate = VALIDATORS.get(option, raise_config_error)
            self.__safe_opts[option] = validate(option, value)
            self._safe = True

    def __set_options(self, **options):
        """Validates and sets all options passed to this object."""
        for option, value in options.iteritems():
            if option in ('slave_okay', 'slaveok'):
                self.slave_okay = value
            elif option == 'safe':
                self.safe = value
            elif option in SAFE_OPTIONS:
                self.__set_safe_option(option, value)

    def __get_slave_okay(self):
        """Is it OK to perform queries on a secondary or slave?

        .. versionadded:: 1.11+
        """
        return self._slave_okay

    def __set_slave_okay(self, value):
        """Property setter for slave_okay"""
        self.__dict__['_slave_okay'] = validate_boolean('slave_okay', value)

    slave_okay = property(__get_slave_okay, __set_slave_okay)

    def __get_safe(self):
        """Use getlasterrer with every write operation?

        .. versionadded:: 1.11+
        """
        return self._safe

    def __set_safe(self, value):
        """Property setter for safe"""
        self.__dict__['_safe'] = validate_boolean('safe', value)

    safe = property(__get_safe, __set_safe)

    def get_lasterror_options(self):
        """Returns a dict of the getlasterror options set
        on this instance.

        .. versionadded:: 1.11+
        """
        return self.__safe_opts.copy()

    def set_lasterror_options(self, **kwargs):
        """Set getlasterror options for this instance.

        Valid options include j=<bool>, w=<int>, wtimeout=<int>,
        and fsync=<bool>. Implies safe=True.

        :Parameters:
            - `**kwargs`: Options should be passed as keyword
                          arguments (e.g. w=2, fsync=True)

        .. versionadded:: 1.11+
        """
        for key, value in kwargs.iteritems():
            self.__set_safe_option(key, value)

    def unset_lasterror_options(self, *options):
        """Unset getlasterror options for this instance.

        If no options are passed unsets all getlasterror options.
        This does not set `safe` to False.
        
        :Parameters:
            - `*options`: The list of options to unset.

        .. versionadded:: 1.11+
        """
        if len(options):
            for option in options:
                self.__safe_opts.pop(option, None)
        else:
            self.__safe_opts = {}


# Copyright 2011-2015 MongoDB, Inc.
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


"""Tools to parse and validate a MongoDB URI."""
import warnings

from bson.py3compat import PY3, iteritems, string_type

if PY3:
    from urllib.parse import unquote_plus
else:
    from urllib import unquote_plus

from pymongo.common import (validate as _validate, get_validated_options)
from pymongo.errors import ConfigurationError, InvalidURI


SCHEME = 'mongodb://'
SCHEME_LEN = len(SCHEME)
DEFAULT_PORT = 27017


def _partition(entity, sep):
    """Python2.4 doesn't have a partition method so we provide
    our own that mimics str.partition from later releases.

    Split the string at the first occurrence of sep, and return a
    3-tuple containing the part before the separator, the separator
    itself, and the part after the separator. If the separator is not
    found, return a 3-tuple containing the string itself, followed
    by two empty strings.
    """
    parts = entity.split(sep, 1)
    if len(parts) == 2:
        return parts[0], sep, parts[1]
    else:
        return entity, '', ''


def _rpartition(entity, sep):
    """Python2.4 doesn't have an rpartition method so we provide
    our own that mimics str.rpartition from later releases.

    Split the string at the last occurrence of sep, and return a
    3-tuple containing the part before the separator, the separator
    itself, and the part after the separator. If the separator is not
    found, return a 3-tuple containing two empty strings, followed
    by the string itself.
    """
    idx = entity.rfind(sep)
    if idx == -1:
        return '', '', entity
    return entity[:idx], sep, entity[idx + 1:]


def parse_userinfo(userinfo):
    """Validates the format of user information in a MongoDB URI.
    Reserved characters like ':', '/', '+' and '@' must be escaped
    following RFC 2396.

    Returns a 2-tuple containing the unescaped username followed
    by the unescaped password.

    :Paramaters:
        - `userinfo`: A string of the form <username>:<password>

    .. versionchanged:: 2.2
       Now uses `urllib.unquote_plus` so `+` characters must be escaped.
    """
    if '@' in userinfo or userinfo.count(':') > 1:
        raise InvalidURI("':' or '@' characters in a username or password "
                         "must be escaped according to RFC 2396.")
    user, _, passwd = _partition(userinfo, ":")
    # No password is expected with GSSAPI authentication.
    if not user:
        raise InvalidURI("The empty string is not valid username.")
    user = unquote_plus(user)
    passwd = unquote_plus(passwd)

    return user, passwd


def parse_ipv6_literal_host(entity, default_port):
    """Validates an IPv6 literal host:port string.

    Returns a 2-tuple of IPv6 literal followed by port where
    port is default_port if it wasn't specified in entity.

    :Parameters:
        - `entity`: A string that represents an IPv6 literal enclosed
                    in braces (e.g. '[::1]' or '[::1]:27017').
        - `default_port`: The port number to use when one wasn't
                          specified in entity.
    """
    if entity.find(']') == -1:
        raise ValueError("an IPv6 address literal must be "
                         "enclosed in '[' and ']' according "
                         "to RFC 2732.")
    i = entity.find(']:')
    if i == -1:
        return entity[1:-1], default_port
    return entity[1: i], entity[i + 2:]


def parse_host(entity, default_port=DEFAULT_PORT):
    """Validates a host string

    Returns a 2-tuple of host followed by port where port is default_port
    if it wasn't specified in the string.

    :Parameters:
        - `entity`: A host or host:port string where host could be a
                    hostname or IP address.
        - `default_port`: The port number to use when one wasn't
                          specified in entity.
    """
    host = entity
    port = default_port
    if entity[0] == '[':
        host, port = parse_ipv6_literal_host(entity, default_port)
    elif entity.endswith(".sock"):
        return entity, default_port
    elif entity.find(':') != -1:
        if entity.count(':') > 1:
            raise ValueError("Reserved characters such as ':' must be "
                             "escaped according RFC 2396. An IPv6 "
                             "address literal must be enclosed in '[' "
                             "and ']' according to RFC 2732.")
        host, port = host.split(':', 1)
    if isinstance(port, string_type):
        if not port.isdigit() or int(port) > 65535 or int(port) <= 0:
            raise ValueError("Port must be an integer between 0 and 65535: %s"
                             % (port,))
        port = int(port)

    # Normalize hostname to lowercase, since DNS is case-insensitive:
    # http://tools.ietf.org/html/rfc4343
    # This prevents useless rediscovery if "foo.com" is in the seed list but
    # "FOO.com" is in the ismaster response.
    return host.lower(), port


def validate_options(opts, warn=False):
    """Validates and normalizes options passed in a MongoDB URI.

    Returns a new dictionary of validated and normalized options. If warn is
    False then errors will be thrown for invalid options, otherwise they will
    be ignored and a warning will be issued.

    :Parameters:
        - `opts`: A dict of MongoDB URI options.
        - `warn` (optional): If ``True`` then warnigns will be logged and
          invalid options will be ignored. Otherwise invalid options will
          cause errors.
    """
    if warn:
        return get_validated_options(opts)
    else:
        return dict([_validate(opt, val) for opt, val in iteritems(opts)])


def _parse_options(opts, delim):
    """Helper method for split_options which creates the options dict.
    Also handles the creation of a list for the URI tag_sets/
    readpreferencetags portion."""
    options = {}
    for opt in opts.split(delim):
        key, val = opt.split("=")
        if key.lower() == 'readpreferencetags':
            options.setdefault('readpreferencetags', []).append(val)
        else:
            # str(option) to ensure that a unicode URI results in plain 'str'
            # option names. 'normalized' is then suitable to be passed as
            # kwargs in all Python versions.
            if str(key) in options:
                warnings.warn("Duplicate URI option %s" % (str(key),))
            options[str(key)] = unquote_plus(val)

    # Special case for deprecated options
    if "wtimeout" in options:
        if "wtimeoutMS" in options:
            options.pop("wtimeout")
        warnings.warn("Option wtimeout is deprecated, use 'wtimeoutMS'"
                      " instead")

    return options


def split_options(opts, validate=True, warn=False):
    """Takes the options portion of a MongoDB URI, validates each option
    and returns the options in a dictionary.

    :Parameters:
        - `opt`: A string representing MongoDB URI options.
        - `validate`: If ``True`` (the default), validate and normalize all
          options.
    """
    and_idx = opts.find("&")
    semi_idx = opts.find(";")
    try:
        if and_idx >= 0 and semi_idx >= 0:
            raise InvalidURI("Can not mix '&' and ';' for option separators.")
        elif and_idx >= 0:
            options = _parse_options(opts, "&")
        elif semi_idx >= 0:
            options = _parse_options(opts, ";")
        elif opts.find("=") != -1:
            options = _parse_options(opts, None)
        else:
            raise ValueError
    except ValueError:
        raise InvalidURI("MongoDB URI options are key=value pairs.")

    if validate:
        return validate_options(options, warn)
    return options


def split_hosts(hosts, default_port=DEFAULT_PORT):
    """Takes a string of the form host1[:port],host2[:port]... and
    splits it into (host, port) tuples. If [:port] isn't present the
    default_port is used.

    Returns a set of 2-tuples containing the host name (or IP) followed by
    port number.

    :Parameters:
        - `hosts`: A string of the form host1[:port],host2[:port],...
        - `default_port`: The port number to use when one wasn't specified
          for a host.
    """
    nodes = []
    for entity in hosts.split(','):
        if not entity:
            raise ConfigurationError("Empty host "
                                     "(or extra comma in host list).")
        port = default_port
        # Unix socket entities don't have ports
        if entity.endswith('.sock'):
            port = None
        nodes.append(parse_host(entity, port))
    return nodes


def parse_uri(uri, default_port=DEFAULT_PORT, validate=True, warn=False):
    """Parse and validate a MongoDB URI.

    Returns a dict of the form::

        {
            'nodelist': <list of (host, port) tuples>,
            'username': <username> or None,
            'password': <password> or None,
            'database': <database name> or None,
            'collection': <collection name> or None,
            'options': <dict of MongoDB URI options>
        }

    :Parameters:
        - `uri`: The MongoDB URI to parse.
        - `default_port`: The port number to use when one wasn't specified
          for a host in the URI.
        - `validate`: If ``True`` (the default), validate and normalize all
          options.
        - `warn` (optional): When validating, if ``True`` then will warn
          the user then ignore any invalid options or values. If ``False``,
          validation will error when options are unsupported or values are
          invalid.

    .. versionchanged:: 3.1
        ``warn`` added so invalid options can be ignored.
    """
    if not uri.startswith(SCHEME):
        raise InvalidURI("Invalid URI scheme: URI "
                         "must begin with '%s'" % (SCHEME,))

    scheme_free = uri[SCHEME_LEN:]

    if not scheme_free:
        raise InvalidURI("Must provide at least one hostname or IP.")

    user = None
    passwd = None
    dbase = None
    collection = None
    options = {}

    # Check for unix domain sockets in the uri
    if '.sock' in scheme_free:
        host_part, _, path_part = _rpartition(scheme_free, '/')
        if not host_part:
            host_part = path_part
            path_part = ""
        if '/' in host_part:
            raise InvalidURI("Any '/' in a unix domain socket must be"
                             " URL encoded: %s" % host_part)
        host_part = unquote_plus(host_part)
        path_part = unquote_plus(path_part)
    else:
        host_part, _, path_part = _partition(scheme_free, '/')

    if not path_part and '?' in host_part:
        raise InvalidURI("A '/' is required between "
                         "the host list and any options.")

    if '@' in host_part:
        userinfo, _, hosts = _rpartition(host_part, '@')
        user, passwd = parse_userinfo(userinfo)
    else:
        hosts = host_part

    nodes = split_hosts(hosts, default_port=default_port)

    if path_part:

        if path_part[0] == '?':
            opts = path_part[1:]
        else:
            dbase, _, opts = _partition(path_part, '?')
            if '.' in dbase:
                dbase, collection = dbase.split('.', 1)

        if opts:
            options = split_options(opts, validate, warn)

    if dbase is not None:
        dbase = unquote_plus(dbase)
    if collection is not None:
        collection = unquote_plus(collection)

    return {
        'nodelist': nodes,
        'username': user,
        'password': passwd,
        'database': dbase,
        'collection': collection,
        'options': options
    }


if __name__ == '__main__':
    import pprint
    import sys
    try:
        pprint.pprint(parse_uri(sys.argv[1]))
    except InvalidURI as e:
        print(e)
    sys.exit(0)

from __future__ import annotations

import sys
from typing import Any, Optional
from urllib.parse import unquote_plus

from pymongo.asynchronous.srv_resolver import _have_dnspython, _SrvResolver
from pymongo.common import SRV_SERVICE_NAME, _CaseInsensitiveDictionary
from pymongo.errors import ConfigurationError, InvalidURI
from pymongo.uri_parser_shared import (
    _ALLOWED_TXT_OPTS,
    _BAD_DB_CHARS,
    DEFAULT_PORT,
    SCHEME,
    SCHEME_LEN,
    SRV_SCHEME,
    SRV_SCHEME_LEN,
    _check_options,
    parse_userinfo,
    split_hosts,
    split_options,
)

_IS_SYNC = False


async def parse_uri(
    uri: str,
    default_port: Optional[int] = DEFAULT_PORT,
    validate: bool = True,
    warn: bool = False,
    normalize: bool = True,
    connect_timeout: Optional[float] = None,
    srv_service_name: Optional[str] = None,
    srv_max_hosts: Optional[int] = None,
) -> dict[str, Any]:
    """Parse and validate a MongoDB URI.

    Returns a dict of the form::

        {
            'nodelist': <list of (host, port) tuples>,
            'username': <username> or None,
            'password': <password> or None,
            'database': <database name> or None,
            'collection': <collection name> or None,
            'options': <dict of MongoDB URI options>,
            'fqdn': <fqdn of the MongoDB+SRV URI> or None
        }

    If the URI scheme is "mongodb+srv://" DNS SRV and TXT lookups will be done
    to build nodelist and options.

    :param uri: The MongoDB URI to parse.
    :param default_port: The port number to use when one wasn't specified
          for a host in the URI.
    :param validate: If ``True`` (the default), validate and
          normalize all options. Default: ``True``.
    :param warn: When validating, if ``True`` then will warn
          the user then ignore any invalid options or values. If ``False``,
          validation will error when options are unsupported or values are
          invalid. Default: ``False``.
    :param normalize: If ``True``, convert names of URI options
          to their internally-used names. Default: ``True``.
    :param connect_timeout: The maximum time in milliseconds to
          wait for a response from the DNS server.
    :param srv_service_name: A custom SRV service name

    .. versionchanged:: 4.6
       The delimiting slash (``/``) between hosts and connection options is now optional.
       For example, "mongodb://example.com?tls=true" is now a valid URI.

    .. versionchanged:: 4.0
       To better follow RFC 3986, unquoted percent signs ("%") are no longer
       supported.

    .. versionchanged:: 3.9
        Added the ``normalize`` parameter.

    .. versionchanged:: 3.6
        Added support for mongodb+srv:// URIs.

    .. versionchanged:: 3.5
        Return the original value of the ``readPreference`` MongoDB URI option
        instead of the validated read preference mode.

    .. versionchanged:: 3.1
        ``warn`` added so invalid options can be ignored.
    """
    result = _validate_uri(uri, default_port, validate, warn, normalize, srv_max_hosts)
    result.update(
        await _parse_srv(
            uri,
            default_port,
            validate,
            warn,
            normalize,
            connect_timeout,
            srv_service_name,
            srv_max_hosts,
        )
    )
    return result


def _validate_uri(
    uri: str,
    default_port: Optional[int] = DEFAULT_PORT,
    validate: bool = True,
    warn: bool = False,
    normalize: bool = True,
    srv_max_hosts: Optional[int] = None,
) -> dict[str, Any]:
    if uri.startswith(SCHEME):
        is_srv = False
        scheme_free = uri[SCHEME_LEN:]
    elif uri.startswith(SRV_SCHEME):
        if not _have_dnspython():
            python_path = sys.executable or "python"
            raise ConfigurationError(
                'The "dnspython" module must be '
                "installed to use mongodb+srv:// URIs. "
                "To fix this error install pymongo again:\n "
                "%s -m pip install pymongo>=4.3" % (python_path)
            )
        is_srv = True
        scheme_free = uri[SRV_SCHEME_LEN:]
    else:
        raise InvalidURI(f"Invalid URI scheme: URI must begin with '{SCHEME}' or '{SRV_SCHEME}'")

    if not scheme_free:
        raise InvalidURI("Must provide at least one hostname or IP")

    user = None
    passwd = None
    dbase = None
    collection = None
    options = _CaseInsensitiveDictionary()

    host_plus_db_part, _, opts = scheme_free.partition("?")
    if "/" in host_plus_db_part:
        host_part, _, dbase = host_plus_db_part.partition("/")
    else:
        host_part = host_plus_db_part

    if dbase:
        dbase = unquote_plus(dbase)
        if "." in dbase:
            dbase, collection = dbase.split(".", 1)
        if _BAD_DB_CHARS.search(dbase):
            raise InvalidURI('Bad database name "%s"' % dbase)
    else:
        dbase = None

    if opts:
        options.update(split_options(opts, validate, warn, normalize))
    if "@" in host_part:
        userinfo, _, hosts = host_part.rpartition("@")
        user, passwd = parse_userinfo(userinfo)
    else:
        hosts = host_part

    if "/" in hosts:
        raise InvalidURI("Any '/' in a unix domain socket must be percent-encoded: %s" % host_part)

    hosts = unquote_plus(hosts)
    fqdn = None
    srv_max_hosts = srv_max_hosts or options.get("srvMaxHosts")
    if is_srv:
        if options.get("directConnection"):
            raise ConfigurationError(f"Cannot specify directConnection=true with {SRV_SCHEME} URIs")
        nodes = split_hosts(hosts, default_port=None)
        if len(nodes) != 1:
            raise InvalidURI(f"{SRV_SCHEME} URIs must include one, and only one, hostname")
        fqdn, port = nodes[0]
        if port is not None:
            raise InvalidURI(f"{SRV_SCHEME} URIs must not include a port number")
    elif not is_srv and options.get("srvServiceName") is not None:
        raise ConfigurationError(
            "The srvServiceName option is only allowed with 'mongodb+srv://' URIs"
        )
    elif not is_srv and srv_max_hosts:
        raise ConfigurationError(
            "The srvMaxHosts option is only allowed with 'mongodb+srv://' URIs"
        )
    else:
        nodes = split_hosts(hosts, default_port=default_port)

    _check_options(nodes, options)

    return {
        "nodelist": nodes,
        "username": user,
        "password": passwd,
        "database": dbase,
        "collection": collection,
        "options": options,
        "fqdn": fqdn,
    }


async def _parse_srv(
    uri: str,
    default_port: Optional[int] = DEFAULT_PORT,
    validate: bool = True,
    warn: bool = False,
    normalize: bool = True,
    connect_timeout: Optional[float] = None,
    srv_service_name: Optional[str] = None,
    srv_max_hosts: Optional[int] = None,
) -> dict[str, Any]:
    if uri.startswith(SCHEME):
        is_srv = False
        scheme_free = uri[SCHEME_LEN:]
    else:
        is_srv = True
        scheme_free = uri[SRV_SCHEME_LEN:]

    options = _CaseInsensitiveDictionary()

    host_plus_db_part, _, opts = scheme_free.partition("?")
    if "/" in host_plus_db_part:
        host_part, _, _ = host_plus_db_part.partition("/")
    else:
        host_part = host_plus_db_part

    if opts:
        options.update(split_options(opts, validate, warn, normalize))
    if srv_service_name is None:
        srv_service_name = options.get("srvServiceName", SRV_SERVICE_NAME)
    if "@" in host_part:
        _, _, hosts = host_part.rpartition("@")
    else:
        hosts = host_part

    hosts = unquote_plus(hosts)
    srv_max_hosts = srv_max_hosts or options.get("srvMaxHosts")
    if is_srv:
        nodes = split_hosts(hosts, default_port=None)
        fqdn, port = nodes[0]

        # Use the connection timeout. connectTimeoutMS passed as a keyword
        # argument overrides the same option passed in the connection string.
        connect_timeout = connect_timeout or options.get("connectTimeoutMS")
        dns_resolver = _SrvResolver(fqdn, connect_timeout, srv_service_name, srv_max_hosts)
        nodes = await dns_resolver.get_hosts()
        dns_options = await dns_resolver.get_options()
        if dns_options:
            parsed_dns_options = split_options(dns_options, validate, warn, normalize)
            if set(parsed_dns_options) - _ALLOWED_TXT_OPTS:
                raise ConfigurationError(
                    "Only authSource, replicaSet, and loadBalanced are supported from DNS"
                )
            for opt, val in parsed_dns_options.items():
                if opt not in options:
                    options[opt] = val
        if options.get("loadBalanced") and srv_max_hosts:
            raise InvalidURI("You cannot specify loadBalanced with srvMaxHosts")
        if options.get("replicaSet") and srv_max_hosts:
            raise InvalidURI("You cannot specify replicaSet with srvMaxHosts")
        if "tls" not in options and "ssl" not in options:
            options["tls"] = True if validate else "true"
    else:
        nodes = split_hosts(hosts, default_port=default_port)

    _check_options(nodes, options)

    return {
        "nodelist": nodes,
        "options": options,
    }


if __name__ == "__main__":
    import pprint

    try:
        if _IS_SYNC:
            pprint.pprint(parse_uri(sys.argv[1]))  # noqa: T203
        else:
            import asyncio

            pprint.pprint(asyncio.run(parse_uri(sys.argv[1])))  # type:ignore[arg-type]  # noqa: T203
    except InvalidURI as exc:
        print(exc)  # noqa: T201
    sys.exit(0)

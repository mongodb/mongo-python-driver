# Copyright 2011-present MongoDB, Inc.
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

import datetime
import inspect
import warnings
from collections import OrderedDict, abc
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from urllib.parse import unquote_plus

from bson import SON
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions, DatetimeConversion, TypeRegistry
from bson.raw_bson import RawBSONDocument
from pymongo.auth import MECHANISMS
from pymongo.compression_support import (
    validate_compressors,
    validate_zlib_compression_level,
)
from pymongo.driver_info import DriverInfo
from pymongo.errors import ConfigurationError
from pymongo.monitoring import _validate_event_listeners
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _MONGOS_MODES, _ServerMode
from pymongo.server_api import ServerApi
from pymongo.write_concern import DEFAULT_WRITE_CONCERN, WriteConcern, validate_boolean

ORDERED_TYPES: Sequence[Type] = (SON, OrderedDict)

# Defaults until we connect to a server and get updated limits.
MAX_BSON_SIZE = 16 * (1024**2)
MAX_MESSAGE_SIZE: int = 2 * MAX_BSON_SIZE
MIN_WIRE_VERSION = 0
MAX_WIRE_VERSION = 0
MAX_WRITE_BATCH_SIZE = 1000

# What this version of PyMongo supports.
MIN_SUPPORTED_SERVER_VERSION = "3.6"
MIN_SUPPORTED_WIRE_VERSION = 6
MAX_SUPPORTED_WIRE_VERSION = 21

# Frequency to call hello on servers, in seconds.
HEARTBEAT_FREQUENCY = 10

# Frequency to clean up unclosed cursors, in seconds.
# See MongoClient._process_kill_cursors.
KILL_CURSOR_FREQUENCY = 1

# Frequency to process events queue, in seconds.
EVENTS_QUEUE_FREQUENCY = 1

# How long to wait, in seconds, for a suitable server to be found before
# aborting an operation. For example, if the client attempts an insert
# during a replica set election, SERVER_SELECTION_TIMEOUT governs the
# longest it is willing to wait for a new primary to be found.
SERVER_SELECTION_TIMEOUT = 30

# Spec requires at least 500ms between hello calls.
MIN_HEARTBEAT_INTERVAL = 0.5

# Spec requires at least 60s between SRV rescans.
MIN_SRV_RESCAN_INTERVAL = 60

# Default connectTimeout in seconds.
CONNECT_TIMEOUT = 20.0

# Default value for maxPoolSize.
MAX_POOL_SIZE = 100

# Default value for minPoolSize.
MIN_POOL_SIZE = 0

# The maximum number of concurrent connection creation attempts per pool.
MAX_CONNECTING = 2

# Default value for maxIdleTimeMS.
MAX_IDLE_TIME_MS: Optional[int] = None

# Default value for maxIdleTimeMS in seconds.
MAX_IDLE_TIME_SEC: Optional[int] = None

# Default value for waitQueueTimeoutMS in seconds.
WAIT_QUEUE_TIMEOUT: Optional[int] = None

# Default value for localThresholdMS.
LOCAL_THRESHOLD_MS = 15

# Default value for retryWrites.
RETRY_WRITES = True

# Default value for retryReads.
RETRY_READS = True

# The error code returned when a command doesn't exist.
COMMAND_NOT_FOUND_CODES: Sequence[int] = (59,)

# Error codes to ignore if GridFS calls createIndex on a secondary
UNAUTHORIZED_CODES: Sequence[int] = (13, 16547, 16548)

# Maximum number of sessions to send in a single endSessions command.
# From the driver sessions spec.
_MAX_END_SESSIONS = 10000

# Default value for srvServiceName
SRV_SERVICE_NAME = "mongodb"


def partition_node(node: str) -> Tuple[str, int]:
    """Split a host:port string into (host, int(port)) pair."""
    host = node
    port = 27017
    idx = node.rfind(":")
    if idx != -1:
        host, port = node[:idx], int(node[idx + 1 :])
    if host.startswith("["):
        host = host[1:-1]
    return host, port


def clean_node(node: str) -> Tuple[str, int]:
    """Split and normalize a node name from a hello response."""
    host, port = partition_node(node)

    # Normalize hostname to lowercase, since DNS is case-insensitive:
    # http://tools.ietf.org/html/rfc4343
    # This prevents useless rediscovery if "foo.com" is in the seed list but
    # "FOO.com" is in the hello response.
    return host.lower(), port


def raise_config_error(key: str, dummy: Any) -> NoReturn:
    """Raise ConfigurationError with the given key name."""
    raise ConfigurationError(f"Unknown option {key}")


# Mapping of URI uuid representation options to valid subtypes.
_UUID_REPRESENTATIONS = {
    "unspecified": UuidRepresentation.UNSPECIFIED,
    "standard": UuidRepresentation.STANDARD,
    "pythonLegacy": UuidRepresentation.PYTHON_LEGACY,
    "javaLegacy": UuidRepresentation.JAVA_LEGACY,
    "csharpLegacy": UuidRepresentation.CSHARP_LEGACY,
}


def validate_boolean_or_string(option: str, value: Any) -> bool:
    """Validates that value is True, False, 'true', or 'false'."""
    if isinstance(value, str):
        if value not in ("true", "false"):
            raise ValueError(f"The value of {option} must be 'true' or 'false'")
        return value == "true"
    return validate_boolean(option, value)


def validate_integer(option: str, value: Any) -> int:
    """Validates that 'value' is an integer (or basestring representation)."""
    if isinstance(value, int):
        return value
    elif isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            raise ValueError(f"The value of {option} must be an integer")
    raise TypeError(f"Wrong type for {option}, value must be an integer")


def validate_positive_integer(option: str, value: Any) -> int:
    """Validate that 'value' is a positive integer, which does not include 0."""
    val = validate_integer(option, value)
    if val <= 0:
        raise ValueError(f"The value of {option} must be a positive integer")
    return val


def validate_non_negative_integer(option: str, value: Any) -> int:
    """Validate that 'value' is a positive integer or 0."""
    val = validate_integer(option, value)
    if val < 0:
        raise ValueError(f"The value of {option} must be a non negative integer")
    return val


def validate_readable(option: str, value: Any) -> Optional[str]:
    """Validates that 'value' is file-like and readable."""
    if value is None:
        return value
    # First make sure its a string py3.3 open(True, 'r') succeeds
    # Used in ssl cert checking due to poor ssl module error reporting
    value = validate_string(option, value)
    open(value).close()
    return value


def validate_positive_integer_or_none(option: str, value: Any) -> Optional[int]:
    """Validate that 'value' is a positive integer or None."""
    if value is None:
        return value
    return validate_positive_integer(option, value)


def validate_non_negative_integer_or_none(option: str, value: Any) -> Optional[int]:
    """Validate that 'value' is a positive integer or 0 or None."""
    if value is None:
        return value
    return validate_non_negative_integer(option, value)


def validate_string(option: str, value: Any) -> str:
    """Validates that 'value' is an instance of `str`."""
    if isinstance(value, str):
        return value
    raise TypeError(f"Wrong type for {option}, value must be an instance of str")


def validate_string_or_none(option: str, value: Any) -> Optional[str]:
    """Validates that 'value' is an instance of `basestring` or `None`."""
    if value is None:
        return value
    return validate_string(option, value)


def validate_int_or_basestring(option: str, value: Any) -> Union[int, str]:
    """Validates that 'value' is an integer or string."""
    if isinstance(value, int):
        return value
    elif isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return value
    raise TypeError(f"Wrong type for {option}, value must be an integer or a string")


def validate_non_negative_int_or_basestring(option: Any, value: Any) -> Union[int, str]:
    """Validates that 'value' is an integer or string."""
    if isinstance(value, int):
        return value
    elif isinstance(value, str):
        try:
            val = int(value)
        except ValueError:
            return value
        return validate_non_negative_integer(option, val)
    raise TypeError(f"Wrong type for {option}, value must be an non negative integer or a string")


def validate_positive_float(option: str, value: Any) -> float:
    """Validates that 'value' is a float, or can be converted to one, and is
    positive.
    """
    errmsg = f"{option} must be an integer or float"
    try:
        value = float(value)
    except ValueError:
        raise ValueError(errmsg)
    except TypeError:
        raise TypeError(errmsg)

    # float('inf') doesn't work in 2.4 or 2.5 on Windows, so just cap floats at
    # one billion - this is a reasonable approximation for infinity
    if not 0 < value < 1e9:
        raise ValueError(f"{option} must be greater than 0 and less than one billion")
    return value


def validate_positive_float_or_zero(option: str, value: Any) -> float:
    """Validates that 'value' is 0 or a positive float, or can be converted to
    0 or a positive float.
    """
    if value == 0 or value == "0":
        return 0
    return validate_positive_float(option, value)


def validate_timeout_or_none(option: str, value: Any) -> Optional[float]:
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds.
    """
    if value is None:
        return value
    return validate_positive_float(option, value) / 1000.0


def validate_timeout_or_zero(option: str, value: Any) -> float:
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds for the case where None is an error
    and 0 is valid. Setting the timeout to nothing in the URI string is a
    config error.
    """
    if value is None:
        raise ConfigurationError(f"{option} cannot be None")
    if value == 0 or value == "0":
        return 0
    return validate_positive_float(option, value) / 1000.0


def validate_timeout_or_none_or_zero(option: Any, value: Any) -> Optional[float]:
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds. value=0 and value="0" are treated the
    same as value=None which means unlimited timeout.
    """
    if value is None or value == 0 or value == "0":
        return None
    return validate_positive_float(option, value) / 1000.0


def validate_timeoutms(option: Any, value: Any) -> Optional[float]:
    """Validates a timeout specified in milliseconds returning
    a value in floating point seconds.
    """
    if value is None:
        return None
    return validate_positive_float_or_zero(option, value) / 1000.0


def validate_max_staleness(option: str, value: Any) -> int:
    """Validates maxStalenessSeconds according to the Max Staleness Spec."""
    if value == -1 or value == "-1":
        # Default: No maximum staleness.
        return -1
    return validate_positive_integer(option, value)


def validate_read_preference(dummy: Any, value: Any) -> _ServerMode:
    """Validate a read preference."""
    if not isinstance(value, _ServerMode):
        raise TypeError(f"{value!r} is not a read preference.")
    return value


def validate_read_preference_mode(dummy: Any, value: Any) -> _ServerMode:
    """Validate read preference mode for a MongoClient.

    .. versionchanged:: 3.5
       Returns the original ``value`` instead of the validated read preference
       mode.
    """
    if value not in _MONGOS_MODES:
        raise ValueError(f"{value} is not a valid read preference")
    return value


def validate_auth_mechanism(option: str, value: Any) -> str:
    """Validate the authMechanism URI option."""
    if value not in MECHANISMS:
        raise ValueError(f"{option} must be in {tuple(MECHANISMS)}")
    return value


def validate_uuid_representation(dummy: Any, value: Any) -> int:
    """Validate the uuid representation option selected in the URI."""
    try:
        return _UUID_REPRESENTATIONS[value]
    except KeyError:
        raise ValueError(
            "{} is an invalid UUID representation. "
            "Must be one of "
            "{}".format(value, tuple(_UUID_REPRESENTATIONS))
        )


def validate_read_preference_tags(name: str, value: Any) -> List[Dict[str, str]]:
    """Parse readPreferenceTags if passed as a client kwarg."""
    if not isinstance(value, list):
        value = [value]

    tag_sets: List = []
    for tag_set in value:
        if tag_set == "":
            tag_sets.append({})
            continue
        try:
            tags = {}
            for tag in tag_set.split(","):
                key, val = tag.split(":")
                tags[unquote_plus(key)] = unquote_plus(val)
            tag_sets.append(tags)
        except Exception:
            raise ValueError(f"{tag_set!r} not a valid value for {name}")
    return tag_sets


_MECHANISM_PROPS = frozenset(
    [
        "SERVICE_NAME",
        "CANONICALIZE_HOST_NAME",
        "SERVICE_REALM",
        "AWS_SESSION_TOKEN",
        "PROVIDER_NAME",
    ]
)


def validate_auth_mechanism_properties(option: str, value: Any) -> Dict[str, Union[bool, str]]:
    """Validate authMechanismProperties."""
    props: Dict[str, Any] = {}
    if not isinstance(value, str):
        if not isinstance(value, dict):
            raise ValueError("Auth mechanism properties must be given as a string or a dictionary")
        for key, value in value.items():
            if isinstance(value, str):
                props[key] = value
            elif isinstance(value, bool):
                props[key] = str(value).lower()
            elif key in ["allowed_hosts"] and isinstance(value, list):
                props[key] = value
            elif inspect.isfunction(value):
                signature = inspect.signature(value)
                if key == "request_token_callback":
                    expected_params = 2
                elif key == "refresh_token_callback":
                    expected_params = 2
                else:
                    raise ValueError(f"Unrecognized Auth mechanism function {key}")
                if len(signature.parameters) != expected_params:
                    msg = f"{key} must accept {expected_params} parameters"
                    raise ValueError(msg)
                props[key] = value
            else:
                raise ValueError(
                    "Auth mechanism property values must be strings or callback functions"
                )
        return props

    value = validate_string(option, value)
    for opt in value.split(","):
        try:
            key, val = opt.split(":")
        except ValueError:
            # Try not to leak the token.
            if "AWS_SESSION_TOKEN" in opt:
                opt = (
                    "AWS_SESSION_TOKEN:<redacted token>, did you forget "
                    "to percent-escape the token with quote_plus?"
                )
            raise ValueError(
                "auth mechanism properties must be "
                "key:value pairs like SERVICE_NAME:"
                "mongodb, not {}.".format(opt)
            )
        if key not in _MECHANISM_PROPS:
            raise ValueError(
                "{} is not a supported auth "
                "mechanism property. Must be one of "
                "{}.".format(key, tuple(_MECHANISM_PROPS))
            )
        if key == "CANONICALIZE_HOST_NAME":
            props[key] = validate_boolean_or_string(key, val)
        else:
            props[key] = unquote_plus(val)

    return props


def validate_document_class(
    option: str, value: Any
) -> Union[Type[MutableMapping], Type[RawBSONDocument]]:
    """Validate the document_class option."""
    # issubclass can raise TypeError for generic aliases like SON[str, Any].
    # In that case we can use the base class for the comparison.
    is_mapping = False
    try:
        is_mapping = issubclass(value, abc.MutableMapping)
    except TypeError:
        if hasattr(value, "__origin__"):
            is_mapping = issubclass(value.__origin__, abc.MutableMapping)
    if not is_mapping and not issubclass(value, RawBSONDocument):
        raise TypeError(
            "{} must be dict, bson.son.SON, "
            "bson.raw_bson.RawBSONDocument, or a "
            "subclass of collections.MutableMapping".format(option)
        )
    return value


def validate_type_registry(option: Any, value: Any) -> Optional[TypeRegistry]:
    """Validate the type_registry option."""
    if value is not None and not isinstance(value, TypeRegistry):
        raise TypeError(f"{option} must be an instance of {TypeRegistry}")
    return value


def validate_list(option: str, value: Any) -> List:
    """Validates that 'value' is a list."""
    if not isinstance(value, list):
        raise TypeError(f"{option} must be a list")
    return value


def validate_list_or_none(option: Any, value: Any) -> Optional[List]:
    """Validates that 'value' is a list or None."""
    if value is None:
        return value
    return validate_list(option, value)


def validate_list_or_mapping(option: Any, value: Any) -> None:
    """Validates that 'value' is a list or a document."""
    if not isinstance(value, (abc.Mapping, list)):
        raise TypeError(
            "{} must either be a list or an instance of dict, "
            "bson.son.SON, or any other type that inherits from "
            "collections.Mapping".format(option)
        )


def validate_is_mapping(option: str, value: Any) -> None:
    """Validate the type of method arguments that expect a document."""
    if not isinstance(value, abc.Mapping):
        raise TypeError(
            "{} must be an instance of dict, bson.son.SON, or "
            "any other type that inherits from "
            "collections.Mapping".format(option)
        )


def validate_is_document_type(option: str, value: Any) -> None:
    """Validate the type of method arguments that expect a MongoDB document."""
    if not isinstance(value, (abc.MutableMapping, RawBSONDocument)):
        raise TypeError(
            "{} must be an instance of dict, bson.son.SON, "
            "bson.raw_bson.RawBSONDocument, or "
            "a type that inherits from "
            "collections.MutableMapping".format(option)
        )


def validate_appname_or_none(option: str, value: Any) -> Optional[str]:
    """Validate the appname option."""
    if value is None:
        return value
    validate_string(option, value)
    # We need length in bytes, so encode utf8 first.
    if len(value.encode("utf-8")) > 128:
        raise ValueError(f"{option} must be <= 128 bytes")
    return value


def validate_driver_or_none(option: Any, value: Any) -> Optional[DriverInfo]:
    """Validate the driver keyword arg."""
    if value is None:
        return value
    if not isinstance(value, DriverInfo):
        raise TypeError(f"{option} must be an instance of DriverInfo")
    return value


def validate_server_api_or_none(option: Any, value: Any) -> Optional[ServerApi]:
    """Validate the server_api keyword arg."""
    if value is None:
        return value
    if not isinstance(value, ServerApi):
        raise TypeError(f"{option} must be an instance of ServerApi")
    return value


def validate_is_callable_or_none(option: Any, value: Any) -> Optional[Callable]:
    """Validates that 'value' is a callable."""
    if value is None:
        return value
    if not callable(value):
        raise ValueError(f"{option} must be a callable")
    return value


def validate_ok_for_replace(replacement: Mapping[str, Any]) -> None:
    """Validate a replacement document."""
    validate_is_mapping("replacement", replacement)
    # Replacement can be {}
    if replacement and not isinstance(replacement, RawBSONDocument):
        first = next(iter(replacement))
        if first.startswith("$"):
            raise ValueError("replacement can not include $ operators")


def validate_ok_for_update(update: Any) -> None:
    """Validate an update document."""
    validate_list_or_mapping("update", update)
    # Update cannot be {}.
    if not update:
        raise ValueError("update cannot be empty")

    is_document = not isinstance(update, list)
    first = next(iter(update))
    if is_document and not first.startswith("$"):
        raise ValueError("update only works with $ operators")


_UNICODE_DECODE_ERROR_HANDLERS = frozenset(["strict", "replace", "ignore"])


def validate_unicode_decode_error_handler(dummy: Any, value: str) -> str:
    """Validate the Unicode decode error handler option of CodecOptions."""
    if value not in _UNICODE_DECODE_ERROR_HANDLERS:
        raise ValueError(
            "{} is an invalid Unicode decode error handler. "
            "Must be one of "
            "{}".format(value, tuple(_UNICODE_DECODE_ERROR_HANDLERS))
        )
    return value


def validate_tzinfo(dummy: Any, value: Any) -> Optional[datetime.tzinfo]:
    """Validate the tzinfo option"""
    if value is not None and not isinstance(value, datetime.tzinfo):
        raise TypeError("%s must be an instance of datetime.tzinfo" % value)
    return value


def validate_auto_encryption_opts_or_none(option: Any, value: Any) -> Optional[Any]:
    """Validate the driver keyword arg."""
    if value is None:
        return value
    from pymongo.encryption_options import AutoEncryptionOpts

    if not isinstance(value, AutoEncryptionOpts):
        raise TypeError(f"{option} must be an instance of AutoEncryptionOpts")

    return value


def validate_datetime_conversion(option: Any, value: Any) -> Optional[DatetimeConversion]:
    """Validate a DatetimeConversion string."""
    if value is None:
        return DatetimeConversion.DATETIME

    if isinstance(value, str):
        if value.isdigit():
            return DatetimeConversion(int(value))
        return DatetimeConversion[value]
    elif isinstance(value, int):
        return DatetimeConversion(value)

    raise TypeError(f"{option} must be a str or int representing DatetimeConversion")


# Dictionary where keys are the names of public URI options, and values
# are lists of aliases for that option.
URI_OPTIONS_ALIAS_MAP: Dict[str, List[str]] = {
    "tls": ["ssl"],
}

# Dictionary where keys are the names of URI options, and values
# are functions that validate user-input values for that option. If an option
# alias uses a different validator than its public counterpart, it should be
# included here as a key, value pair.
URI_OPTIONS_VALIDATOR_MAP: Dict[str, Callable[[Any, Any], Any]] = {
    "appname": validate_appname_or_none,
    "authmechanism": validate_auth_mechanism,
    "authmechanismproperties": validate_auth_mechanism_properties,
    "authsource": validate_string,
    "compressors": validate_compressors,
    "connecttimeoutms": validate_timeout_or_none_or_zero,
    "directconnection": validate_boolean_or_string,
    "heartbeatfrequencyms": validate_timeout_or_none,
    "journal": validate_boolean_or_string,
    "localthresholdms": validate_positive_float_or_zero,
    "maxidletimems": validate_timeout_or_none,
    "maxconnecting": validate_positive_integer,
    "maxpoolsize": validate_non_negative_integer_or_none,
    "maxstalenessseconds": validate_max_staleness,
    "readconcernlevel": validate_string_or_none,
    "readpreference": validate_read_preference_mode,
    "readpreferencetags": validate_read_preference_tags,
    "replicaset": validate_string_or_none,
    "retryreads": validate_boolean_or_string,
    "retrywrites": validate_boolean_or_string,
    "loadbalanced": validate_boolean_or_string,
    "serverselectiontimeoutms": validate_timeout_or_zero,
    "sockettimeoutms": validate_timeout_or_none_or_zero,
    "tls": validate_boolean_or_string,
    "tlsallowinvalidcertificates": validate_boolean_or_string,
    "tlsallowinvalidhostnames": validate_boolean_or_string,
    "tlscafile": validate_readable,
    "tlscertificatekeyfile": validate_readable,
    "tlscertificatekeyfilepassword": validate_string_or_none,
    "tlsdisableocspendpointcheck": validate_boolean_or_string,
    "tlsinsecure": validate_boolean_or_string,
    "w": validate_non_negative_int_or_basestring,
    "wtimeoutms": validate_non_negative_integer,
    "zlibcompressionlevel": validate_zlib_compression_level,
    "srvservicename": validate_string,
    "srvmaxhosts": validate_non_negative_integer,
    "timeoutms": validate_timeoutms,
}

# Dictionary where keys are the names of URI options specific to pymongo,
# and values are functions that validate user-input values for those options.
NONSPEC_OPTIONS_VALIDATOR_MAP: Dict[str, Callable[[Any, Any], Any]] = {
    "connect": validate_boolean_or_string,
    "driver": validate_driver_or_none,
    "server_api": validate_server_api_or_none,
    "fsync": validate_boolean_or_string,
    "minpoolsize": validate_non_negative_integer,
    "tlscrlfile": validate_readable,
    "tz_aware": validate_boolean_or_string,
    "unicode_decode_error_handler": validate_unicode_decode_error_handler,
    "uuidrepresentation": validate_uuid_representation,
    "waitqueuemultiple": validate_non_negative_integer_or_none,
    "waitqueuetimeoutms": validate_timeout_or_none,
    "datetime_conversion": validate_datetime_conversion,
}

# Dictionary where keys are the names of keyword-only options for the
# MongoClient constructor, and values are functions that validate user-input
# values for those options.
KW_VALIDATORS: Dict[str, Callable[[Any, Any], Any]] = {
    "document_class": validate_document_class,
    "type_registry": validate_type_registry,
    "read_preference": validate_read_preference,
    "event_listeners": _validate_event_listeners,
    "tzinfo": validate_tzinfo,
    "username": validate_string_or_none,
    "password": validate_string_or_none,
    "server_selector": validate_is_callable_or_none,
    "auto_encryption_opts": validate_auto_encryption_opts_or_none,
    "authoidcallowedhosts": validate_list,
}

# Dictionary where keys are any URI option name, and values are the
# internally-used names of that URI option. Options with only one name
# variant need not be included here. Options whose public and internal
# names are the same need not be included here.
INTERNAL_URI_OPTION_NAME_MAP: Dict[str, str] = {
    "ssl": "tls",
}

# Map from deprecated URI option names to a tuple indicating the method of
# their deprecation and any additional information that may be needed to
# construct the warning message.
URI_OPTIONS_DEPRECATION_MAP: Dict[str, Tuple[str, str]] = {
    # format: <deprecated option name>: (<mode>, <message>),
    # Supported <mode> values:
    # - 'renamed': <message> should be the new option name. Note that case is
    #   preserved for renamed options as they are part of user warnings.
    # - 'removed': <message> may suggest the rationale for deprecating the
    #   option and/or recommend remedial action.
    # For example:
    # 'wtimeout': ('renamed', 'wTimeoutMS'),
}

# Augment the option validator map with pymongo-specific option information.
URI_OPTIONS_VALIDATOR_MAP.update(NONSPEC_OPTIONS_VALIDATOR_MAP)
for optname, aliases in URI_OPTIONS_ALIAS_MAP.items():
    for alias in aliases:
        if alias not in URI_OPTIONS_VALIDATOR_MAP:
            URI_OPTIONS_VALIDATOR_MAP[alias] = URI_OPTIONS_VALIDATOR_MAP[optname]

# Map containing all URI option and keyword argument validators.
VALIDATORS: Dict[str, Callable[[Any, Any], Any]] = URI_OPTIONS_VALIDATOR_MAP.copy()
VALIDATORS.update(KW_VALIDATORS)

# List of timeout-related options.
TIMEOUT_OPTIONS: List[str] = [
    "connecttimeoutms",
    "heartbeatfrequencyms",
    "maxidletimems",
    "maxstalenessseconds",
    "serverselectiontimeoutms",
    "sockettimeoutms",
    "waitqueuetimeoutms",
]


_AUTH_OPTIONS = frozenset(["authmechanismproperties"])


def validate_auth_option(option: str, value: Any) -> Tuple[str, Any]:
    """Validate optional authentication parameters."""
    lower, value = validate(option, value)
    if lower not in _AUTH_OPTIONS:
        raise ConfigurationError(f"Unknown authentication option: {option}")
    return option, value


def validate(option: str, value: Any) -> Tuple[str, Any]:
    """Generic validation function."""
    lower = option.lower()
    validator = VALIDATORS.get(lower, raise_config_error)
    value = validator(option, value)
    return option, value


def get_validated_options(
    options: Mapping[str, Any], warn: bool = True
) -> MutableMapping[str, Any]:
    """Validate each entry in options and raise a warning if it is not valid.
    Returns a copy of options with invalid entries removed.

    :Parameters:
        - `opts`: A dict containing MongoDB URI options.
        - `warn` (optional): If ``True`` then warnings will be logged and
          invalid options will be ignored. Otherwise, invalid options will
          cause errors.
    """
    validated_options: MutableMapping[str, Any]
    if isinstance(options, _CaseInsensitiveDictionary):
        validated_options = _CaseInsensitiveDictionary()
        get_normed_key = lambda x: x  # noqa: E731
        get_setter_key = lambda x: options.cased_key(x)  # noqa: E731
    else:
        validated_options = {}
        get_normed_key = lambda x: x.lower()  # noqa: E731
        get_setter_key = lambda x: x  # noqa: E731

    for opt, value in options.items():
        normed_key = get_normed_key(opt)
        try:
            validator = URI_OPTIONS_VALIDATOR_MAP.get(normed_key, raise_config_error)
            value = validator(opt, value)
        except (ValueError, TypeError, ConfigurationError) as exc:
            if warn:
                warnings.warn(str(exc))
            else:
                raise
        else:
            validated_options[get_setter_key(normed_key)] = value
    return validated_options


def _esc_coll_name(encrypted_fields, name):
    return encrypted_fields.get("escCollection", f"enxcol_.{name}.esc")


def _ecoc_coll_name(encrypted_fields, name):
    return encrypted_fields.get("ecocCollection", f"enxcol_.{name}.ecoc")


# List of write-concern-related options.
WRITE_CONCERN_OPTIONS = frozenset(["w", "wtimeout", "wtimeoutms", "fsync", "j", "journal"])


class BaseObject:
    """A base class that provides attributes and methods common
    to multiple pymongo classes.

    SHOULD NOT BE USED BY DEVELOPERS EXTERNAL TO MONGODB.
    """

    def __init__(
        self,
        codec_options: CodecOptions,
        read_preference: _ServerMode,
        write_concern: WriteConcern,
        read_concern: ReadConcern,
    ) -> None:
        if not isinstance(codec_options, CodecOptions):
            raise TypeError("codec_options must be an instance of bson.codec_options.CodecOptions")
        self.__codec_options = codec_options

        if not isinstance(read_preference, _ServerMode):
            raise TypeError(
                "{!r} is not valid for read_preference. See "
                "pymongo.read_preferences for valid "
                "options.".format(read_preference)
            )
        self.__read_preference = read_preference

        if not isinstance(write_concern, WriteConcern):
            raise TypeError(
                "write_concern must be an instance of pymongo.write_concern.WriteConcern"
            )
        self.__write_concern = write_concern

        if not isinstance(read_concern, ReadConcern):
            raise TypeError("read_concern must be an instance of pymongo.read_concern.ReadConcern")
        self.__read_concern = read_concern

    @property
    def codec_options(self) -> CodecOptions:
        """Read only access to the :class:`~bson.codec_options.CodecOptions`
        of this instance.
        """
        return self.__codec_options

    @property
    def write_concern(self) -> WriteConcern:
        """Read only access to the :class:`~pymongo.write_concern.WriteConcern`
        of this instance.

        .. versionchanged:: 3.0
          The :attr:`write_concern` attribute is now read only.
        """
        return self.__write_concern

    def _write_concern_for(self, session):
        """Read only access to the write concern of this instance or session."""
        # Override this operation's write concern with the transaction's.
        if session and session.in_transaction:
            return DEFAULT_WRITE_CONCERN
        return self.write_concern

    @property
    def read_preference(self) -> _ServerMode:
        """Read only access to the read preference of this instance.

        .. versionchanged:: 3.0
          The :attr:`read_preference` attribute is now read only.
        """
        return self.__read_preference

    def _read_preference_for(self, session):
        """Read only access to the read preference of this instance or session."""
        # Override this operation's read preference with the transaction's.
        if session:
            return session._txn_read_preference() or self.__read_preference
        return self.__read_preference

    @property
    def read_concern(self) -> ReadConcern:
        """Read only access to the :class:`~pymongo.read_concern.ReadConcern`
        of this instance.

        .. versionadded:: 3.2
        """
        return self.__read_concern


class _CaseInsensitiveDictionary(abc.MutableMapping):
    def __init__(self, *args, **kwargs):
        self.__casedkeys = {}
        self.__data = {}
        self.update(dict(*args, **kwargs))

    def __contains__(self, key):
        return key.lower() in self.__data

    def __len__(self):
        return len(self.__data)

    def __iter__(self):
        return (key for key in self.__casedkeys)

    def __repr__(self):
        return str({self.__casedkeys[k]: self.__data[k] for k in self})

    def __setitem__(self, key, value):
        lc_key = key.lower()
        self.__casedkeys[lc_key] = key
        self.__data[lc_key] = value

    def __getitem__(self, key):
        return self.__data[key.lower()]

    def __delitem__(self, key):
        lc_key = key.lower()
        del self.__casedkeys[lc_key]
        del self.__data[lc_key]

    def __eq__(self, other):
        if not isinstance(other, abc.Mapping):
            return NotImplemented
        if len(self) != len(other):
            return False
        for key in other:
            if self[key] != other[key]:
                return False

        return True

    def get(self, key, default=None):
        return self.__data.get(key.lower(), default)

    def pop(self, key, *args, **kwargs):
        lc_key = key.lower()
        self.__casedkeys.pop(lc_key, None)
        return self.__data.pop(lc_key, *args, **kwargs)

    def popitem(self):
        lc_key, cased_key = self.__casedkeys.popitem()
        value = self.__data.pop(lc_key)
        return cased_key, value

    def clear(self):
        self.__casedkeys.clear()
        self.__data.clear()

    def setdefault(self, key, default=None):
        lc_key = key.lower()
        if key in self:
            return self.__data[lc_key]
        else:
            self.__casedkeys[lc_key] = key
            self.__data[lc_key] = default
            return default

    def update(self, other):
        if isinstance(other, _CaseInsensitiveDictionary):
            for key in other:
                self[other.cased_key(key)] = other[key]
        else:
            for key in other:
                self[key] = other[key]

    def cased_key(self, key):
        return self.__casedkeys[key.lower()]

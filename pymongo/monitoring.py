# Copyright 2015 MongoDB, Inc.
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

"""Tools to monitor driver events.

Use :func:`register` to register global listeners for specific events.
Currently only command events are published. Listeners must be
a subclass of :class:`CommandListener` and implement
:meth:`~CommandListener.started`, :meth:`~CommandListener.succeeded`, and
:meth:`~CommandListener.failed`.

For example, a simple command logger might be implemented like this::

    import logging

    from pymongo import monitoring

    class CommandLogger(monitoring.CommandListener):

        def started(self, event):
            logging.info("Command {0.command_name} with request id "
                         "{0.request_id} started on server "
                         "{0.connection_id}".format(event))

        def succeeded(self, event):
            logging.info("Command {0.command_name} with request id "
                         "{0.request_id} on server {0.connection_id} "
                         "succeeded in {0.duration_micros} "
                         "microseconds".format(event))

        def failed(self, event):
            logging.info("Command {0.command_name} with request id "
                         "{0.request_id} on server {0.connection_id} "
                         "failed in {0.duration_micros} "
                         "microseconds".format(event))

    monitoring.register(CommandLogger())

Event listeners can also be registered per instance of
:class:`~pymongo.mongo_client.MongoClient`::

    client = MongoClient(event_listeners=[CommandLogger()])

Note that previously registered global listeners are automatically included when
configuring per client event listeners. Registering a new global listener will
not add that listener to existing client instances.

.. note:: Events are delivered **synchronously**. Application threads block
  waiting for event handlers (e.g. :meth:`~CommandListener.started`) to
  return. Care must be taken to ensure that your event handlers are efficient
  enough to not adversely affect overall application performance.

.. warning:: The command documents published through this API are *not* copies.
  If you intend to modify them in any way you must copy them in your event
  handler first.
"""

import sys
import traceback

from collections import namedtuple, Sequence

_Listeners = namedtuple('Listeners', ('command_listeners',))

_LISTENERS = _Listeners([])


class CommandListener(object):
    """Abstract base class for command listeners."""

    def started(self, event):
        """Abstract method to handle CommandStartedEvent.

        :Parameters:
          - `event`: An instance of :class:`CommandStartedEvent`
        """
        raise NotImplementedError

    def succeeded(self, event):
        """Abstract method to handle CommandSucceededEvent.

        :Parameters:
          - `event`: An instance of :class:`CommandSucceededEvent`
        """
        raise NotImplementedError

    def failed(self, event):
        """Abstract method to handle CommandFailedEvent.

        :Parameters:
          - `event`: An instance of :class:`CommandFailedEvent`
        """
        raise NotImplementedError


def _to_micros(dur):
    """Convert duration 'dur' to microseconds."""
    if hasattr(dur, 'total_seconds'):
        return int(dur.total_seconds() * 10e5)
    # Python 2.6
    return dur.microseconds + (dur.seconds + dur.days * 24 * 3600) * 1000000


def _validate_event_listeners(option, listeners):
    """Validate event listeners"""
    if not isinstance(listeners, Sequence):
        raise TypeError("%s must be a list or tuple" % (option,))
    for listener in listeners:
        if not isinstance(listener, CommandListener):
            raise TypeError("Only subclasses of "
                            "pymongo.monitoring.CommandListener are supported")
    return listeners


def register(listener):
    """Register a global event listener.

    :Parameters:
      - `listener`: A subclass of :class:`CommandListener`.
    """
    _validate_event_listeners('listener', [listener])
    _LISTENERS.command_listeners.append(listener)


def _handle_exception():
    """Print exceptions raised by subscribers to stderr."""
    # Heavily influenced by logging.Handler.handleError.

    # See note here:
    # https://docs.python.org/3.4/library/sys.html#sys.__stderr__
    if sys.stderr:
        einfo = sys.exc_info()
        try:
            traceback.print_exception(einfo[0], einfo[1], einfo[2],
                                      None, sys.stderr)
        except IOError:
            pass
        finally:
            del einfo

# Note - to avoid bugs from forgetting which if these is all lowercase and
# which are camelCase, and at the same time avoid having to add a test for
# every command, use all lowercase here and test against command_name.lower().
_SENSITIVE_COMMANDS = set(
    ["authenticate", "saslstart", "saslcontinue", "getnonce", "createuser",
     "updateuser", "copydbgetnonce", "copydbsaslstart", "copydb"])


class _CommandEvent(object):
    """Base class for command events."""

    __slots__ = ("__cmd_name", "__rqst_id", "__conn_id", "__op_id")

    def __init__(self, command_name, request_id, connection_id, operation_id):
        self.__cmd_name = command_name
        self.__rqst_id = request_id
        self.__conn_id = connection_id
        self.__op_id = operation_id

    @property
    def command_name(self):
        """The command name."""
        return self.__cmd_name

    @property
    def request_id(self):
        """The request id for this operation."""
        return self.__rqst_id

    @property
    def connection_id(self):
        """The address (host, port) of the server this command was sent to."""
        return self.__conn_id

    @property
    def operation_id(self):
        """An id for this series of events or None."""
        return self.__op_id


class CommandStartedEvent(_CommandEvent):
    """Event published when a command starts.

    :Parameters:
      - `command`: The command document.
      - `database_name`: The name of the database this command was run against.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
      - `operation_id`: An optional identifier for a series of related events.
    """
    __slots__ = ("__cmd", "__db")

    def __init__(self, command, database_name, *args):
        if not command:
            raise ValueError("%r is not a valid command" % (command,))
        # Command name must be first key.
        command_name = next(iter(command))
        super(CommandStartedEvent, self).__init__(command_name, *args)
        if command_name.lower() in _SENSITIVE_COMMANDS:
            self.__cmd = {}
        else:
            self.__cmd = command
        self.__db = database_name

    @property
    def command(self):
        """The command document."""
        return self.__cmd

    @property
    def database_name(self):
        """The name of the database this command was run against."""
        return self.__db


class CommandSucceededEvent(_CommandEvent):
    """Event published when a command succeeds.

    :Parameters:
      - `duration`: The command duration as a datetime.timedelta.
      - `reply`: The server reply document.
      - `command_name`: The command name.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
      - `operation_id`: An optional identifier for a series of related events.
    """
    __slots__ = ("__duration_micros", "__reply")

    def __init__(self, duration, reply, command_name,
                 request_id, connection_id, operation_id):
        super(CommandSucceededEvent, self).__init__(
            command_name, request_id, connection_id, operation_id)
        self.__duration_micros = _to_micros(duration)
        if command_name.lower() in _SENSITIVE_COMMANDS:
            self.__reply = {}
        else:
            self.__reply = reply

    @property
    def duration_micros(self):
        """The duration of this operation in microseconds."""
        return self.__duration_micros

    @property
    def reply(self):
        """The server failure document for this operation."""
        return self.__reply


class CommandFailedEvent(_CommandEvent):
    """Event published when a command fails.

    :Parameters:
      - `duration`: The command duration as a datetime.timedelta.
      - `failure`: The server reply document.
      - `command_name`: The command name.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
      - `operation_id`: An optional identifier for a series of related events.
    """
    __slots__ = ("__duration_micros", "__failure")

    def __init__(self, duration, failure, *args):
        super(CommandFailedEvent, self).__init__(*args)
        self.__duration_micros = _to_micros(duration)
        self.__failure = failure

    @property
    def duration_micros(self):
        """The duration of this operation in microseconds."""
        return self.__duration_micros

    @property
    def failure(self):
        """The server failure document for this operation."""
        return self.__failure


class _EventListeners(object):
    """Configure event listeners for a client instance.

    Any event listeners registered globally are included by default.

    :Parameters:
      - `listeners`: A list of event listeners.
    """
    def __init__(self, listeners):
        self.__command_listeners = _LISTENERS.command_listeners[:]
        if listeners is not None:
            self.__command_listeners.extend(listeners)
        self.__enabled_for_commands = bool(self.__command_listeners)

    @property
    def enabled_for_commands(self):
        """Are any CommandListener instances registered?"""
        return self.__enabled_for_commands

    @property
    def event_listeners(self):
        """List of registered event listeners."""
        return self.__command_listeners[:]

    def publish_command_start(self, command, database_name,
                              request_id, connection_id, op_id=None):
        """Publish a CommandStartedEvent to all command listeners.

        :Parameters:
          - `command`: The command document.
          - `database_name`: The name of the database this command was run
            against.
          - `request_id`: The request id for this operation.
          - `connection_id`: The address (host, port) of the server this
            command was sent to.
          - `op_id`: The (optional) operation id for this operation.
        """
        if op_id is None:
            op_id = request_id
        event = CommandStartedEvent(
            command, database_name, request_id, connection_id, op_id)
        for subscriber in self.__command_listeners:
            try:
                subscriber.started(event)
            except Exception:
                _handle_exception()


    def publish_command_success(self, duration, reply, command_name,
                                request_id, connection_id, op_id=None):
        """Publish a CommandSucceededEvent to all command listeners.

        :Parameters:
          - `duration`: The command duration as a datetime.timedelta.
          - `reply`: The server reply document.
          - `command_name`: The command name.
          - `request_id`: The request id for this operation.
          - `connection_id`: The address (host, port) of the server this
            command was sent to.
          - `op_id`: The (optional) operation id for this operation.
        """
        if op_id is None:
            op_id = request_id
        event = CommandSucceededEvent(
            duration, reply, command_name, request_id, connection_id, op_id)
        for subscriber in self.__command_listeners:
            try:
                subscriber.succeeded(event)
            except Exception:
                _handle_exception()


    def publish_command_failure(self, duration, failure, command_name,
                                request_id, connection_id, op_id=None):
        """Publish a CommandFailedEvent to all command listeners.

        :Parameters:
          - `duration`: The command duration as a datetime.timedelta.
          - `failure`: The server reply document or failure description
            document.
          - `command_name`: The command name.
          - `request_id`: The request id for this operation.
          - `connection_id`: The address (host, port) of the server this
            command was sent to.
          - `op_id`: The (optional) operation id for this operation.
        """
        if op_id is None:
            op_id = request_id
        event = CommandFailedEvent(
            duration, failure, command_name, request_id, connection_id, op_id)
        for subscriber in self.__command_listeners:
            try:
                subscriber.failed(event)
            except Exception:
                _handle_exception()

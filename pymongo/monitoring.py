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

Use :func:`subscribe` to register subscribers for specific events. Only
events of type :data:`COMMAND` are currently supported. Subscribers must be
a subclass of :class:`Subscriber` and implement :meth:`~Subscriber.started`,
:meth:`~Subscriber.succeeded`, and :meth:`~Subscriber.failed`.

For example, a simple logging subscriber might be implemented like this::

    import logging

    from pymongo import monitoring

    class LoggingSubscriber(monitoring.Subscriber):

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

    monitoring.subscribe(LoggingSubscriber(), monitoring.COMMAND)
"""

import sys
import traceback

_SUBSCRIBERS = []

COMMAND = 0


class Subscriber(object):
    """Abstract base class for all subscribers."""

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
        return int(dur.total_seconds() * 10e6)
    # Python 2.6
    return dur.microseconds + (dur.seconds + dur.days * 24 * 3600) * 10e6


def _validate_events(events):
    """Validate that 'event' is an int."""
    if not isinstance(events, int) or events != COMMAND:
        raise ValueError("only events of type monitoring.COMMAND "
                         "are currently supported")


def subscribe(subscriber, events=COMMAND):
    """Register a subscriber for events.

    This version of PyMongo only publishes events of type :data:`COMMAND`.

    :Parameters:
      - `subscriber`: A subclass of abstract class :class:`Subscriber`.
      - `events`: Optional integer to set event subscriptions
    """
    _validate_events(events)
    if not isinstance(subscriber, Subscriber):
        raise TypeError("subscriber must be a subclass "
                        "of pymongo.monitoring.Subscriber")
    _SUBSCRIBERS.append(subscriber)


def get_subscribers(event=COMMAND):
    """Get the list of subscribers for `event`.

    :Parameters:
      - `event`: Return subscribers for this event type.
    """
    _validate_events(event)
    return _SUBSCRIBERS[:]


def enabled():
    return bool(_SUBSCRIBERS)


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


def publish_command_start(command, database_name, request_id, connection_id):
    """Publish a CommandStartedEvent to all command event subscribers.

    :Parameters:
      - `command`: The command document.
      - `database_name`: The name of the database this command was run against.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
    """
    event = CommandStartedEvent(
        command, database_name, request_id, connection_id)
    for subscriber in get_subscribers(COMMAND):
        try:
            subscriber.started(event)
        except Exception:
            _handle_exception()


def publish_command_success(
        duration, reply, command_name, request_id, connection_id):
    """Publish a CommandSucceededEvent to all command event subscribers.

    :Parameters:
      - `duration`: The command duration as a datetime.timedelta.
      - `reply`: The server reply document.
      - `command_name`: The command name.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
    """
    event = CommandSucceededEvent(
        duration, reply, command_name, request_id, connection_id)
    for subscriber in get_subscribers(COMMAND):
        try:
            subscriber.succeeded(event)
        except Exception:
            _handle_exception()


def publish_command_failure(
        duration, failure, command_name, request_id, connection_id):
    """Publish a CommandFailedEvent to all command event subscribers.

    :Parameters:
      - `duration`: The command duration as a datetime.timedelta.
      - `failure`: The server reply document.
      - `command_name`: The command name.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
    """
    event = CommandFailedEvent(
        duration, failure, command_name, request_id, connection_id)
    for subscriber in get_subscribers(COMMAND):
        try:
            subscriber.failed(event)
        except Exception:
            _handle_exception()


class _CommandEvent(object):
    """Base class for command events."""

    __slots__ = ("__cmd_name", "__rqst_id", "__conn_id")

    def __init__(self, command_name, request_id, connection_id):
        self.__cmd_name = command_name
        self.__rqst_id = request_id
        self.__conn_id = connection_id

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


class CommandStartedEvent(_CommandEvent):
    """Event published when a command starts.

    :Parameters:
      - `command`: The command document.
      - `database_name`: The name of the database this command was run against.
      - `request_id`: The request id for this operation.
      - `connection_id`: The address (host, port) of the server this command
        was sent to.
    """
    __slots__ = ("__cmd", "__db")

    def __init__(self, command, database_name, request_id, connection_id):
        if not command:
            raise ValueError("%r is not a valid command" % (command,))
        # Command name must be first key.
        command_name = next(iter(command))
        super(CommandStartedEvent, self).__init__(command_name,
                                                  request_id,
                                                  connection_id)
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
    """
    __slots__ = ("__duration_micros", "__reply")

    def __init__(self, duration, reply, *args):
        super(CommandSucceededEvent, self).__init__(*args)
        self.__duration_micros = _to_micros(duration)
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

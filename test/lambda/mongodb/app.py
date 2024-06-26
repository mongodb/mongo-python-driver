"""
Lambda function for Python Driver testing

Creates the client that is cached for all requests, subscribes to
relevant events, and forces the connection pool to get populated.
"""
from __future__ import annotations

import json
import os

from bson import has_c as has_bson_c
from pymongo import MongoClient
from pymongo import has_c as has_pymongo_c
from pymongo.monitoring import (
    CommandListener,
    ConnectionPoolListener,
    ServerHeartbeatListener,
)

open_connections = 0
heartbeat_count = 0
streaming_heartbeat_count = 0
total_heartbeat_duration = 0
total_commands = 0
total_command_duration = 0

# Ensure we are using C extensions
assert has_bson_c()
assert has_pymongo_c()


class CommandHandler(CommandListener):
    def started(self, event):
        print("command started", event)

    def succeeded(self, event):
        global total_commands, total_command_duration
        total_commands += 1
        total_command_duration += event.duration_micros / 1e6
        print("command succeeded", event)

    def failed(self, event):
        global total_commands, total_command_duration
        total_commands += 1
        total_command_duration += event.duration_micros / 1e6
        print("command failed", event)


class ServerHeartbeatHandler(ServerHeartbeatListener):
    def started(self, event):
        print("server heartbeat started", event)

    def succeeded(self, event):
        global heartbeat_count, total_heartbeat_duration, streaming_heartbeat_count
        heartbeat_count += 1
        total_heartbeat_duration += event.duration
        if event.awaited:
            streaming_heartbeat_count += 1
        print("server heartbeat succeeded", event)

    def failed(self, event):
        global heartbeat_count, total_heartbeat_duration
        heartbeat_count += 1
        total_heartbeat_duration += event.duration
        print("server heartbeat  failed", event)


class ConnectionHandler(ConnectionPoolListener):
    def connection_created(self, event):
        global open_connections
        open_connections += 1
        print("connection created")

    def connection_ready(self, event):
        pass

    def connection_closed(self, event):
        global open_connections
        open_connections -= 1
        print("connection closed")

    def connection_check_out_started(self, event):
        pass

    def connection_check_out_failed(self, event):
        pass

    def connection_checked_out(self, event):
        pass

    def connection_checked_in(self, event):
        pass

    def pool_created(self, event):
        pass

    def pool_ready(self, event):
        pass

    def pool_cleared(self, event):
        pass

    def pool_closed(self, event):
        pass


listeners = [CommandHandler(), ServerHeartbeatHandler(), ConnectionHandler()]
print("Creating client")
client = MongoClient(os.environ["MONGODB_URI"], event_listeners=listeners)


# Populate the connection pool.
print("Connecting")
client.lambdaTest.list_collections()
print("Connected")


# Create the response to send back.
def create_response():
    return dict(
        averageCommandDuration=total_command_duration / total_commands,
        averageHeartbeatDuration=total_heartbeat_duration / heartbeat_count
        if heartbeat_count
        else 0,
        openConnections=open_connections,
        heartbeatCount=heartbeat_count,
    )


# Reset the numbers.
def reset():
    global \
        open_connections, \
        heartbeat_count, \
        total_heartbeat_duration, \
        total_commands, \
        total_command_duration
    open_connections = 0
    heartbeat_count = 0
    total_heartbeat_duration = 0
    total_commands = 0
    total_command_duration = 0


def lambda_handler(event, context):
    """
    The handler function itself performs an insert/delete and returns the
    id of the document in play.
    """
    print("initializing")
    db = client.lambdaTest
    collection = db.test
    result = collection.insert_one({"n": 1})
    collection.delete_one({"_id": result.inserted_id})
    # Create the response and then reset the numbers.
    response = json.dumps(create_response())
    reset()
    print("finished!")
    assert (
        streaming_heartbeat_count == 0
    ), f"streaming_heartbeat_count was {streaming_heartbeat_count} not 0"

    return dict(statusCode=200, body=response)

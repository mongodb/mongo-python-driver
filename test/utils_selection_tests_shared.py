# Copyright 2015-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for testing Server Selection and Max Staleness."""
from __future__ import annotations

import datetime
import os
import sys

sys.path[0:0] = [""]

from pymongo.common import MIN_SUPPORTED_WIRE_VERSION, clean_node
from pymongo.hello import Hello, HelloCompat
from pymongo.server_description import ServerDescription


def get_addresses(server_list):
    seeds = []
    hosts = []
    for server in server_list:
        seeds.append(clean_node(server["address"]))
        hosts.append(server["address"])
    return seeds, hosts


def make_last_write_date(server):
    epoch = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc).replace(tzinfo=None)
    millis = server.get("lastWrite", {}).get("lastWriteDate")
    if millis:
        diff = ((millis % 1000) + 1000) % 1000
        seconds = (millis - diff) / 1000
        micros = diff * 1000
        return epoch + datetime.timedelta(seconds=seconds, microseconds=micros)
    else:
        # "Unknown" server.
        return epoch


def make_server_description(server, hosts):
    """Make a ServerDescription from server info in a JSON test."""
    server_type = server["type"]
    if server_type in ("Unknown", "PossiblePrimary"):
        return ServerDescription(clean_node(server["address"]), Hello({}))

    hello_response = {"ok": True, "hosts": hosts}
    if server_type not in ("Standalone", "Mongos", "RSGhost"):
        hello_response["setName"] = "rs"

    if server_type == "RSPrimary":
        hello_response[HelloCompat.LEGACY_CMD] = True
    elif server_type == "RSSecondary":
        hello_response["secondary"] = True
    elif server_type == "Mongos":
        hello_response["msg"] = "isdbgrid"
    elif server_type == "RSGhost":
        hello_response["isreplicaset"] = True
    elif server_type == "RSArbiter":
        hello_response["arbiterOnly"] = True

    hello_response["lastWrite"] = {"lastWriteDate": make_last_write_date(server)}

    for field in "maxWireVersion", "tags", "idleWritePeriodMillis":
        if field in server:
            hello_response[field] = server[field]

    hello_response.setdefault("maxWireVersion", MIN_SUPPORTED_WIRE_VERSION)

    # Sets _last_update_time to now.
    sd = ServerDescription(
        clean_node(server["address"]),
        Hello(hello_response),
        round_trip_time=server["avg_rtt_ms"] / 1000.0,
    )

    if "lastUpdateTime" in server:
        sd._last_update_time = server["lastUpdateTime"] / 1000.0  # ms to sec.

    return sd


def get_topology_type_name(scenario_def):
    td = scenario_def["topology_description"]
    name = td["type"]
    if name == "Unknown":
        # PyMongo never starts a topology in type Unknown.
        return "Sharded" if len(td["servers"]) > 1 else "Single"
    else:
        return name

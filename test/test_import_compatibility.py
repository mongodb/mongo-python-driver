#
# Copyright 2024-present MongoDB, Inc.
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

"""Test that public API imports are consistent with our public docs."""
from __future__ import annotations


def test_imports():
    import pymongo
    from pymongo import (
        auth_oidc,
        change_stream,
        client_options,
        client_session,
        collation,
        collection,
        command_cursor,
        cursor,
        database,
        driver_info,
        encryption,
        encryption_options,
        errors,
        event_loggers,
        mongo_client,
        monitoring,
        operations,
        pool,
        read_concern,
        read_preferences,
        results,
        server_api,
        server_description,
        topology_description,
        uri_parser,
        write_concern,
    )

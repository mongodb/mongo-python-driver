# Copyright 2009-2014 MongoDB, Inc.
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

"""Criteria to select some ServerDescriptions out of a list."""

from pymongo.ismaster import SERVER_TYPE


def any_server_selector(server_descriptions):
    return server_descriptions


def writable_server_selector(server_descriptions):
    return [s for s in server_descriptions if s.is_writable]


def secondary_server_selector(server_descriptions):
    return [s for s in server_descriptions
            if s.server_type == SERVER_TYPE.RSSecondary]

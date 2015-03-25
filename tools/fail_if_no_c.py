# Copyright 2009-2015 MongoDB, Inc.
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

"""Fail if the C extension module doesn't exist.

Only really intended to be used by internal build scripts.
"""

import sys
sys.path[0:0] = [""]

import bson
import pymongo

if not pymongo.has_c() or not bson.has_c():
    sys.exit("could not load C extensions")

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
from __future__ import annotations

import logging
import sys

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

sys.path[0:0] = [""]

import bson  # noqa: E402
import pymongo  # noqa: E402


def main() -> None:
    if not pymongo.has_c() or not bson.has_c():
        try:
            from pymongo import _cmessage  # type:ignore[attr-defined] # noqa: F401
        except Exception as e:
            LOGGER.exception(e)
        try:
            from bson import _cbson  # type:ignore[attr-defined] # noqa: F401
        except Exception as e:
            LOGGER.exception(e)
        sys.exit("could not load C extensions")


if __name__ == "__main__":
    main()

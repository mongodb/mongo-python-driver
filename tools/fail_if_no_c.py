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

import os
import subprocess
import sys
from pathlib import Path

sys.path[0:0] = [""]

import bson  # noqa: E402
import pymongo  # noqa: E402

if not pymongo.has_c() or not bson.has_c():
    try:
        from pymongo import _cmessage  # type:ignore[attr-defined] # noqa: F401
    except Exception as e:
        print(e)
    try:
        from bson import _cbson  # type:ignore[attr-defined] # noqa: F401
    except Exception as e:
        print(e)
    sys.exit("could not load C extensions")

if os.environ.get("ENSURE_UNIVERSAL2") == "1":
    parent_dir = Path(pymongo.__path__[0]).parent
    for pkg in ["pymongo", "bson", "grifs"]:
        for so_file in Path(f"{parent_dir}/{pkg}").glob("*.so"):
            print(f"Checking universal2 compatibility in {so_file}...")
            output = subprocess.check_output(["file", so_file])  # noqa: S603, S607
            if "arm64" not in output.decode("utf-8"):
                sys.exit("Universal wheel was not compiled with arm64 support")
            if "x86_64" not in output.decode("utf-8"):
                sys.exit("Universal wheel was not compiled with x86_64 support")

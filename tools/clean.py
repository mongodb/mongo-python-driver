# Copyright 2009-2014 MongoDB, Inc.
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

"""Clean up script for build artifacts.

Only really intended to be used by internal build scripts.
"""

import os
import sys

try:
    os.remove("pymongo/_cmessage.so")
    os.remove("bson/_cbson.so")
except:
    pass

try:
    os.remove("pymongo/_cmessage.pyd")
    os.remove("bson/_cbson.pyd")
except:
    pass

try:
    from pymongo import _cmessage
    sys.exit("could still import _cmessage")
except ImportError:
    pass

try:
    from bson import _cbson
    sys.exit("could still import _cbson")
except ImportError:
    pass

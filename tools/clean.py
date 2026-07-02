# Copyright 2009-present MongoDB, Inc.
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

"""Remove built C extensions from the source tree.

Used as cibuildwheel's before-build hook so artifacts from one Python
version do not leak into the wheel built for the next.
"""

from __future__ import annotations

from pathlib import Path

for pkg in ("bson", "pymongo"):
    for pattern in ("*.so", "*.pyd"):
        for path in Path(pkg).glob(pattern):
            path.unlink()
            print(f"removed {path}")

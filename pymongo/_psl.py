# Copyright 2024-present MongoDB, Inc.
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

"""Public Suffix List lookup for srvAllowedHostsSuffix validation."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

_PUBLIC_SUFFIXES: Optional[tuple[set[str], set[str], set[str]]] = None


def _load_public_suffixes() -> tuple[set[str], set[str], set[str]]:
    path = Path(__file__).parent / "public_suffix_list.dat"
    suffixes: set[str] = set()
    wildcards: set[str] = set()
    exceptions: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()  # noqa: PLW2901
            if not line or line.startswith("//"):
                continue
            if line.startswith("!"):
                exceptions.add(line[1:].lower())
            elif line.startswith("*."):
                wildcards.add(line[2:].lower())
            else:
                suffixes.add(line.lower())
    return suffixes, wildcards, exceptions


def is_public_suffix(domain: str) -> bool:
    """Return True if domain is a public suffix per the bundled Public Suffix List."""
    global _PUBLIC_SUFFIXES  # noqa: PLW0603
    if _PUBLIC_SUFFIXES is None:
        _PUBLIC_SUFFIXES = _load_public_suffixes()
    suffixes, wildcards, exceptions = _PUBLIC_SUFFIXES

    domain = domain.lower().strip(".")
    if domain in exceptions:
        return False
    if domain in suffixes:
        return True
    parts = domain.split(".")
    return len(parts) > 1 and ".".join(parts[1:]) in wildcards

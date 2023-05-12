# Copyright 2023-present MongoDB, Inc.
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

"""Azure helpers."""
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
from urllib.request import Request, urlopen

_CACHE: Dict[str, Tuple[str, datetime]] = {}
_TOKEN_BUFFER_MINUTES = 5


def _get_azure_token(resource: str, timeout: float = 5) -> str:
    """Get an azure token for a given resource."""
    cached_value = _CACHE.get(resource)
    now_utc = datetime.now(timezone.utc)
    if cached_value:
        token, exp_utc = cached_value
        buffer_seconds = _TOKEN_BUFFER_MINUTES * 60
        if (exp_utc - now_utc).total_seconds() >= buffer_seconds:
            return token
        del _CACHE[resource]

    url = "http://169.254.169.254/metadata/identity/oauth2/token"
    url += "?api-version=2018-02-01"
    url += f"&resource={resource}"
    headers = {"Metadata": "true", "Accept": "application/json"}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            status = response.status
            body = response.read().decode("utf8")
    except Exception as e:
        msg = "Failed to acquire IMDS access token: %s" % e
        raise ValueError(msg)

    if status != 200:
        print(body)
        msg = "Failed to acquire IMDS access token."
        raise ValueError(msg)
    try:
        data = json.loads(body)
    except Exception:
        raise ValueError("Azure IMDS response must be in JSON format.")

    for key in ["access_token", "expires_in"]:
        if not data.get(key):
            msg = "Azure IMDS response must contain %s, but was %s."
            msg = msg % (key, body)
            raise ValueError(msg)

    token = data["access_token"]
    exp_utc = now_utc + timedelta(seconds=data["expires_in"])
    _CACHE[resource] = (token, exp_utc)
    return token

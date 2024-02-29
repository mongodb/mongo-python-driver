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

"""GCP helpers."""
from __future__ import annotations

import json
from typing import Any
from urllib.request import Request, urlopen


def _get_gcp_response(audience: str, timeout: float = 5) -> dict[str, Any]:
    url = "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity"
    url += f"?audience={audience}"
    headers = {"Metadata-Flavor": "Google", "Accept": "application/json"}
    request = Request(url, headers=headers)  # noqa: S310
    print("fetching url", url)  # noqa: T201
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            status = response.status
            body = response.read().decode("utf8")
    except Exception as e:
        msg = "Failed to acquire IMDS access token: %s" % e
        raise ValueError(msg) from None

    if status != 200:
        msg = "Failed to acquire IMDS access token."
        raise ValueError(msg)
    try:
        data = json.loads(body)
    except Exception:
        raise ValueError("GCP IMDS response must be in JSON format.") from None

    for key in ["access_token", "expires_in"]:
        if not data.get(key):
            msg = "GCP IMDS response must contain %s, but was %s."
            msg = msg % (key, body)
            raise ValueError(msg)

    return data

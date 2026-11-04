# Copyright 2024-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you
# may not use this file except in compliance with the License.  You
# may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.


"""Constants, types, and classes shared across OIDC auth implementations."""

from __future__ import annotations

import abc
import os
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Optional
from urllib.parse import quote

import bson
from bson.binary import Binary
from pymongo._azure_helpers import _get_azure_response
from pymongo._gcp_helpers import _get_gcp_response
from pymongo.errors import ConfigurationError, OperationFailure
from pymongo.helpers_shared import _AUTHENTICATION_FAILURE_CODE

if TYPE_CHECKING:
    from pymongo.auth_shared import MongoCredential


@dataclass
class OIDCIdPInfo:
    issuer: str
    clientId: Optional[str] = field(default=None)
    requestScopes: Optional[list[str]] = field(default=None)


@dataclass
class OIDCCallbackContext:
    timeout_seconds: float
    username: str
    version: int
    refresh_token: Optional[str] = field(default=None)
    idp_info: Optional[OIDCIdPInfo] = field(default=None)


@dataclass
class OIDCCallbackResult:
    access_token: str
    expires_in_seconds: Optional[float] = field(default=None)
    refresh_token: Optional[str] = field(default=None)


class OIDCCallback(abc.ABC):
    """A base class for defining OIDC callbacks."""

    @abc.abstractmethod
    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        """Convert the given BSON value into our own type."""


@dataclass
class _OIDCProperties:
    callback: Optional[OIDCCallback] = field(default=None)
    human_callback: Optional[OIDCCallback] = field(default=None)
    environment: Optional[str] = field(default=None)
    allowed_hosts: list[str] = field(default_factory=list)
    token_resource: Optional[str] = field(default=None)
    username: str = ""


"""Mechanism properties for MONGODB-OIDC authentication."""

TOKEN_BUFFER_MINUTES = 5
HUMAN_CALLBACK_TIMEOUT_SECONDS = 5 * 60
CALLBACK_VERSION = 1
MACHINE_CALLBACK_TIMEOUT_SECONDS = 60
TIME_BETWEEN_CALLS_SECONDS = 0.1


class _OIDCTestCallback(OIDCCallback):
    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        token_file = os.environ.get("OIDC_TOKEN_FILE")
        if not token_file:
            raise RuntimeError(
                'MONGODB-OIDC with an "test" provider requires "OIDC_TOKEN_FILE" to be set'
            )
        with open(token_file) as fid:
            return OIDCCallbackResult(access_token=fid.read().strip())


class _OIDCAWSCallback(OIDCCallback):
    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
        if not token_file:
            raise RuntimeError(
                'MONGODB-OIDC with an "aws" provider requires "AWS_WEB_IDENTITY_TOKEN_FILE" to be set'
            )
        with open(token_file) as fid:
            return OIDCCallbackResult(access_token=fid.read().strip())


class _OIDCAzureCallback(OIDCCallback):
    def __init__(self, token_resource: str) -> None:
        self.token_resource = quote(token_resource)

    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        resp = _get_azure_response(self.token_resource, context.username, context.timeout_seconds)
        return OIDCCallbackResult(
            access_token=resp["access_token"], expires_in_seconds=resp["expires_in"]
        )


class _OIDCGCPCallback(OIDCCallback):
    def __init__(self, token_resource: str) -> None:
        self.token_resource = quote(token_resource)

    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        resp = _get_gcp_response(self.token_resource, context.timeout_seconds)
        return OIDCCallbackResult(access_token=resp["access_token"])


class _OIDCK8SCallback(OIDCCallback):
    def fetch(self, context: OIDCCallbackContext) -> OIDCCallbackResult:
        return OIDCCallbackResult(access_token=_get_k8s_token())


def _get_k8s_token() -> str:
    fname = "/var/run/secrets/kubernetes.io/serviceaccount/token"
    for key in ["AZURE_FEDERATED_TOKEN_FILE", "AWS_WEB_IDENTITY_TOKEN_FILE"]:
        if key in os.environ:
            fname = os.environ[key]
    with open(fname) as fid:
        return fid.read()


@dataclass
class _OIDCAuthenticatorBase:
    """Shared base class for async and sync _OIDCAuthenticator.

    Subclasses contain all methods that perform I/O.
    """

    username: str
    properties: _OIDCProperties
    refresh_token: Optional[str] = field(default=None)
    access_token: Optional[str] = field(default=None)
    idp_info: Optional[OIDCIdPInfo] = field(default=None)
    token_gen_id: int = field(default=0)
    last_call_time: float = field(default=0)

    def get_spec_auth_cmd(self) -> Optional[MutableMapping[str, Any]]:
        """Get the appropriate speculative auth command."""
        if not self.access_token:
            return None
        return self._get_start_command({"jwt": self.access_token})

    def _is_auth_error(self, err: Exception) -> bool:
        if not isinstance(err, OperationFailure):
            return False
        return err.code == _AUTHENTICATION_FAILURE_CODE

    def _invalidate(self, oidc_token_gen_id: Optional[int]) -> None:
        # Ignore the invalidation if a token gen id is given and is less than our
        # current token gen id.
        if (oidc_token_gen_id or 0) < self.token_gen_id:
            return
        self.access_token = None

    def _get_start_command(self, payload: Optional[Mapping[str, Any]]) -> MutableMapping[str, Any]:
        if payload is None:
            principal_name = self.username
            if principal_name:
                payload = {"n": principal_name}
            else:
                payload = {}
        bin_payload = Binary(bson.encode(payload))
        return {"saslStart": 1, "mechanism": "MONGODB-OIDC", "payload": bin_payload}

    def _get_continue_command(
        self, payload: Mapping[str, Any], start_resp: Mapping[str, Any]
    ) -> MutableMapping[str, Any]:
        bin_payload = Binary(bson.encode(payload))
        return {
            "saslContinue": 1,
            "payload": bin_payload,
            "conversationId": start_resp["conversationId"],
        }


def _get_authenticator(
    credentials: MongoCredential, address: tuple[str, int], authenticator_cls: Callable[..., Any]
) -> Any:
    # Extract values.
    principal_name = credentials.username
    properties = credentials.mechanism_properties

    # Validate that the address is allowed.
    if properties.human_callback is not None:
        found = False
        allowed_hosts = properties.allowed_hosts
        for patt in allowed_hosts:
            if patt == address[0]:
                found = True
            elif patt.startswith("*.") and address[0].endswith(patt[1:]):
                found = True
        if not found:
            raise ConfigurationError(
                f"Refusing to connect to {address[0]}, which is not in authOIDCAllowedHosts: {allowed_hosts}"
            )

    if credentials.cache.data:
        return credentials.cache.data

    # Get or create the cache data.
    credentials.cache.data = authenticator_cls(username=principal_name, properties=properties)
    return credentials.cache.data

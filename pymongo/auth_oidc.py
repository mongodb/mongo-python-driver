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

"""MONGODB-OIDC Authentication helpers."""
from __future__ import annotations

import abc
import os
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Union

import bson
from bson.binary import Binary
from bson.son import SON
from pymongo._csot import remaining
from pymongo.azure_helpers import _get_azure_response
from pymongo.errors import ConfigurationError, OperationFailure

if TYPE_CHECKING:
    from pymongo.auth import MongoCredential
    from pymongo.pool import Connection


@dataclass
class OIDCIdPInfo:
    issuer: str
    clientId: str
    requestScopes: Optional[list[str]] = field(default=None)


@dataclass
class OIDCHumanCallbackContext:
    timeout_seconds: float
    version: int
    refresh_token: Optional[str] = field(default=None)


@dataclass
class OIDCMachineCallbackContext:
    timeout_seconds: float
    version: int


@dataclass
class OIDCHumanCallbackResult:
    access_token: str
    expires_in_seconds: Optional[float] = field(default=None)
    refresh_token: Optional[str] = field(default=None)


@dataclass
class OIDCMachineCallbackResult:
    access_token: str
    expires_in_seconds: Optional[float] = field(default=None)


class OIDCMachineCallback(abc.ABC):
    """A base class for defining OIDC machine (workload federation)
    callbacks.
    """

    @abc.abstractmethod
    def fetch(self, context: OIDCMachineCallbackContext) -> OIDCMachineCallbackResult:
        """Convert the given BSON value into our own type."""


class OIDCHumanCallback(abc.ABC):
    """A base class for defining OIDC human (workforce federation)
    callbacks.
    """

    @abc.abstractmethod
    def fetch(
        self, idp_info: OIDCIdPInfo, context: OIDCHumanCallbackContext
    ) -> OIDCHumanCallbackResult:
        """Convert the given BSON value into our own type."""


@dataclass
class _OIDCProperties:
    request_token_callback: Optional[OIDCHumanCallback] = field(default=None)
    custom_token_callback: Optional[OIDCMachineCallback] = field(default=None)
    provider_name: Optional[str] = field(default=None)
    allowed_hosts: list[str] = field(default_factory=list)


"""Mechanism properties for MONGODB-OIDC authentication."""

TOKEN_BUFFER_MINUTES = 5
HUMAN_CALLBACK_TIMEOUT_SECONDS = 5 * 60
HUMAN_CALLBACK_VERSION = 1
MACHINE_CALLBACK_TIMEOUT_SECONDS = 60
MACHINE_CALLBACK_VERSION = 1
TIME_BETWEEN_CALLS_SECONDS = 0.1


def _get_authenticator(
    credentials: MongoCredential, address: tuple[str, int]
) -> _OIDCAuthenticator:
    if credentials.cache.data:
        return credentials.cache.data

    # Extract values.
    principal_name = credentials.username
    properties = credentials.mechanism_properties

    # Validate that the address is allowed.
    if not properties.provider_name:
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

    # Get or create the cache data.
    credentials.cache.data = _OIDCAuthenticator(username=principal_name, properties=properties)
    return credentials.cache.data


class _OIDCAWSCallback(OIDCMachineCallback):
    def fetch(self, context: OIDCMachineCallbackContext) -> OIDCMachineCallbackResult:
        token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
        if not token_file:
            raise RuntimeError(
                'MONGODB-OIDC with an "aws" provider requires "AWS_WEB_IDENTITY_TOKEN_FILE" to be set'
            )
        with open(token_file) as fid:
            return OIDCMachineCallbackResult(access_token=fid.read().strip())


class _OIDCAzureCallback(OIDCMachineCallback):
    def __init__(self, token_audience: str, username: Optional[str]) -> None:
        self.token_audience = token_audience
        self.username = username

    def fetch(self, context: OIDCMachineCallbackContext) -> OIDCMachineCallbackResult:
        resp = _get_azure_response(self.token_audience, self.username, context.timeout_seconds)
        return OIDCMachineCallbackResult(
            access_token=resp["access_token"], expires_in_seconds=resp["expires_in"]
        )


@dataclass
class _OIDCAuthenticator:
    username: str
    properties: _OIDCProperties
    refresh_token: Optional[str] = field(default=None)
    access_token: Optional[str] = field(default=None)
    idp_info: Optional[OIDCIdPInfo] = field(default=None)
    access_token_validated: bool = field(default=False)
    token_gen_id: int = field(default=0)
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_call_time: float = field(default=0)

    def get_current_token(self, use_human_callback: bool = True) -> Optional[str]:
        properties = self.properties
        cb: Union[None, OIDCHumanCallback, OIDCMachineCallback]
        resp: Union[None, OIDCHumanCallbackResult, OIDCMachineCallbackResult]

        if properties.custom_token_callback:
            cb = properties.custom_token_callback
        elif not use_human_callback:
            cb = None
        elif properties.request_token_callback:
            cb = properties.request_token_callback

        prev_token = self.access_token
        if prev_token:
            return prev_token

        if cb is None and not prev_token:
            return None

        if not prev_token and cb is not None:
            with self.lock:
                # See if the token was changed while we were waiting for the
                # lock.
                new_token = self.access_token
                if new_token != prev_token:
                    return new_token

                # Ensure that we are waiting a min time between callback invocations.
                delta = time.time() - self.last_call_time
                if delta < TIME_BETWEEN_CALLS_SECONDS:
                    time.sleep(TIME_BETWEEN_CALLS_SECONDS - delta)
                self.last_call_time = time.time()

                if isinstance(cb, OIDCHumanCallback):
                    human_context = OIDCHumanCallbackContext(
                        timeout_seconds=HUMAN_CALLBACK_TIMEOUT_SECONDS,
                        version=HUMAN_CALLBACK_VERSION,
                        refresh_token=self.refresh_token,
                    )
                    assert self.idp_info is not None
                    resp = cb.fetch(self.idp_info, human_context)
                    if not isinstance(resp, OIDCHumanCallbackResult):
                        raise ValueError("Callback result must be of type OIDCHumanCallbackResult")
                    self.refresh_token = resp.refresh_token
                else:
                    machine_context = OIDCMachineCallbackContext(
                        timeout_seconds=remaining() or MACHINE_CALLBACK_TIMEOUT_SECONDS,
                        version=MACHINE_CALLBACK_VERSION,
                    )
                    resp = cb.fetch(machine_context)
                    if not isinstance(resp, OIDCMachineCallbackResult):
                        raise ValueError(
                            "Callback result must be of type OIDCMachineCallbackResult"
                        )
                self.access_token = resp.access_token
                self.access_token_validated = False
                self.token_gen_id += 1

        return self.access_token

    def principal_step_cmd(self) -> SON[str, Any]:
        """Get a SASL start command with an optional principal name"""
        # Send the SASL start with the optional principal name.
        payload = {}

        principal_name = self.username
        if principal_name:
            payload["n"] = principal_name

        return SON(
            [
                ("saslStart", 1),
                ("mechanism", "MONGODB-OIDC"),
                ("payload", Binary(bson.encode(payload))),
                ("autoAuthorize", 1),
            ]
        )

    def auth_start_cmd(self, use_human_callback: bool = True) -> Optional[SON[str, Any]]:
        if self.properties.request_token_callback is not None and self.idp_info is None:
            return self.principal_step_cmd()

        token = self.get_current_token(use_human_callback)
        if not token:
            return None
        bin_payload = Binary(bson.encode({"jwt": token}))
        return SON(
            [
                ("saslStart", 1),
                ("mechanism", "MONGODB-OIDC"),
                ("payload", bin_payload),
            ]
        )

    def run_command(
        self, conn: Connection, cmd: MutableMapping[str, Any]
    ) -> Optional[Mapping[str, Any]]:
        try:
            return conn.command("$external", cmd, no_reauth=True)  # type: ignore[call-arg]
        except OperationFailure:
            self.access_token = None
            raise

    def reauthenticate(self, conn: Connection) -> Optional[Mapping[str, Any]]:
        """Handle a reauthenticate from the server."""
        # First see if we have the a newer token on the authenticator.
        prev_id = conn.oidc_token_gen_id or 0
        # If we've already changed tokens, make one optimistic attempt.
        if (prev_id < self.token_gen_id) and self.access_token:
            try:
                return self.authenticate(conn)
            except OperationFailure:
                pass

        self.access_token = None
        self.access_token_validated = False

        # If we are using machine callbacks, clear the access token and
        # re-authenticate.
        if self.properties.provider_name:
            return self.authenticate(conn)

        # Next see if the idp info has changed.
        prev_idp_info = self.idp_info
        self.idp_info = None
        cmd = self.principal_step_cmd()
        resp = self.run_command(conn, cmd)
        assert resp is not None
        server_resp: dict = bson.decode(resp["payload"])
        if "issuer" in server_resp:
            self.idp_info = OIDCIdPInfo(**server_resp)

        # Handle the case of changed idp info.
        if self.idp_info != prev_idp_info:
            self.access_token = None
            self.access_token_validated = False
            self.refresh_token = None

        # If we have a refresh token, try using that.
        if self.refresh_token:
            try:
                return self.finish_auth(resp, conn)
            except OperationFailure:
                self.refresh_token = None
                # If that fails, try again without the refresh token.
                return self.authenticate(conn)

        # If we don't have a refresh token, just try once.
        return self.finish_auth(resp, conn)

    def authenticate(self, conn: Connection) -> Optional[Mapping[str, Any]]:
        ctx = conn.auth_ctx
        cmd = None

        if ctx and ctx.speculate_succeeded():
            resp = ctx.speculative_authenticate
        else:
            cmd = self.auth_start_cmd()
            assert cmd is not None
            resp = self.run_command(conn, cmd)

        assert resp is not None
        if resp["done"]:
            self.access_token_validated = True
            conn.oidc_token_gen_id = self.token_gen_id
            return None

        server_resp: dict = bson.decode(resp["payload"])
        if "issuer" in server_resp:
            self.idp_info = OIDCIdPInfo(**server_resp)

        return self.finish_auth(resp, conn)

    def finish_auth(
        self, orig_resp: Mapping[str, Any], conn: Connection
    ) -> Optional[Mapping[str, Any]]:
        conversation_id = orig_resp["conversationId"]
        token = self.get_current_token()
        conn.oidc_token_gen_id = self.token_gen_id
        bin_payload = Binary(bson.encode({"jwt": token}))
        cmd = SON(
            [
                ("saslContinue", 1),
                ("conversationId", conversation_id),
                ("payload", bin_payload),
            ]
        )
        resp = self.run_command(conn, cmd)
        assert resp is not None
        if not resp["done"]:
            raise OperationFailure("SASL conversation failed to complete.")
        self.access_token_validated = True
        return resp


def _authenticate_oidc(
    credentials: MongoCredential, conn: Connection, reauthenticate: bool
) -> Optional[Mapping[str, Any]]:
    """Authenticate using MONGODB-OIDC."""
    authenticator = _get_authenticator(credentials, conn.address)
    if reauthenticate:
        return authenticator.reauthenticate(conn)
    else:
        try:
            had_cache = authenticator.access_token_validated
            return authenticator.authenticate(conn)
        except Exception as e:
            # Try one more time an an authentication failure and had used a cached value.
            if isinstance(e, OperationFailure) and e.code == 18 and had_cache:
                return authenticator.authenticate(conn)
            raise

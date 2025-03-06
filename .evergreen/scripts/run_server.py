from __future__ import annotations

import os
from typing import Any

from utils import DRIVERS_TOOLS, ROOT, get_test_options, run_command


def set_env(name: str, value: Any = "1") -> None:
    os.environ[name] = str(value)


def start_server():
    opts = get_test_options()
    test_name = opts.test_name

    if test_name == "auth_aws":
        set_env("AUTH_AWS")

    elif test_name == "load_balancer":
        set_env("LOAD_BALANCER")

    if not os.environ.get("TEST_CRYPT_SHARED"):
        set_env("SKIP_CRYPT_SHARED")

    if opts.ssl:
        certs = ROOT / "test/certificates"
        set_env("TLS_CERT_KEY_FILE", certs / "client.pem")
        set_env("TLS_PEM_KEY_FILE", certs / "server.pem")
        set_env("TLS_CA_FILE", certs / "ca.pem")

    cmd = f"{DRIVERS_TOOLS.as_posix()}/.evergreen/run-orchestration.sh"
    run_command(cmd, cwd=DRIVERS_TOOLS)


if __name__ == "__main__":
    start_server()

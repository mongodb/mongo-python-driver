from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from utils import DRIVERS_TOOLS, ROOT, get_test_options, run_command


def set_env(name: str, value: Any = "1") -> None:
    os.environ[name] = str(value)


def start_server():
    run_mongodb_script = (
        Path(DRIVERS_TOOLS) / ".evergreen" / "run-mongodb.sh" if DRIVERS_TOOLS else None
    )
    want_help = bool({"-h", "--help"} & set(sys.argv[1:]))
    if want_help and run_mongodb_script and run_mongodb_script.is_file():
        # Forward straight to run-mongodb.sh's own help, without run_command's
        # "Running command..." logging noise.
        subprocess.run(  # noqa: S603
            ["bash", str(run_mongodb_script), "start", "-h"],  # noqa: S607
            cwd=DRIVERS_TOOLS,
            check=True,
        )
        return

    # DRIVERS_TOOLS is only needed to actually start a server. When it's unset and
    # -h/--help was requested, fall through to get_test_options' own argparse help below.
    if not want_help and not DRIVERS_TOOLS:
        raise ValueError(
            "DRIVERS_TOOLS is not set; run `just run-server` from an Evergreen task "
            "or set DRIVERS_TOOLS to a drivers-evergreen-tools checkout."
        )

    opts, extra_opts = get_test_options(
        "Run a MongoDB server.  All given flags will be passed to run-mongodb.sh in DRIVERS_TOOLS.",
        require_sub_test_name=False,
        allow_extra_opts=True,
    )
    test_name = opts.test_name

    # drivers-evergreen-tools expects the version variable to be named MONGODB_VERSION.
    if "VERSION" in os.environ:
        os.environ["MONGODB_VERSION"] = os.environ["VERSION"]

    if test_name == "auth_aws":
        set_env("AUTH_AWS")

    elif test_name == "load_balancer":
        set_env("LOAD_BALANCER")

    elif test_name == "search_index":
        os.environ["TOPOLOGY"] = "replica_set"
        os.environ["MONGODB_VERSION"] = "7.0"

    if not os.environ.get("TEST_CRYPT_SHARED"):
        set_env("SKIP_CRYPT_SHARED")

    if opts.ssl:
        extra_opts.append("--ssl")
        if test_name != "ocsp":
            certs = ROOT / "test/certificates"
            set_env("TLS_CERT_KEY_FILE", certs / "client.pem")
            set_env("TLS_PEM_KEY_FILE", certs / "server.pem")
            set_env("TLS_CA_FILE", certs / "ca.pem")

    if opts.auth:
        extra_opts.append("--auth")

    if opts.verbose:
        extra_opts.append("-v")
    elif opts.quiet:
        extra_opts.append("-q")

    cmd = ["bash", f"{DRIVERS_TOOLS}/.evergreen/run-mongodb.sh", "start", *extra_opts]
    run_command(cmd, cwd=DRIVERS_TOOLS)


if __name__ == "__main__":
    start_server()

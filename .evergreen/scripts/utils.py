from __future__ import annotations

import argparse
import dataclasses
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).absolute().parent
ROOT = HERE.parent.parent
DRIVERS_TOOLS = os.environ.get("DRIVERS_TOOLS", "").replace(os.sep, "/")

LOGGER = logging.getLogger("test")
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
ENV_FILE = HERE / "test-env.sh"
PLATFORM = "windows" if os.name == "nt" else sys.platform.lower()


@dataclasses.dataclass
class Distro:
    name: str
    version_id: str
    arch: str


# Map the test name to a test suite.
TEST_SUITE_MAP = {
    "atlas": "atlas",
    "auth_aws": "auth_aws",
    "auth_oidc": "auth_oidc",
    "data_lake": "data_lake",
    "default": "",
    "default_async": "default_async",
    "default_sync": "default",
    "encryption": "encryption",
    "enterprise_auth": "auth",
    "index_management": "index_management",
    "kms": "kms",
    "load_balancer": "load_balancer",
    "mockupdb": "mockupdb",
    "pyopenssl": "",
    "ocsp": "ocsp",
    "perf": "perf",
    "serverless": "",
}

# Tests that require a sub test suite.
SUB_TEST_REQUIRED = ["auth_aws", "kms"]


def get_test_options():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "test_name",
        choices=sorted(TEST_SUITE_MAP),
        nargs="?",
        default="default",
        help="The name of the test suite to set up, typically the same name as a pytest marker.",
    )
    parser.add_argument("sub_test_name", nargs="?", help="The sub test name, for example 'azure'")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Whether to log at the DEBUG level"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Whether to log at the WARNING level"
    )
    parser.add_argument("--auth", action="store_true", help="Whether to add authentication")
    parser.add_argument("--ssl", action="store_true", help="Whether to add TLS configuration")
    # Get the options.
    opts = parser.parse_args()
    if opts.verbose:
        LOGGER.setLevel(logging.DEBUG)
    elif opts.quiet:
        LOGGER.setLevel(logging.WARNING)

    # Handle validation and environment variable overrides.
    test_name = opts.test_name
    sub_test_name = opts.sub_test_name
    if test_name in SUB_TEST_REQUIRED and not sub_test_name:
        raise ValueError(f"Test '{test_name}' requires a sub_test_name")
    if "auth" in test_name or os.environ.get("AUTH") == "auth":
        opts.auth = True
        # 'auth_aws ecs' shouldn't have extra auth set.
        if test_name == "auth_aws" and sub_test_name == "ecs":
            opts.auth = False
    if os.environ.get("SSL") == "ssl":
        opts.ssl = True
    return opts


def read_env(path: Path | str) -> dict[str, Any]:
    config = dict()
    with Path(path).open() as fid:
        for line in fid.readlines():
            if "=" not in line:
                continue
            name, _, value = line.strip().partition("=")
            if value.startswith(('"', "'")):
                value = value[1:-1]
            name = name.replace("export ", "")
            config[name] = value
    return config


def write_env(name: str, value: Any = "1") -> None:
    with ENV_FILE.open("a", newline="\n") as fid:
        # Remove any existing quote chars.
        value = str(value).replace('"', "")
        fid.write(f'export {name}="{value}"\n')


def run_command(cmd: str, **kwargs: Any) -> None:
    LOGGER.info("Running command %s...", cmd)
    kwargs.setdefault("check", True)
    subprocess.run(shlex.split(cmd), **kwargs)  # noqa: PLW1510, S603
    LOGGER.info("Running command %s... done.", cmd)

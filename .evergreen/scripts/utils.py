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
TMP_DRIVER_FILE = "/tmp/mongo-python-driver.tgz"  # noqa: S108

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
    "atlas_connect": "atlas_connect",
    "auth_aws": "auth_aws",
    "auth_oidc": "auth_oidc",
    "default": "",
    "default_async": "default_async",
    "default_sync": "default",
    "encryption": "encryption",
    "enterprise_auth": "auth",
    "search_index": "search_index",
    "kms": "kms",
    "load_balancer": "load_balancer",
    "mockupdb": "mockupdb",
    "ocsp": "ocsp",
    "perf": "perf",
}

# Tests that require a sub test suite.
SUB_TEST_REQUIRED = ["auth_aws", "auth_oidc", "kms", "mod_wsgi", "perf"]

EXTRA_TESTS = ["mod_wsgi", "aws_lambda", "doctest"]

# Tests that do not use run-orchestration directly.
NO_RUN_ORCHESTRATION = [
    "auth_oidc",
    "atlas_connect",
    "aws_lambda",
    "mockupdb",
    "ocsp",
]

# Mapping of env variables to options
OPTION_TO_ENV_VAR = {"cov": "COVERAGE", "crypt_shared": "TEST_CRYPT_SHARED"}


def get_test_options(
    description, require_sub_test_name=True, allow_extra_opts=False
) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(
        description=description, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    if require_sub_test_name:
        parser.add_argument(
            "test_name",
            choices=sorted(list(TEST_SUITE_MAP) + EXTRA_TESTS),
            nargs="?",
            default="default",
            help="The optional name of the test suite to set up, typically the same name as a pytest marker.",
        )
        parser.add_argument(
            "sub_test_name", nargs="?", help="The optional sub test name, for example 'azure'."
        )
    else:
        parser.add_argument(
            "test_name",
            choices=set(list(TEST_SUITE_MAP) + EXTRA_TESTS) - set(NO_RUN_ORCHESTRATION),
            nargs="?",
            default="default",
            help="The optional name of the test suite to be run, which informs the server configuration.",
        )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Whether to log at the DEBUG level."
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Whether to log at the WARNING level."
    )
    parser.add_argument("--auth", action="store_true", help="Whether to add authentication.")
    parser.add_argument("--ssl", action="store_true", help="Whether to add TLS configuration.")
    parser.add_argument(
        "--test-min-deps", action="store_true", help="Test against minimum dependency versions"
    )

    # Add the test modifiers.
    if require_sub_test_name:
        parser.add_argument(
            "--debug-log", action="store_true", help="Enable pymongo standard logging."
        )
        parser.add_argument("--cov", action="store_true", help="Add test coverage.")
        parser.add_argument(
            "--green-framework",
            nargs=1,
            choices=["gevent"],
            help="Optional green framework to test against.",
        )
        parser.add_argument(
            "--compressor",
            nargs=1,
            choices=["zlib", "zstd", "snappy"],
            help="Optional compression algorithm.",
        )
        parser.add_argument("--crypt-shared", action="store_true", help="Test with crypt_shared.")
        parser.add_argument("--no-ext", action="store_true", help="Run without c extensions.")
        parser.add_argument(
            "--mongodb-api-version", choices=["1"], help="MongoDB stable API version to use."
        )
        parser.add_argument(
            "--disable-test-commands", action="store_true", help="Disable test commands."
        )

    # Get the options.
    if not allow_extra_opts:
        opts, extra_opts = parser.parse_args(), []
    else:
        opts, extra_opts = parser.parse_known_args()

    # Convert list inputs to strings.
    for name in vars(opts):
        value = getattr(opts, name)
        if isinstance(value, list):
            setattr(opts, name, value[0])

    # Handle validation and environment variable overrides.
    test_name = opts.test_name
    sub_test_name = opts.sub_test_name if require_sub_test_name else ""
    if require_sub_test_name and test_name in SUB_TEST_REQUIRED and not sub_test_name:
        raise ValueError(f"Test '{test_name}' requires a sub_test_name")
    handle_env_overrides(parser, opts)
    if "auth" in test_name:
        opts.auth = True
        # 'auth_aws ecs' shouldn't have extra auth set.
        if test_name == "auth_aws" and sub_test_name == "ecs":
            opts.auth = False
    if opts.verbose:
        LOGGER.setLevel(logging.DEBUG)
    elif opts.quiet:
        LOGGER.setLevel(logging.WARNING)
    return opts, extra_opts


def handle_env_overrides(parser: argparse.ArgumentParser, opts: argparse.Namespace) -> None:
    # Get the options, and then allow environment variable overrides.
    for key in vars(opts):
        if key in OPTION_TO_ENV_VAR:
            env_var = OPTION_TO_ENV_VAR[key]
        else:
            env_var = key.upper()
        if env_var in os.environ:
            if parser.get_default(key) != getattr(opts, key):
                LOGGER.info("Overriding env var '%s' with cli option", env_var)
            elif env_var == "AUTH":
                opts.auth = os.environ.get("AUTH") == "auth"
            elif env_var == "SSL":
                ssl_opt = os.environ.get("SSL", "")
                opts.ssl = ssl_opt and ssl_opt.lower() != "nossl"
            elif isinstance(getattr(opts, key), bool):
                if os.environ[env_var]:
                    setattr(opts, key, True)
            else:
                setattr(opts, key, os.environ[env_var])


def read_env(path: Path | str) -> dict[str, str]:
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


def run_command(cmd: str | list[str], **kwargs: Any) -> None:
    if isinstance(cmd, list):
        cmd = " ".join(cmd)
    LOGGER.info("Running command '%s'...", cmd)
    kwargs.setdefault("check", True)
    # Prevent overriding the python used by other tools.
    env = kwargs.pop("env", os.environ).copy()
    if "UV_PYTHON" in env:
        del env["UV_PYTHON"]
    kwargs["env"] = env
    try:
        subprocess.run(shlex.split(cmd), **kwargs)  # noqa: PLW1510, S603
    except subprocess.CalledProcessError as e:
        LOGGER.error(e.output)
        LOGGER.error(str(e))
        sys.exit(e.returncode)
    LOGGER.info("Running command '%s'... done.", cmd)


def create_archive() -> str:
    run_command("git add .", cwd=ROOT)
    run_command('git commit --no-verify -m "add files"', check=False, cwd=ROOT)
    run_command(f"git archive -o {TMP_DRIVER_FILE} HEAD", cwd=ROOT)
    return TMP_DRIVER_FILE

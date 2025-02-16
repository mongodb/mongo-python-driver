from __future__ import annotations

import argparse
import base64
import logging
import os
import platform
import shlex
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).absolute().parent
ROOT = HERE.parent.parent
ENV_FILE = HERE / "test-env.sh"
DRIVERS_TOOLS = os.environ.get("DRIVERS_TOOLS", "").replace(os.sep, "/")

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Passthrough environment variables.
PASS_THROUGH_ENV = [
    "GREEN_FRAMEWORK",
    "NO_EXT",
    "SETDEFAULTENCODING",
    "MONGODB_API_VERSION",
    "MONGODB_URI",
]

# Map the test name to a test suite.
TEST_SUITE_MAP = {
    "atlas": "atlas",
    "auth_aws": "auth_aws",
    "auth_oidc": "auth_oidc",
    "data_lake": "data_lake",
    "default": "default or default_async",
    "default_async": "default_async",
    "default_sync": "default",
    "encryption": "encryption",
    "enterprise_auth": "auth",
    "index_management": "index_management",
    "kms": "csfle",
    "load_balancer": "load_balancer",
    "mockupdb": "mockupdb",
    "ocsp": "ocsp",
    "perf": "perf",
    "serverless": "default or default_async",
}

# Map the test name to test extra.
EXTRAS_MAP = {
    "auth_aws": "aws",
    "auth_oidc": "aws",
    "encryption": "encryption",
    "enterprise_auth": "gssapi",
    "kms": "encryption",
    "ocsp": "ocsp",
    "pyopenssl": "ocsp",
}


# Map the test name to test group.
GROUP_MAP = dict(mockupdb="mockupdb", perf="perf")


def write_env(name: str, value: Any = "1") -> None:
    with ENV_FILE.open("a", newline="\n") as fid:
        # Remove any existing quote chars.
        value = str(value).replace('"', "")
        fid.write(f'export {name}="{value}"\n')


def is_set(var: str) -> bool:
    value = os.environ.get(var, "")
    return len(value.strip()) > 0


def run_command(cmd: str) -> None:
    LOGGER.info("Running command %s...", cmd)
    subprocess.check_call(shlex.split(cmd))  # noqa: S603
    LOGGER.info("Running command %s... done.", cmd)


def read_env(path: Path) -> dict[str, Any]:
    config = dict()
    with path.open() as fid:
        for line in fid.readlines():
            if "=" not in line:
                continue
            name, _, value = line.partition("=")
            if value.startswith(('"', "'")):
                value = value[1:-1]
            name = name.replace("export ", "")
            config[name] = value
    return config


def get_options():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("test_name", choices=sorted(TEST_SUITE_MAP), default="default")
    parser.add_argument("sub_test_name")
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
    return opts


def handle_test_env() -> None:
    opts = get_options()
    test_name = opts.test_name
    sub_test_name = opts.sub_test_name
    AUTH = os.environ.get("AUTH", "noauth")
    if opts.auth:
        AUTH = "auth"
    SSL = os.environ.get("SSL", "nossl")
    if opts.ssl or "auth" in test_name:
        AUTH = "ssl"
    TEST_ARGS = ""
    # Start compiling the args we'll pass to uv.
    # Run in an isolated environment so as not to pollute the base venv.
    UV_ARGS = ["--isolated --extra test"]

    # Create the test env file with the initial set of values.
    with ENV_FILE.open("w", newline="\n") as fid:
        fid.write("#!/usr/bin/env bash\n")
        fid.write("set +x\n")
    ENV_FILE.chmod(ENV_FILE.stat().st_mode | stat.S_IEXEC)

    write_env("AUTH", AUTH)
    write_env("SSL", SSL)
    # Skip CSOT tests on non-linux platforms.
    if sys.platform != "Linux":
        write_env("SKIP_CSOT_TESTS")
    # Set an environment variable for the test name.
    write_env(f"TEST_{test_name.upper()}")
    # Handle pass through env vars.
    for var in PASS_THROUGH_ENV:
        if is_set(var):
            write_env(var, os.environ[var])

    if extra := EXTRAS_MAP.get(test_name, ""):
        UV_ARGS.append(f"--extra {extra}")

    if group := GROUP_MAP.get(test_name, ""):
        UV_ARGS.append(f"--group {group}")

    if AUTH != "noauth":
        if test_name == "data_lake":
            config = read_env(f"{DRIVERS_TOOLS}/.evergreen/atlas_data_lake/secrets-export.sh")
            DB_USER = config("ADL_USERNAME")
            DB_PASSWORD = config("ADL_PASSWORD")
        elif test_name == "serverless":
            config = read_env(f"{DRIVERS_TOOLS}/.evergreen/serverless/secrets-export.sh")
            DB_USER = config("SERVERLESS_ATLAS_USER")
            DB_PASSWORD = config("SERVERLESS_ATLAS_PASSWORD")
            write_env("MONGODB_URI", config("SERVERLESS_URI"))
            write_env("SINGLE_MONGOS_LB_URI", config("SERVERLESS_URI"))
            write_env("MULTI_MONGOS_LB_URI", config("SERVERLESS_URI"))
        elif test_name == "auth_oidc":
            config = read_env(f"{DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh")
            DB_USER = config("OIDC_ADMIN_USER")
            DB_PASSWORD = config("OIDC_ADMIN_PWD")
            write_env("DB_IP", config("MONGODB_URI"))
        elif test_name == "index_management":
            config = read_env(f"{DRIVERS_TOOLS}/.evergreen/atlas/secrets-export.sh")
            DB_USER = config("DRIVERS_ATLAS_LAMBDA_USER")
            DB_PASSWORD = config("DRIVERS_ATLAS_LAMBDA_PASSWORD")
        else:
            DB_USER = "bob"
            DB_PASSWORD = "pwd123"  # noqa: S105
        write_env("DB_USER", DB_USER)
        write_env("DB_PASSWORD", DB_PASSWORD)
        LOGGER.info("Added auth, DB_USER: %s", DB_USER)

    if is_set("MONGODB_URI"):
        write_env("PYMONGO_MUST_CONNECT", "true")

    if is_set("DISABLE_TEST_COMMANDS"):
        write_env("PYMONGO_DISABLE_TEST_COMMANDS", "1")

    if test_name == "enterprise_auth":
        if os.name == "nt":
            LOGGER.info("Setting GSSAPI_PASS")
            write_env("GSSAPI_PASS", os.environ["SASL_PASS"])
            write_env("GSSAPI_CANONICALIZE", "true")
        else:
            # BUILD-3830
            krb_conf = ROOT / ".evergreen/krb5.conf.empty"
            krb_conf.touch()
            write_env("KRB5_CONFIG", krb_conf)
            LOGGER.info("Writing keytab")
            keytab = base64.b64decode(os.environ["KEYTAB_BASE64"])
            keytab_file = ROOT / ".evergreen/drivers.keytab"
            with keytab_file.open("wb") as fid:
                fid.write(keytab)
            principal = os.environ["PRINCIPAL"]
            LOGGER.info("Running kinit")
            os.environ["KRB5_CONFIG"] = str(krb_conf)
            cmd = f"kinit -k -t {keytab_file} -p {principal}"
            run_command(cmd)

        LOGGER.info("Setting GSSAPI variables")
        write_env("GSSAPI_HOST", os.environ["SASL_HOST"])
        write_env("GSSAPI_PORT", os.environ["SASL_PORT"])
        write_env("GSSAPI_PRINCIPAL", os.environ["PRINCIPAL"])

    if test_name == "load_balancer":
        SINGLE_MONGOS_LB_URI = os.environ.get(
            "SINGLE_MONGOS_LB_URI", "mongodb://127.0.0.1:8000/?loadBalanced=true"
        )
        MULTI_MONGOS_LB_URI = os.environ.get(
            "MULTI_MONGOS_LB_URI", "mongodb://127.0.0.1:8001/?loadBalanced=true"
        )
        if SSL != "nossl":
            SINGLE_MONGOS_LB_URI += "&tls=true"
            MULTI_MONGOS_LB_URI += "&tls=true"
        write_env("SINGLE_MONGOS_LB_URI", SINGLE_MONGOS_LB_URI)
        write_env("MULTI_MONGOS_LB_URI", MULTI_MONGOS_LB_URI)
        if not DRIVERS_TOOLS:
            raise RuntimeError("Missing DRIVERS_TOOLS")
        cmd = f'bash "{DRIVERS_TOOLS}/.evergreen/run-load-balancer.sh" start'
        run_command(cmd)

    if SSL != "nossl":
        if not DRIVERS_TOOLS:
            raise RuntimeError("Missing DRIVERS_TOOLS")
        write_env("CLIENT_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/client.pem")
        write_env("CA_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem")

    compressors = os.environ.get("COMPRESSORS")
    if compressors == "snappy":
        UV_ARGS.append("--extra snappy")
    elif compressors == "zstd":
        UV_ARGS.append("--extra zstandard")

    if test_name in ["encryption", "kms"]:
        # Check for libmongocrypt download.
        if not (ROOT / "libmongocrypt").exists():
            run_command(f"bash {HERE.as_posix()}/setup-libmongocrypt.sh")

        # TODO: Test with 'pip install pymongocrypt'
        UV_ARGS.append("--group pymongocrypt_source")

        # Use the nocrypto build to avoid dependency issues with older windows/python versions.
        BASE = ROOT / "libmongocrypt/nocrypto"
        if sys.platform == "linux":
            if (BASE / "lib/libmongocrypt.so").exists():
                PYMONGOCRYPT_LIB = BASE / "lib/libmongocrypt.so"
            else:
                PYMONGOCRYPT_LIB = BASE / "lib64/libmongocrypt.so"
        elif sys.platform == "darwin":
            PYMONGOCRYPT_LIB = BASE / "lib/libmongocrypt.dylib"
        else:
            PYMONGOCRYPT_LIB = BASE / "bin/mongocrypt.dll"
        if not PYMONGOCRYPT_LIB.exists():
            raise RuntimeError("Cannot find libmongocrypt shared object file")
        write_env("PYMONGOCRYPT_LIB", PYMONGOCRYPT_LIB.as_posix())
        # PATH is updated by configure-env.sh for access to mongocryptd.

    if test_name == "encryption":
        if not DRIVERS_TOOLS:
            raise RuntimeError("Missing DRIVERS_TOOLS")
        run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/setup-secrets.sh")
        run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/start-servers.sh")

        if sub_test_name == "pyopenssl":
            UV_ARGS.append("--extra ocsp")

    if is_set("TEST_CRYPT_SHARED"):
        config = read_env(f"{DRIVERS_TOOLS}/mo-expansion.sh")
        CRYPT_SHARED_DIR = Path(config["CRYPT_SHARED_LIB_PATH"]).parent.as_posix()
        LOGGER.info("Using crypt_shared_dir %s", CRYPT_SHARED_DIR)
        if os.name == "nt":
            write_env("PATH", f"{CRYPT_SHARED_DIR}:$PATH")
        else:
            write_env(
                "DYLD_FALLBACK_LIBRARY_PATH",
                f"{CRYPT_SHARED_DIR}:${{DYLD_FALLBACK_LIBRARY_PATH:-}}",
            )
            write_env("LD_LIBRARY_PATH", f"{CRYPT_SHARED_DIR}:${{LD_LIBRARY_PATH:-}}")

    if test_name == "kms":
        if sub_test_name.startswith("azure"):
            write_env("TEST_FLE_AZURE_AUTO")
        else:
            write_env("TEST_FLE_GCP_AUTO")

        write_env("SUCCESS", "fail" not in sub_test_name)
        MONGODB_URI = os.environ.get("MONGODB_URI", "")
        if "@" in MONGODB_URI:
            raise RuntimeError("MONGODB_URI unexpectedly contains user credentials in FLE test!")

    if test_name == "ocsp":
        write_env("CA_FILE", os.environ["CA_FILE"])
        write_env("OCSP_TLS_SHOULD_SUCCEED", os.environ["OCSP_TLS_SHOULD_SUCCEED"])

    if test_name == "perf":
        # PYTHON-4769 Run perf_test.py directly otherwise pytest's test collection negatively
        # affects the benchmark results.
        TEST_ARGS = f"test/performance/perf_test.py {TEST_ARGS}"

    # Add coverage if requested.
    # Only cover CPython. PyPy reports suspiciously low coverage.
    if is_set("COVERAGE") and platform.python_implementation() == "CPython":
        # Keep in sync with combine-coverage.sh.
        # coverage >=5 is needed for relative_files=true.
        UV_ARGS.append("--group coverage")
        TEST_ARGS = f"{TEST_ARGS} --cov"
        write_env("COVERAGE")

    if is_set("GREEN_FRAMEWORK"):
        framework = os.environ["GREEN_FRAMEWORK"]
        UV_ARGS.append(f"--group {framework}")

    else:
        # Use --capture=tee-sys so pytest prints test output inline:
        # https://docs.pytest.org/en/stable/how-to/capture-stdout-stderr.html
        TEST_ARGS = f"-v --capture=tee-sys --durations=5 {TEST_ARGS}"
        TEST_SUITES = TEST_SUITE_MAP[test_name]
        TEST_ARGS = f'-m "{TEST_SUITES}" {TEST_ARGS}'

    write_env("TEST_ARGS", TEST_ARGS)
    write_env("UV_ARGS", " ".join(UV_ARGS))


if __name__ == "__main__":
    handle_test_env()

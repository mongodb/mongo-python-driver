from __future__ import annotations

import base64
import logging
import os
import platform
import re
import shlex
import stat
import subprocess
import sys
from pathlib import Path
from pprint import pprint

HERE = Path(__file__).absolute().parent
ROOT = HERE.parent.parent
ENV_FILE = HERE / "test-env.sh"

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

EXPECTED_VARS = [
    "TEST_ENCRYPTION",
    "TEST_ENCRYPTION_PYOPENSSL",
    "TEST_CRYPT_SHARED",
    "TEST_PYOPENSSL",
    "TEST_LOAD_BALANCER",
    "TEST_SERVERLESS",
    "TEST_INDEX_MANAGEMENT",
    "TEST_ENTERPRISE_AUTH",
    "TEST_FLE_AZURE_AUTO",
    "TEST_FLE_GCP_AUTO",
    "TEST_LOADBALANCER",
    "TEST_DATA_LAKE",
    "TEST_ATLAS",
    "TEST_OCSP",
    "TEST_AUTH_AWS",
    "TEST_AUTH_OIDC",
    "COMPRESSORS",
    "MONGODB_URI",
    "PERF_TEST",
    "GREEN_FRAMEWORK",
    "PYTHON_BINARY",
]

# Handle the test suite based on the presence of env variables.
TEST_SUITE_MAP = dict(
    TEST_DATA_LAKE="data_lake",
    TEST_AUTH_OIDC="auth_oidc",
    TEST_INDEX_MANAGEMENT="index_management",
    TEST_ENTERPRISE_AUTH="auth",
    TEST_LOADBALANCER="load_balancer",
    TEST_ENCRYPTION="encryption",
    TEST_FLE_AZURE_AUTO="csfle",
    TEST_FLE_GCP_AUTO="csfle",
    TEST_ATLAS="atlas",
    TEST_OCSP="ocsp",
    TEST_AUTH_AWS="auth_aws",
    PERF_TEST="perf",
)

# Handle extras based on the presence of env variables.
EXTRAS_MAP = dict(
    TEST_AUTH_OIDC="aws",
    TEST_AUTH_AWS="aws",
    TEST_OCSP="ocsp",
    TEST_PYOPENSSL="ocsp",
    TEST_ENTERPRISE_AUTH="gssapi",
    TEST_ENCRYPTION="encryption",
    TEST_FLE_AZURE_AUTO="encryption",
    TEST_FLE_GCP_AUTO="encryption",
    TEST_ENCRYPTION_PYOPENSSL="ocsp",
)


def write_env(name, value):
    with ENV_FILE.open("a") as fid:
        # Remove any existing quote chars.
        value = value.replace('"', "")
        fid.write(f'export {name}="{value}"\n')


pprint(os.environ)  # noqa: T203


def source_env(path):
    with open(path) as fid:
        for line in fid:
            match = re.match("export (.*?)=(.+?)", line.strip())
            if not match:
                continue
            name, value = match.groups()
            print("Setting", name, value)  # noqa: T201
            print(line)  # noqa: T201
            os.environ[name] = value


def is_set(var):
    return os.environ.get(var)


def run_command(cmd):
    subprocess.check_call(shlex.split(cmd))  # noqa: S603


# Load variables.
if (ROOT / "secrets-export.sh").exists():
    source_env(ROOT / "secrets-export.sh")
if (HERE / "env.sh").exists():
    source_env(HERE / "env.sh")

# Set up default values.
DRIVERS_TOOLS = os.environ.get("DRIVERS_TOOLS").replace(os.sep, "/")
AUTH = os.environ.get("AUTH", "noauth")
SSL = os.environ.get("SSL", "nossl")
TEST_SUITES = os.environ.get("TEST_SUITES", "")
TEST_ARGS = " ".join(sys.argv[1:])
# Start compiling the args we'll pass to uv.
# Run in an isolated environment so as not to pollute the base venv.
UV_ARGS = ["--isolated --extra test"]

# Save variables in EXPECTED_VARS that have values.
with ENV_FILE.open("w") as fid:
    fid.write("set +x\n")
    fid.write(f"export AUTH={AUTH}\n")
    fid.write(f"export SSL={SSL}\n")
    for var in EXPECTED_VARS:
        value = os.environ.get(var, "")
        # Remove any existing quote chars.
        value = value.replace('"', "")
        if value:
            fid.write(f'export {var}="{value}"\n')
ENV_FILE.chmod(ENV_FILE.stat().st_mode | stat.S_IEXEC)

for env_var, extra in EXTRAS_MAP.items():
    if env_var in os.environ:
        UV_ARGS.append(f"--extra {extra}")

for env_var, suite in TEST_SUITE_MAP.items():
    if TEST_SUITES:
        continue
    if env_var in os.environ:
        TEST_SUITES = suite

if AUTH == "noauth":
    if is_set("TEST_DATA_LAKE"):
        DB_USER = "mhuser"
        DB_PASSWORD = "pencil"  # noqa: S105
    elif is_set("TEST_SERVERLESS"):
        source_env(DRIVERS_TOOLS / "/.evergreen/serverless/secrets-export.sh")
        DB_USER = os.environ("SERVERLESS_ATLAS_USER")
        DB_PASSWORD = os.environ("SERVERLESS_ATLAS_PASSWORD")
        write_env("MONGODB_URI", os.environ("SERVERLESS_URI"))
        write_env("SINGLE_MONGOS_LB_URI", os.environ("SERVERLESS_URI"))
        write_env("MULTI_MONGOS_LB_URI", os.environ("SERVERLESS_URI"))
    elif is_set("TEST_AUTH_OIDC"):
        DB_USER = os.environ["OIDC_ADMIN_USER"]
        DB_PASSWORD = os.environ["OIDC_ADMIN_PWD"]
        write_env("DB_IP", os.environ["MONGODB_URI"])
    elif is_set("TEST_INDEX_MANAGEMENT"):
        source_env(DRIVERS_TOOLS / ".evergreen/atlas/secrets-export.sh")
        DB_USER = os.environ["DRIVERS_ATLAS_LAMBDA_USER"]
        DB_PASSWORD = os.environ["DRIVERS_ATLAS_LAMBDA_PASSWORD"]
    else:
        DB_USER = "bob"
        DB_PASSWORD = "pwd123"  # noqa: S105
    write_env("DB_USER", DB_USER)
    write_env("DB_PASSWORD", DB_PASSWORD)
    LOGGER.info("Added auth, DB_USER: %s", DB_USER)


if is_set("MONGODB_STARTED"):
    write_env("PYMONGO_MUST_CONNECT", "true")

if is_set("DISABLE_TEST_COMMANDS"):
    write_env("PYMONGO_DISABLE_TEST_COMMANDS", "1")


if is_set("TEST_ENTERPRISE_AUTH"):
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
        with keytab_file.open("w") as fid:
            fid.write(keytab)
        principal = os.environ["PRINCIPAL"]
        LOGGER.info("Running kinit")
        cmd = f"kinit -k -t {keytab_file} -p {principal}"
        run_command(cmd)

    LOGGER.info("Setting GSSAPI variables")
    write_env("GSSAPI_HOST", os.environ["SASL_HOST"])
    write_env("GSSAPI_PORT", os.environ["SASL_PORT"])
    write_env("GSSAPI_PRINCIPAL", os.environ["PRINCIPAL"])


if is_set("TEST_LOADBALANCER"):
    write_env("LOAD_BALANCER", "1")
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
    cmd = f'bash "{DRIVERS_TOOLS}/.evergreen/run-load-balancer.sh" start'
    run_command(cmd)

if SSL != "nossl":
    write_env("CLIENT_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/client.pem")
    write_env("CA_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem")

compressors = os.environ.get("COMPRESSORS")
if compressors == "snappy":
    UV_ARGS.append("--extra snappy")
elif compressors == "zstd":
    UV_ARGS.append("--extra zstandard")


if is_set("TEST_ENCRYPTION") or is_set("TEST_FLE_AZURE_AUTO") or is_set("TEST_FLE_GCP_AUTO"):
    # Check for libmongocrypt download.
    if not (ROOT / "libmongocrypt").exists():
        run_command(f"bash {HERE}/setup-libmongocrypt.sh")

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
        # libmongocrypt's windows dll is not marked executable.
        PYMONGOCRYPT_LIB.chmod(PYMONGOCRYPT_LIB.stat().st_mode | stat.S_IEXEC)
    if not PYMONGOCRYPT_LIB.exists():
        raise RuntimeError("Cannot find libmongocrypt shared object file")
    write_env("PYMONGOCRYPT_LIB", PYMONGOCRYPT_LIB.as_posix())
    # PATH is updated by configure-env.sh for access to mongocryptd.

if is_set("TEST_ENCRYPTION"):
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/setup-secrets.sh")
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/start-servers.sh")


if is_set("TEST_CRYPT_SHARED"):
    CRYPT_SHARED_DIR = Path(os.environ["CRYPT_SHARED_LIB_PATH"]).parent()
    LOGGER.info("Using crypt_shared_dir %s", CRYPT_SHARED_DIR)
    DYLD_FALLBACK_LIBRARY_PATH = os.environ.get("DYLD_FALLBACK_LIBRARY_PATH", "")
    LD_LIBRARY_PATH = os.environ.get("LD_LIBRARY_PATH", "")
    PATH = os.environ("PATH")
    write_env("DYLD_FALLBACK_LIBRARY_PATH", f"{CRYPT_SHARED_DIR}:{DYLD_FALLBACK_LIBRARY_PATH}")
    write_env("LD_LIBRARY_PATH", f"{CRYPT_SHARED_DIR}:{LD_LIBRARY_PATH}")
    write_env("PATH", f"{CRYPT_SHARED_DIR}:{PATH}")


if is_set("TEST_FLE_AZURE_AUTO") or is_set("TEST_FLE_GCP_AUTO"):
    if "SUCCESS" not in os.environ:
        raise RuntimeError("Must define SUCCESS")

    write_env("SUCCESS", os.environ["SUCCESS"])
    MONGODB_URI = os.environ["MONGODB_URI"]
    if "@" in MONGODB_URI:
        raise RuntimeError("MONGODB_URI unexpectedly contains user credentials in FLE test!")


if is_set("PERF_TEST"):
    UV_ARGS.append("--group perf")
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


if is_set("GREEN_FRAMEWORK"):
    LOGGER.info("REALLY?: *%s*", os.environ["GREEN_FRAMEWORK"])
    framework = os.environ["GREEN_FRAMEWORK"]
    UV_ARGS.append(f"--group {framework}")

else:
    # Use --capture=tee-sys so pytest prints test output inline:
    # https://docs.pytest.org/en/stable/how-to/capture-stdout-stderr.html
    TEST_ARGS = f"-v --capture=tee-sys --durations=5 {TEST_ARGS}"
    if TEST_SUITES:
        TEST_ARGS = f"-m {TEST_SUITES} {TEST_ARGS}"

write_env("TEST_ARGS", TEST_ARGS)
write_env("UV_ARGS", " ".join(UV_ARGS))

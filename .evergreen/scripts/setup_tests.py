from __future__ import annotations

import base64
import io
import os
import platform
import shutil
import stat
import tarfile
from pathlib import Path
from urllib import request

from utils import (
    DRIVERS_TOOLS,
    ENV_FILE,
    HERE,
    LOGGER,
    PLATFORM,
    ROOT,
    TEST_SUITE_MAP,
    Distro,
    get_test_options,
    read_env,
    run_command,
    write_env,
)

# Passthrough environment variables.
PASS_THROUGH_ENV = ["GREEN_FRAMEWORK", "NO_EXT", "MONGODB_API_VERSION", "DEBUG_LOG"]

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


def is_set(var: str) -> bool:
    value = os.environ.get(var, "")
    return len(value.strip()) > 0


def get_distro() -> Distro:
    name = ""
    version_id = ""
    arch = platform.machine()
    with open("/etc/os-release") as fid:
        for line in fid.readlines():
            line = line.replace('"', "")  # noqa: PLW2901
            if line.startswith("NAME="):
                _, _, name = line.strip().partition("=")
            if line.startswith("VERSION_ID="):
                _, _, version_id = line.strip().partition("=")
    return Distro(name=name, version_id=version_id, arch=arch)


def setup_libmongocrypt():
    target = ""
    if PLATFORM == "windows":
        # PYTHON-2808 Ensure this machine has the CA cert for google KMS.
        if is_set("TEST_FLE_GCP_AUTO"):
            run_command('powershell.exe "Invoke-WebRequest -URI https://oauth2.googleapis.com/"')
        target = "windows-test"

    elif PLATFORM == "darwin":
        target = "macos"

    else:
        distro = get_distro()
        if distro.name.startswith("Debian"):
            target = f"debian{distro.version_id}"
        elif distro.name.startswith("Red Hat"):
            if distro.version_id.startswith("7"):
                target = "rhel-70-64-bit"
            elif distro.version_id.startswith("8"):
                if distro.arch == "aarch64":
                    target = "rhel-82-arm64"
                else:
                    target = "rhel-80-64-bit"

    if not is_set("LIBMONGOCRYPT_URL"):
        if not target:
            raise ValueError("Cannot find libmongocrypt target for current platform!")
        url = f"https://s3.amazonaws.com/mciuploads/libmongocrypt/{target}/master/latest/libmongocrypt.tar.gz"
    else:
        url = os.environ["LIBMONGOCRYPT_URL"]

    shutil.rmtree(HERE / "libmongocrypt", ignore_errors=True)

    LOGGER.info(f"Fetching {url}...")
    with request.urlopen(request.Request(url), timeout=15.0) as response:  # noqa: S310
        if response.status == 200:
            fileobj = io.BytesIO(response.read())
        with tarfile.open("libmongocrypt.tar.gz", fileobj=fileobj) as fid:
            fid.extractall(Path.cwd() / "libmongocrypt")
    LOGGER.info(f"Fetching {url}... done.")

    run_command("ls -la libmongocrypt")
    run_command("ls -la libmongocrypt/nocrypto")

    if PLATFORM == "windows":
        # libmongocrypt's windows dll is not marked executable.
        run_command("chmod +x libmongocrypt/nocrypto/bin/mongocrypt.dll")


def handle_test_env() -> None:
    opts, _ = get_test_options("Set up the test environment and services.")
    test_name = opts.test_name
    sub_test_name = opts.sub_test_name
    AUTH = "auth" if opts.auth else "noauth"
    SSL = "ssl" if opts.ssl else "nossl"
    TEST_ARGS = ""

    # Start compiling the args we'll pass to uv.
    # Run in an isolated environment so as not to pollute the base venv.
    UV_ARGS = ["--isolated --extra test"]

    test_title = test_name
    if sub_test_name:
        test_title += f" {sub_test_name}"
    LOGGER.info(f"Setting up '{test_title}' with {AUTH=} and {SSL=}...")

    # Create the test env file with the initial set of values.
    with ENV_FILE.open("w", newline="\n") as fid:
        fid.write("#!/usr/bin/env bash\n")
        fid.write("set +x\n")
    ENV_FILE.chmod(ENV_FILE.stat().st_mode | stat.S_IEXEC)

    write_env("AUTH", AUTH)
    write_env("SSL", SSL)
    write_env("PIP_QUIET")  # Quiet by default.
    write_env("PIP_PREFER_BINARY")  # Prefer binary dists by default.
    write_env("UV_FROZEN")  # Do not modify lock files.

    # Skip CSOT tests on non-linux platforms.
    if PLATFORM != "linux":
        write_env("SKIP_CSOT_TESTS")

    # Set an environment variable for the test name and sub test name.
    write_env(f"TEST_{test_name.upper()}")
    write_env("TEST_NAME", test_name)
    write_env("SUB_TEST_NAME", sub_test_name)

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
            DB_USER = config["ADL_USERNAME"]
            DB_PASSWORD = config["ADL_PASSWORD"]
        elif test_name == "serverless":
            config = read_env(f"{DRIVERS_TOOLS}/.evergreen/serverless/secrets-export.sh")
            DB_USER = config["SERVERLESS_ATLAS_USER"]
            DB_PASSWORD = config["SERVERLESS_ATLAS_PASSWORD"]
            write_env("MONGODB_URI", config["SERVERLESS_URI"])
            write_env("SINGLE_MONGOS_LB_URI", config["SERVERLESS_URI"])
            write_env("MULTI_MONGOS_LB_URI", config["SERVERLESS_URI"])
        elif test_name == "auth_oidc":
            DB_USER = os.environ["OIDC_ADMIN_USER"]
            DB_PASSWORD = os.environ["OIDC_ADMIN_PWD"]
            write_env("DB_IP", os.environ["MONGODB_URI"])
        elif test_name == "index_management":
            config = read_env(f"{DRIVERS_TOOLS}/.evergreen/atlas/secrets-export.sh")
            DB_USER = config["DRIVERS_ATLAS_LAMBDA_USER"]
            DB_PASSWORD = config["DRIVERS_ATLAS_LAMBDA_PASSWORD"]
            write_env("MONGODB_URI", config["MONGODB_URI"])
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
        config = read_env(f"{ROOT}/secrets-export.sh")
        if PLATFORM == "windows":
            LOGGER.info("Setting GSSAPI_PASS")
            write_env("GSSAPI_PASS", config["SASL_PASS"])
            write_env("GSSAPI_CANONICALIZE", "true")
        else:
            # BUILD-3830
            krb_conf = ROOT / ".evergreen/krb5.conf.empty"
            krb_conf.touch()
            write_env("KRB5_CONFIG", krb_conf)
            LOGGER.info("Writing keytab")
            keytab = base64.b64decode(config["KEYTAB_BASE64"])
            keytab_file = ROOT / ".evergreen/drivers.keytab"
            with keytab_file.open("wb") as fid:
                fid.write(keytab)
            principal = config["PRINCIPAL"]
            LOGGER.info("Running kinit")
            os.environ["KRB5_CONFIG"] = str(krb_conf)
            cmd = f"kinit -k -t {keytab_file} -p {principal}"
            run_command(cmd)

        LOGGER.info("Setting GSSAPI variables")
        write_env("GSSAPI_HOST", config["SASL_HOST"])
        write_env("GSSAPI_PORT", config["SASL_PORT"])
        write_env("GSSAPI_PRINCIPAL", config["PRINCIPAL"])

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

    if test_name == "ocsp":
        if sub_test_name:
            os.environ["OCSP_SERVER_TYPE"] = sub_test_name
        for name in ["OCSP_SERVER_TYPE", "ORCHESTRATION_FILE"]:
            if name not in os.environ:
                raise ValueError(f"Please set {name}")

        server_type = os.environ["OCSP_SERVER_TYPE"]
        ocsp_algo = os.environ["ORCHESTRATION_FILE"].split("-")[0]
        if server_type == "no-reponder":
            should_succeed = "mustStaple" not in ocsp_algo
        else:
            should_succeed = "true" if "valid" in server_type else "false"

        write_env("OCSP_TLS_SHOULD_SUCCEED", should_succeed)
        write_env("CA_FILE", f"{DRIVERS_TOOLS}/.evergreen/ocsp/{ocsp_algo}/ca.pem")

        if server_type != "no-responder":
            env = os.environ.copy()
            env["SERVER_TYPE"] = server_type
            env["OCSP_ALGORITHM"] = ocsp_algo
            run_command(f"bash {DRIVERS_TOOLS}/.evergreen/ocsp/setup.sh", env=env)

    if SSL != "nossl":
        if not DRIVERS_TOOLS:
            raise RuntimeError("Missing DRIVERS_TOOLS")
        write_env("CLIENT_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/client.pem")
        write_env("CA_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem")

    compressors = os.environ.get("COMPRESSORS")
    if compressors == "snappy":
        UV_ARGS.append("--extra snappy")
    elif compressors == "zstd":
        UV_ARGS.append("--extra zstd")

    if test_name in ["encryption", "kms"]:
        # Check for libmongocrypt download.
        if not (ROOT / "libmongocrypt").exists():
            setup_libmongocrypt()

        # TODO: Test with 'pip install pymongocrypt'
        UV_ARGS.append("--group pymongocrypt_source")

        # Use the nocrypto build to avoid dependency issues with older windows/python versions.
        BASE = ROOT / "libmongocrypt/nocrypto"
        if PLATFORM == "linux":
            if (BASE / "lib/libmongocrypt.so").exists():
                PYMONGOCRYPT_LIB = BASE / "lib/libmongocrypt.so"
            else:
                PYMONGOCRYPT_LIB = BASE / "lib64/libmongocrypt.so"
        elif PLATFORM == "darwin":
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
        if PLATFORM == "windows":
            write_env("PATH", f"{CRYPT_SHARED_DIR}:$PATH")
        else:
            write_env(
                "DYLD_FALLBACK_LIBRARY_PATH",
                f"{CRYPT_SHARED_DIR}:${{DYLD_FALLBACK_LIBRARY_PATH:-}}",
            )
            write_env("LD_LIBRARY_PATH", f"{CRYPT_SHARED_DIR}:${{LD_LIBRARY_PATH:-}}")

    if test_name == "kms":
        from kms_tester import setup_kms

        setup_kms(sub_test_name)

    if test_name == "auth_aws" and sub_test_name != "ecs-remote":
        auth_aws_dir = f"{DRIVERS_TOOLS}/.evergreen/auth_aws"
        if "AWS_ROLE_SESSION_NAME" in os.environ:
            write_env("AWS_ROLE_SESSION_NAME")
        if sub_test_name != "ecs":
            aws_setup = f"{auth_aws_dir}/aws_setup.sh"
            run_command(f"bash {aws_setup} {sub_test_name}")
            creds = read_env(f"{auth_aws_dir}/test-env.sh")
            for name, value in creds.items():
                write_env(name, value)
        else:
            run_command(f"bash {auth_aws_dir}/setup-secrets.sh")

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
        TEST_SUITE = TEST_SUITE_MAP[test_name]
        if TEST_SUITE:
            TEST_ARGS = f"-m {TEST_SUITE} {TEST_ARGS}"

    write_env("TEST_ARGS", TEST_ARGS)
    write_env("UV_ARGS", " ".join(UV_ARGS))

    LOGGER.info(f"Setting up test '{test_title}' with {AUTH=} and {SSL=}... done.")


if __name__ == "__main__":
    handle_test_env()

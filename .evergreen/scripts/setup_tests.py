from __future__ import annotations

import base64
import os
import platform
import shutil
import stat
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
PASS_THROUGH_ENV = [
    "GREEN_FRAMEWORK",
    "NO_EXT",
    "MONGODB_API_VERSION",
    "DEBUG_LOG",
    "UV_PYTHON",
    "REQUIRE_FIPS",
    "IS_WIN32",
]

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

# The python version used for perf tests.
PERF_PYTHON_VERSION = "3.10.11"


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
        elif distro.name.startswith("Ubuntu"):
            if distro.version_id == "20.04":
                target = "debian11"
            elif distro.version_id == "22.04":
                target = "debian12"
            elif distro.version_id == "24.04":
                target = "debian13"
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
            with Path("libmongocrypt.tar.gz").open("wb") as f:
                f.write(response.read())
            Path("libmongocrypt").mkdir()
            run_command("tar -xzf libmongocrypt.tar.gz -C libmongocrypt")
    LOGGER.info(f"Fetching {url}... done.")

    run_command("ls -la libmongocrypt")
    run_command("ls -la libmongocrypt/nocrypto")

    if PLATFORM == "windows":
        # libmongocrypt's windows dll is not marked executable.
        run_command("chmod +x libmongocrypt/nocrypto/bin/mongocrypt.dll")


def load_config_from_file(path: str | Path) -> dict[str, str]:
    config = read_env(path)
    for key, value in config.items():
        write_env(key, value)
    return config


def get_secrets(name: str) -> dict[str, str]:
    secrets_dir = Path(f"{DRIVERS_TOOLS}/.evergreen/secrets_handling")
    run_command(f"bash {secrets_dir.as_posix()}/setup-secrets.sh {name}", cwd=secrets_dir)
    return load_config_from_file(secrets_dir / "secrets-export.sh")


def handle_test_env() -> None:
    opts, _ = get_test_options("Set up the test environment and services.")
    test_name = opts.test_name
    sub_test_name = opts.sub_test_name
    AUTH = "auth" if opts.auth else "noauth"
    SSL = "ssl" if opts.ssl else "nossl"
    TEST_ARGS = ""

    # Start compiling the args we'll pass to uv.
    UV_ARGS = ["--extra test --no-group dev"]

    test_title = test_name
    if sub_test_name:
        test_title += f" {sub_test_name}"

    # Create the test env file with the initial set of values.
    with ENV_FILE.open("w", newline="\n") as fid:
        fid.write("#!/usr/bin/env bash\n")
        fid.write("set +x\n")
    ENV_FILE.chmod(ENV_FILE.stat().st_mode | stat.S_IEXEC)

    write_env("PIP_QUIET")  # Quiet by default.
    write_env("PIP_PREFER_BINARY")  # Prefer binary dists by default.

    # Set an environment variable for the test name and sub test name.
    write_env(f"TEST_{test_name.upper()}")
    write_env("TEST_NAME", test_name)
    write_env("SUB_TEST_NAME", sub_test_name)

    # Handle pass through env vars.
    for var in PASS_THROUGH_ENV:
        if is_set(var) or getattr(opts, var.lower(), ""):
            write_env(var, os.environ.get(var, getattr(opts, var.lower(), "")))

    if extra := EXTRAS_MAP.get(test_name, ""):
        UV_ARGS.append(f"--extra {extra}")

    if group := GROUP_MAP.get(test_name, ""):
        UV_ARGS.append(f"--group {group}")

    if opts.test_min_deps:
        UV_ARGS.append("--resolution=lowest-direct")

    if test_name == "auth_oidc":
        from oidc_tester import setup_oidc

        config = setup_oidc(sub_test_name)
        if not config:
            AUTH = "noauth"

    if test_name in ["aws_lambda", "search_index"]:
        env = os.environ.copy()
        env["MONGODB_VERSION"] = "7.0"
        env["LAMBDA_STACK_NAME"] = "dbx-python-lambda"
        write_env("LAMBDA_STACK_NAME", env["LAMBDA_STACK_NAME"])
        run_command(
            f"bash {DRIVERS_TOOLS}/.evergreen/atlas/setup-atlas-cluster.sh",
            env=env,
            cwd=DRIVERS_TOOLS,
        )

    if test_name == "search_index":
        AUTH = "auth"

    if test_name == "ocsp":
        SSL = "ssl"

    write_env("AUTH", AUTH)
    write_env("SSL", SSL)
    LOGGER.info(f"Setting up '{test_title}' with {AUTH=} and {SSL=}...")

    if test_name == "aws_lambda":
        UV_ARGS.append("--group pip")
        # Store AWS creds if they were given.
        if "AWS_ACCESS_KEY_ID" in os.environ:
            for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
                if key in os.environ:
                    write_env(key, os.environ[key])

    if AUTH != "noauth":
        if test_name == "auth_oidc":
            DB_USER = config["OIDC_ADMIN_USER"]
            DB_PASSWORD = config["OIDC_ADMIN_PWD"]
        elif test_name == "search_index":
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

    if opts.disable_test_commands:
        write_env("PYMONGO_DISABLE_TEST_COMMANDS", "1")

    if test_name == "enterprise_auth":
        config = get_secrets("drivers/enterprise_auth")
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

    if test_name == "doctest":
        UV_ARGS.append("--extra docs")

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

    if test_name == "mod_wsgi":
        from mod_wsgi_tester import setup_mod_wsgi

        setup_mod_wsgi(sub_test_name)

    if test_name == "ocsp":
        if sub_test_name:
            os.environ["OCSP_SERVER_TYPE"] = sub_test_name
        for name in ["OCSP_SERVER_TYPE", "ORCHESTRATION_FILE"]:
            if name not in os.environ:
                raise ValueError(f"Please set {name}")

        server_type = os.environ["OCSP_SERVER_TYPE"]
        orch_file = os.environ["ORCHESTRATION_FILE"]
        ocsp_algo = orch_file.split("-")[0]
        if server_type == "no-responder":
            tls_should_succeed = "false" if "mustStaple-disableStapling" in orch_file else "true"
        else:
            tls_should_succeed = "true" if "valid" in server_type else "false"

        write_env("OCSP_TLS_SHOULD_SUCCEED", tls_should_succeed)
        write_env("CA_FILE", f"{DRIVERS_TOOLS}/.evergreen/ocsp/{ocsp_algo}/ca.pem")

        if server_type != "no-responder":
            env = os.environ.copy()
            env["SERVER_TYPE"] = server_type
            env["OCSP_ALGORITHM"] = ocsp_algo
            run_command(f"bash {DRIVERS_TOOLS}/.evergreen/ocsp/setup.sh", env=env)

        # The mock OCSP responder MUST BE started before the mongod as the mongod expects that
        # a responder will be available upon startup.
        version = os.environ.get("VERSION", "latest")
        cmd = [
            "bash",
            f"{DRIVERS_TOOLS}/.evergreen/run-orchestration.sh",
            "--ssl",
            "--version",
            version,
        ]
        if opts.verbose:
            cmd.append("-v")
        elif opts.quiet:
            cmd.append("-q")
        run_command(cmd, cwd=DRIVERS_TOOLS)

    if SSL != "nossl":
        if not DRIVERS_TOOLS:
            raise RuntimeError("Missing DRIVERS_TOOLS")
        write_env("CLIENT_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/client.pem")
        write_env("CA_PEM", f"{DRIVERS_TOOLS}/.evergreen/x509gen/ca.pem")

    compressors = os.environ.get("COMPRESSORS") or opts.compressor
    if compressors == "snappy":
        UV_ARGS.append("--extra snappy")
    elif compressors == "zstd":
        UV_ARGS.append("--extra zstd")

    if test_name in ["encryption", "kms"]:
        # Check for libmongocrypt download.
        if not (ROOT / "libmongocrypt").exists():
            setup_libmongocrypt()

        if not opts.test_min_deps:
            UV_ARGS.append(
                "--with pymongocrypt@git+https://github.com/mongodb/libmongocrypt@master#subdirectory=bindings/python"
            )

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
        csfle_dir = Path(f"{DRIVERS_TOOLS}/.evergreen/csfle")
        run_command(f"bash {csfle_dir.as_posix()}/setup-secrets.sh", cwd=csfle_dir)
        load_config_from_file(csfle_dir / "secrets-export.sh")
        run_command(f"bash {csfle_dir.as_posix()}/start-servers.sh")

    if sub_test_name == "pyopenssl":
        UV_ARGS.append("--extra ocsp")

    if opts.crypt_shared:
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

    if test_name == "atlas_connect":
        secrets = get_secrets("drivers/atlas_connect")

        # Write file with Atlas X509 client certificate:
        decoded = base64.b64decode(secrets["ATLAS_X509_DEV_CERT_BASE64"]).decode("utf8")
        cert_file = ROOT / ".evergreen/atlas_x509_dev_client_certificate.pem"
        with cert_file.open("w") as file:
            file.write(decoded)
        write_env(
            "ATLAS_X509_DEV_WITH_CERT",
            secrets["ATLAS_X509_DEV"] + "&tlsCertificateKeyFile=" + str(cert_file),
        )

        # We do not want the default client_context to be initialized.
        write_env("DISABLE_CONTEXT")

    if test_name == "perf":
        data_dir = ROOT / "specifications/source/benchmarking/data"
        if not data_dir.exists():
            run_command("git clone --depth 1 https://github.com/mongodb/specifications.git")
            run_command("tar xf extended_bson.tgz", cwd=data_dir)
            run_command("tar xf parallel.tgz", cwd=data_dir)
            run_command("tar xf single_and_multi_document.tgz", cwd=data_dir)
        write_env("TEST_PATH", str(data_dir))
        write_env("OUTPUT_FILE", str(ROOT / "results.json"))
        # Overwrite the UV_PYTHON from the env.sh file.
        write_env("UV_PYTHON", "")

        UV_ARGS.append(f"--python={PERF_PYTHON_VERSION}")

        # PYTHON-4769 Run perf_test.py directly otherwise pytest's test collection negatively
        # affects the benchmark results.
        if sub_test_name == "sync":
            TEST_ARGS = f"test/performance/perf_test.py {TEST_ARGS}"
        else:
            TEST_ARGS = f"test/performance/async_perf_test.py {TEST_ARGS}"

    # Add coverage if requested.
    # Only cover CPython. PyPy reports suspiciously low coverage.
    if opts.cov and platform.python_implementation() == "CPython":
        # Keep in sync with combine-coverage.sh.
        # coverage >=5 is needed for relative_files=true.
        UV_ARGS.append("--group coverage")
        TEST_ARGS = f"{TEST_ARGS} --cov"
        write_env("COVERAGE")

    if opts.green_framework:
        framework = opts.green_framework or os.environ["GREEN_FRAMEWORK"]
        UV_ARGS.append(f"--group {framework}")

    else:
        TEST_ARGS = f"-v --durations=5 {TEST_ARGS}"
        TEST_SUITE = TEST_SUITE_MAP.get(test_name)
        if TEST_SUITE:
            TEST_ARGS = f"-m {TEST_SUITE} {TEST_ARGS}"

    write_env("TEST_ARGS", TEST_ARGS)
    write_env("UV_ARGS", " ".join(UV_ARGS))

    LOGGER.info(f"Setting up test '{test_title}' with {AUTH=} and {SSL=}... done.")


if __name__ == "__main__":
    handle_test_env()

from __future__ import annotations

import os

from utils import DRIVERS_TOOLS, TMP_DRIVER_FILE, create_archive, read_env, run_command, write_env

K8S_NAMES = ["aks", "gke", "eks"]
K8S_REMOTE_NAMES = [f"{n}-remote" for n in K8S_NAMES]


def _get_target_dir(sub_test_name: str) -> str:
    if sub_test_name == "test":
        target_dir = "auth_oidc"
    elif sub_test_name.startswith("azure"):
        target_dir = "auth_oidc/azure"
    elif sub_test_name.startswith("gcp"):
        target_dir = "auth_oidc/gcp"
    elif sub_test_name in K8S_NAMES + K8S_REMOTE_NAMES:
        target_dir = "auth_oidc/k8s"
    else:
        raise ValueError(f"Invalid sub test name '{sub_test_name}'")
    return f"{DRIVERS_TOOLS}/.evergreen/{target_dir}"


def setup_oidc(sub_test_name: str) -> dict[str, str] | None:
    target_dir = _get_target_dir(sub_test_name)
    env = os.environ.copy()
    if sub_test_name == "azure":
        env["AZUREOIDC_VMNAME_PREFIX"] = "PYTHON_DRIVER"
    if "-remote" not in sub_test_name:
        run_command(f"bash {target_dir}/setup.sh", env=env)
    if sub_test_name in K8S_NAMES:
        run_command(f"bash {target_dir}/setup-pod.sh {sub_test_name}")
        run_command(f"bash {target_dir}/run-self-test.sh")
        return None

    source_file = None
    if sub_test_name == "test":
        source_file = f"{target_dir}/secrets-export.sh"
    elif sub_test_name == "azure-remote":
        source_file = "./env.sh"
    elif sub_test_name == "gcp-remote":
        source_file = "./secrets-export.sh"
    if sub_test_name in K8S_REMOTE_NAMES:
        return os.environ.copy()
    if source_file is None:
        return None

    config = read_env(source_file)
    write_env("MONGODB_URI_SINGLE", config["MONGODB_URI_SINGLE"])
    write_env("MONGODB_URI", config["MONGODB_URI"])
    write_env("DB_IP", config["MONGODB_URI"])

    if sub_test_name == "test":
        write_env("OIDC_TOKEN_FILE", config["OIDC_TOKEN_FILE"])
        write_env("OIDC_TOKEN_DIR", config["OIDC_TOKEN_DIR"])
        if "OIDC_DOMAIN" in config:
            write_env("OIDC_DOMAIN", config["OIDC_DOMAIN"])
    elif sub_test_name == "azure-remote":
        write_env("AZUREOIDC_RESOURCE", config["AZUREOIDC_RESOURCE"])
    elif sub_test_name == "gcp-remote":
        write_env("GCPOIDC_AUDIENCE", config["GCPOIDC_AUDIENCE"])
    return config


def test_oidc_send_to_remote(sub_test_name: str) -> None:
    env = os.environ.copy()
    target_dir = _get_target_dir(sub_test_name)
    create_archive()
    if sub_test_name in ["azure", "gcp"]:
        upper_name = sub_test_name.upper()
        env[f"{upper_name}OIDC_DRIVERS_TAR_FILE"] = TMP_DRIVER_FILE
        env[
            f"{upper_name}OIDC_TEST_CMD"
        ] = f"OIDC_ENV={sub_test_name} ./.evergreen/run-mongodb-oidc-test.sh"
    elif sub_test_name in K8S_NAMES:
        env["K8S_DRIVERS_TAR_FILE"] = TMP_DRIVER_FILE
        env["K8S_TEST_CMD"] = "OIDC_ENV=k8s ./.evergreen/run-mongodb-oidc-test.sh"
    if sub_test_name == "eks" and "AWS_ACCESS_KEY_ID" in os.environ:
        # Remove AWS creds that would interfere with kubectl access.
        for key in ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]:
            if key in os.environ:
                del os.environ[key]
    run_command(f"bash {target_dir}/run-driver-test.sh", env=env)


def teardown_oidc(sub_test_name: str) -> None:
    target_dir = _get_target_dir(sub_test_name)
    if sub_test_name in K8S_NAMES:
        run_command(f"bash {target_dir}/teardown-pod.sh")
    run_command(f"bash {target_dir}/teardown.sh")

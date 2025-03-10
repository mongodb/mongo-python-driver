from __future__ import annotations

import os

from utils import (
    DRIVERS_TOOLS,
    TMP_DRIVER_FILE,
    run_command,
)

K8S_NAMES = ["aks", "gke", "eks"]


def _get_target_dir(sub_test_name: str) -> str:
    if sub_test_name == "test":
        target_dir = "auth_oidc"
    elif sub_test_name == "azure":
        target_dir = "auth_oidc/azure"
    elif sub_test_name == "gcp":
        target_dir = "auth_oidc/gcp"
    elif sub_test_name in K8S_NAMES:
        target_dir = "auth_oidc/k8s"
    else:
        raise ValueError(f"Invalid sub test name '{sub_test_name}'")
    return f"{DRIVERS_TOOLS}/.evergreen/{target_dir}"


def setup_oidc(sub_test_name: str) -> None:
    target_dir = _get_target_dir(sub_test_name)
    env = os.environ.copy()
    if sub_test_name == "azure":
        env["AZUREOIDC_VMNAME_PREFIX"] = "PYTHON_DRIVER"
    run_command(f"bash {target_dir}/setup.sh", env=env)
    if sub_test_name in K8S_NAMES:
        run_command(f"bash {target_dir}/setup-pod.sh")
        run_command(f"bash {target_dir}/run-self-test.sh")


def test_oidc_remote(sub_test_name: str) -> None:
    env = os.environ.copy()
    target_dir = _get_target_dir(sub_test_name)
    if sub_test_name in ["azure", "gcp"]:
        upper_name = sub_test_name.upper()
        env[f"{upper_name}OIDC_DRIVERS_TAR_FILE"] = TMP_DRIVER_FILE
        env[
            f"{upper_name}OIDC_TEST_CMD"
        ] = f"OIDC_ENV={sub_test_name} ./.evergreen/run-mongodb-oidc-test.sh"
    elif sub_test_name in K8S_NAMES:
        env["K8S_DRIVERS_TAR_FILE"] = TMP_DRIVER_FILE
        env["K8S_TEST_CMD"] = "OIDC_ENV=k8s ./.evergreen/run-mongodb-oidc-test.sh"

    run_command(f"bash {target_dir}/run-driver-test.sh")


def teardown_oidc(sub_test_name: str) -> None:
    target_dir = _get_target_dir(sub_test_name)
    if sub_test_name in K8S_NAMES:
        run_command(f"bash {target_dir}/teardown-pod.sh")
    run_command(f"bash {target_dir}/teardown.sh")

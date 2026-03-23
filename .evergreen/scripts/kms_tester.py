from __future__ import annotations

import os

from utils import (
    DRIVERS_TOOLS,
    LOGGER,
    TMP_DRIVER_FILE,
    create_archive,
    read_env,
    run_command,
    write_env,
)

DIRS = dict(
    gcp=f"{DRIVERS_TOOLS}/.evergreen/csfle/gcpkms",
    azure=f"{DRIVERS_TOOLS}/.evergreen/csfle/azurekms",
)


def _setup_azure_vm(base_env: dict[str, str]) -> None:
    LOGGER.info("Setting up Azure VM...")
    azure_dir = DIRS["azure"]
    env = base_env.copy()
    env["AZUREKMS_SRC"] = TMP_DRIVER_FILE
    env["AZUREKMS_DST"] = "~/"
    run_command(f"{azure_dir}/copy-file.sh", env=env)

    env = base_env.copy()
    env["AZUREKMS_CMD"] = "tar xf mongo-python-driver.tgz"
    run_command(f"{azure_dir}/run-command.sh", env=env)

    env["AZUREKMS_CMD"] = "sudo apt-get install -y python3-dev build-essential"
    run_command(f"{azure_dir}/run-command.sh", env=env)

    env["AZUREKMS_CMD"] = "bash .evergreen/just.sh setup-tests kms azure-remote"
    run_command(f"{azure_dir}/run-command.sh", env=env)
    LOGGER.info("Setting up Azure VM... done.")


def _setup_gcp_vm(base_env: dict[str, str]) -> None:
    LOGGER.info("Setting up GCP VM...")
    gcp_dir = DIRS["gcp"]
    env = base_env.copy()
    env["GCPKMS_SRC"] = TMP_DRIVER_FILE
    env["GCPKMS_DST"] = f"{env['GCPKMS_INSTANCENAME']}:"
    run_command(f"{gcp_dir}/copy-file.sh", env=env)

    env = base_env.copy()
    env["GCPKMS_CMD"] = "tar xf mongo-python-driver.tgz"
    run_command(f"{gcp_dir}/run-command.sh", env=env)

    env["GCPKMS_CMD"] = "sudo apt-get install -y python3-dev build-essential"
    run_command(f"{gcp_dir}/run-command.sh", env=env)

    env["GCPKMS_CMD"] = "bash ./.evergreen/just.sh setup-tests kms gcp-remote"
    run_command(f"{gcp_dir}/run-command.sh", env=env)
    LOGGER.info("Setting up GCP VM...")


def _load_kms_config(sub_test_target: str) -> dict[str, str]:
    target_dir = DIRS[sub_test_target]
    config = read_env(f"{target_dir}/secrets-export.sh")
    base_env = os.environ.copy()
    for key, value in config.items():
        base_env[key] = str(value)
    return base_env


def setup_kms(sub_test_name: str) -> None:
    if "-" in sub_test_name:
        sub_test_target, sub_test_type = sub_test_name.split("-")
    else:
        sub_test_target = sub_test_name
        sub_test_type = ""

    assert sub_test_target in ["azure", "gcp"], sub_test_target
    assert sub_test_type in ["", "remote", "fail"], sub_test_type
    success = sub_test_type != "fail"
    kms_dir = DIRS[sub_test_target]

    if sub_test_target == "azure":
        write_env("TEST_FLE_AZURE_AUTO")
    else:
        write_env("TEST_FLE_GCP_AUTO")

    write_env("SUCCESS", success)

    # For remote tests, there is no further work required.
    if sub_test_type == "remote":
        return

    if sub_test_target == "azure":
        run_command("./setup-secrets.sh", cwd=kms_dir)

    if success:
        create_archive()
        if sub_test_target == "azure":
            os.environ["AZUREKMS_VMNAME_PREFIX"] = "PYTHON_DRIVER"

            # Found using "az vm image list --output table"
            os.environ[
                "AZUREKMS_IMAGE"
            ] = "Canonical:0001-com-ubuntu-server-jammy:22_04-lts-gen2:latest"
        else:
            os.environ["GCPKMS_IMAGEFAMILY"] = "debian-12"

        run_command("./setup.sh", cwd=kms_dir)
        base_env = _load_kms_config(sub_test_target)

        if sub_test_target == "azure":
            _setup_azure_vm(base_env)
        else:
            _setup_gcp_vm(base_env)

    if sub_test_target == "azure":
        config = read_env(f"{kms_dir}/secrets-export.sh")
        if success:
            write_env("AZUREKMS_VMNAME", config["AZUREKMS_VMNAME"])

        write_env("KEY_NAME", config["AZUREKMS_KEYNAME"])
        write_env("KEY_VAULT_ENDPOINT", config["AZUREKMS_KEYVAULTENDPOINT"])


def test_kms_send_to_remote(sub_test_name: str) -> None:
    env = _load_kms_config(sub_test_name)
    if sub_test_name == "azure":
        key_name = os.environ["KEY_NAME"]
        key_vault_endpoint = os.environ["KEY_VAULT_ENDPOINT"]
        env[
            "AZUREKMS_CMD"
        ] = f'KEY_NAME="{key_name}" KEY_VAULT_ENDPOINT="{key_vault_endpoint}" bash ./.evergreen/just.sh run-tests'
    else:
        env["GCPKMS_CMD"] = "./.evergreen/just.sh run-tests"
    cmd = f"{DIRS[sub_test_name]}/run-command.sh"
    run_command(cmd, env=env)


def teardown_kms(sub_test_name: str) -> None:
    run_command(f"{DIRS[sub_test_name]}/teardown.sh")


if __name__ == "__main__":
    setup_kms()

from __future__ import annotations

import os

from utils import DRIVERS_TOOLS, LOGGER, ROOT, read_env, run_command, write_env

TMP_DRIVER_FILE = "/tmp/mongo-python-driver.tgz"  # noqa: S108
CSFLE_FOLDER = f"{DRIVERS_TOOLS}/.evergreen/csfle"


def setup_azure_vm() -> None:
    LOGGER.info("Setting up Azure VM...")
    cmd = f"""AZUREKMS_SRC="{TMP_DRIVER_FILE}" AZUREKMS_DST="~/" \
        {CSFLE_FOLDER}/azurekms/copy-file.sh"""
    run_command(cmd)
    cmd = """AZUREKMS_CMD="tar xf mongo-python-driver.tgz" \
        {CSFLE_FOLDER}/azurekms/run-command.sh"""
    run_command(cmd)
    cmd = f"""AZUREKMS_CMD="bash .evergreen/just.sh setup-test kms azure-remote" \
        {CSFLE_FOLDER}/azurekms/run-command.sh"""
    run_command(cmd)
    LOGGER.info("Setting up Azure VM... done.")


def setup_gcp_vm() -> None:
    LOGGER.info("Setting up GCP VM...")
    cmd = f"GCPKMS_SRC={TMP_DRIVER_FILE} GCPKMS_DST=$GCPKMS_INSTANCENAME: {CSFLE_FOLDER}/gcpkms/copy-file.sh"
    run_command(cmd)
    cmd = f'GCPKMS_CMD="tar xf mongo-python-driver.tgz" {CSFLE_FOLDER}/gcpkms/run-command.sh'
    run_command(cmd)
    cmd = f'GCPKMS_CMD="bash ./.evergreen/just.sh setup-test kms gcp-remote" {CSFLE_FOLDER}/gcpkms/run-command.sh'
    run_command(cmd)
    LOGGER.info("Setting up GCP VM...")


def create_archive():
    run_command("git add .", cwd=ROOT)
    run_command('git commit -m "add files"', check=False, cwd=ROOT)
    run_command(f"git archive -o {TMP_DRIVER_FILE} HEAD", cwd=ROOT)


def setup_kms(sub_test_name: str) -> None:
    if "-" in sub_test_name:
        sub_test_target, sub_test_type = sub_test_name.split("-")[0]
    else:
        sub_test_target = sub_test_name
        sub_test_type = ""

    assert sub_test_target in ["azure", "kms"]
    assert sub_test_type in ["", "remote", "fail"]
    success = sub_test_type != "fail"

    if sub_test_target == "azure":
        write_env("TEST_FLE_AZURE_AUTO")
    else:
        write_env("TEST_FLE_GCP_AUTO")

    write_env("SUCCESS", success)

    # For remote tests, there is no further work required.
    if sub_test_type == "remote":
        return

    if sub_test_target == "azure":
        os.environ["AZUREKMS_VMNAME_PREFIX"] = "PYTHON_DRIVER"

    run_command(f"{CSFLE_FOLDER}/{sub_test_target}kms/setup-secrets.sh")
    config = read_env(f"{CSFLE_FOLDER}/{sub_test_target}kms/secrets-export.sh")
    if success:
        run_command(f"{CSFLE_FOLDER}/{sub_test_target}kms/setup.sh")
        create_archive()

        if sub_test_target == "azure":
            setup_azure_vm(config)
        else:
            setup_gcp_vm(config)

    if sub_test_target == "azure":
        write_env("KEY_NAME", config["AZUREKMS_KEYNAME"])
        write_env("KEY_VAULT_ENDPOINT", config["AZUREKMS_KEYVAULTENDPOINT"])


def test_kms_vm(sub_test_name: str) -> None:
    if sub_test_name == "azure":
        key_name = os.environ["KEY_NAME"]
        key_vault_endpoint = os.environ["KEY_VAULT_ENDPOINT"]
        cmd = (
            f'AZUREKMS_CMD="KEY_NAME="{key_name}" KEY_VAULT_ENDPOINT="{key_vault_endpoint}" bash ./.evergreen/just.sh test-eg"'
            f"{CSFLE_FOLDER}/azurekms/run-command.sh"
        )
    else:
        cmd = f'GCPKMS_CMD="./.evergreen/just.sh test-eg" {CSFLE_FOLDER}/gcpkms/run-command.sh'
    run_command(cmd)


if __name__ == "__main__":
    setup_kms()

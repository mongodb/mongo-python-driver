from __future__ import annotations

import os

from utils import DRIVERS_TOOLS, LOGGER, ROOT, read_env, run_command, write_env

TMP_DRIVER_FILE = "/tmp/mongo-python-driver.tgz"  # noqa: S108


def setup_azurekms() -> None:
    LOGGER.info("Copying files to Azure VM...")
    cmd = f"""AZUREKMS_SRC="{TMP_DRIVER_FILE}" AZUREKMS_DST="~/" \
        {DRIVERS_TOOLS}/.evergreen/csfle/azurekms/copy-file.sh"""
    run_command(cmd)
    cmd = """AZUREKMS_CMD="tar xf mongo-python-driver.tgz" \
        {DRIVERS_TOOLS}/.evergreen/csfle/azurekms/run-command.sh"""
    run_command(cmd)
    LOGGER.info("Copying files to Azure VM... done.")


def setup_gcpkms() -> None:
    LOGGER.info("Copying files to GCP VM...")
    cmd = f"GCPKMS_SRC={TMP_DRIVER_FILE} GCPKMS_DST=$GCPKMS_INSTANCENAME: {DRIVERS_TOOLS}/.evergreen/csfle/gcpkms/copy-file.sh"
    run_command(cmd)
    cmd = f'GCPKMS_CMD="tar xf mongo-python-driver.tgz" {DRIVERS_TOOLS}/.evergreen/csfle/gcpkms/run-command.sh'
    run_command(cmd)
    LOGGER.info("Copying files to GCP VM...")


def create_archive():
    run_command("git add .", cwd=ROOT)
    run_command('git commit -m "add files"', check=False, cwd=ROOT)
    run_command(f"git archive -o {TMP_DRIVER_FILE} HEAD", cwd=ROOT)


def setup_kms(sub_test_name: str, success: bool) -> None:
    success = "fail" not in sub_test_name
    sub_test_type = sub_test_name.split()[0]
    if sub_test_name.startswith("azure"):
        write_env("TEST_FLE_AZURE_AUTO")
    else:
        write_env("TEST_FLE_GCP_AUTO")

    write_env("SUCCESS", success)

    if sub_test_type == "azure":
        os.environ["AZUREKMS_VMNAME_PREFIX"] = "PYTHON_DRIVER"

    run_command(f"{DRIVERS_TOOLS}/.evergreen/csfle/{sub_test_type}kms/setup-secrets.sh")
    config = read_env(f"{DRIVERS_TOOLS}/csfle/a{sub_test_type}kms/secrets-export.sh")
    if success:
        run_command(f"{DRIVERS_TOOLS}/.evergreen/csfle/{sub_test_type}kms/setup.sh")
        create_archive()

        if sub_test_name == "azure":
            mongodb_uri = setup_azurekms(config)
        else:
            mongodb_uri = setup_gcpkms(config)

    elif sub_test_type == "azure":
        write_env("KEY_NAME", config["AZUREKMS_KEYNAME"])
        write_env("KEY_VAULT_ENDPOINT", config["AZUREKMS_KEYVAULTENDPOINT"])

    if "@" in mongodb_uri:
        raise RuntimeError("MONGODB_URI unexpectedly contains user credentials in FLE test!")

    write_env("MONGODB_URI", mongodb_uri)


if __name__ == "__main__":
    setup_kms()

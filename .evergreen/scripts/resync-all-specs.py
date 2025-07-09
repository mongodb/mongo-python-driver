from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
from subprocess import CalledProcessError


def resync_specs(directory: pathlib.Path, errored: dict[str, str]) -> None:
    """Actually sync the specs"""
    for spec in os.scandir(directory):
        if not spec.is_dir():
            continue

        if spec.name in ["asynchronous"]:
            continue
        try:
            subprocess.run(
                ["bash", "./.evergreen/resync-specs.sh", spec.name],  # noqa: S603, S607
                capture_output=True,
                text=True,
                check=True,
            )
        except CalledProcessError as exc:
            errored[spec.name] = exc.stderr


def apply_patches():
    subprocess.run(["bash", "./.evergreen/remove-unimplemented-tests.sh"], check=True)  # noqa: S603, S607
    subprocess.run(["git apply -R --allow-empty ./.evergreen/patch/*"], shell=True, check=True)  # noqa: S602, S607


def check_new_spec_directories(directory: pathlib.Path) -> list[str]:
    """Check to see if there are any directories in the spec repo that don't exist in pymongo/test"""
    spec_dir = pathlib.Path(os.environ["MDB_SPECS"]) / "source"
    spec_set = {
        entry.name.replace("-", "_")
        for entry in os.scandir(spec_dir)
        if entry.is_dir()
        and (pathlib.Path(entry.path) / "tests").is_dir()
        and len(list(os.scandir(pathlib.Path(entry.path) / "tests"))) > 1
    }
    test_set = {entry.name.replace("-", "_") for entry in os.scandir(directory) if entry.is_dir()}
    known_mappings = {
        "ocsp_support": "ocsp",
        "client_side_operations_timeout": "csot",
        "mongodb_handshake": "handshake",
        "load_balancers": "load_balancer",
        "atlas_data_lake_testing": "atlas",
        "connection_monitoring_and_pooling": "connection_monitoring",
        "command_logging_and_monitoring": "command_logging",
        "initial_dns_seedlist_discovery": "srv_seedlist",
        "server_discovery_and_monitoring": "sdam_monitoring",
    }

    for k, v in known_mappings.items():
        if k in spec_set:
            spec_set.remove(k)
            spec_set.add(v)
    return list(spec_set - test_set)


def write_summary(errored: dict[str, str], new: list[str]) -> None:
    """Generate the PR description"""
    pr_body = ""
    process = subprocess.run(
        ["git diff --name-only | awk -F'/' '{print $2}' | sort | uniq"],  # noqa: S607
        shell=True,  # noqa: S602
        capture_output=True,
        text=True,
        check=True,
    )
    succeeded = [i for i in process.stdout.strip().split() if "data/mci/" not in i]
    if len(succeeded) > 0:
        pr_body += "The following specs were changed:\n -"
        pr_body += "\n -".join(succeeded)
        pr_body += "\n"
    if len(errored) > 0:
        pr_body += "\n\nThe following spec syncs encountered errors:\n -"
        for k, v in errored.items():
            pr_body += f"\n -{k}\n```{v}\n```"
        pr_body += "\n"
    if len(new) > 0:
        pr_body += "\n\nThe following directories are in the specification repository and not in our test directory:\n -"
        pr_body += "\n -".join(new)
        pr_body += "\n"
    if pr_body != "":
        with open("spec_sync.txt", "w") as f:
            # replacements made for proper json
            f.write(pr_body.replace("\n", "\\n").replace("\t", "\\t"))


def main():
    directory = pathlib.Path("./test")
    errored: dict[str, str] = {}
    resync_specs(directory, errored)
    apply_patches()
    new = check_new_spec_directories(directory)
    write_summary(errored, new)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Python Script to resync all specs and generate summary for PR."
    )
    parser.add_argument("filename", help="Name of file for the summary to be written into.")
    args = parser.parse_args()
    main()

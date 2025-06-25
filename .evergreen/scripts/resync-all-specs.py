import os
import pathlib
import subprocess
import argparse


def resync_specs(directory: pathlib.Path, succeeded: list[str], errored: dict[str, str]) -> None:
    for entry in os.scandir(directory):
        if not entry.is_dir():
            continue

        print(entry.path)
        spec_name = entry.path.split("/")[-1]
        if spec_name in ["asynchronous"]:
            continue
        process = subprocess.run(
            ["bash", "./.evergreen/resync-specs.sh", spec_name],
            capture_output=True,
            text=True)
        print(process.returncode)
        if process.returncode == 0:
            succeeded.append(spec_name)
        else:
            errored[spec_name] = process.stdout

def write_summary(succeeded: list[str], errored: dict[str, str]) -> None:
    pr_body = ""
    if len(succeeded) > 0:
        pr_body += "The following specs were changed:\n- "
        pr_body += "\n- ".join(succeeded)
        pr_body += "\n"
    if len(errored) > 0:
        pr_body += "\n\nThe following spec syncs encountered errors:"
        for k, v in errored.items():
            pr_body += f"\n- {k}\n```{v}\n```"

    if pr_body != "":
        with open("spec_sync.txt", "w") as f:
            # replacements made for to be json
            f.write(pr_body.replace("\n", "\\n").replace("\t", "\\t"))

def main():
    directory = pathlib.Path("./test")
    succeeded: list[str] = []
    errored: dict[str, str] = {}
    resync_specs(directory, succeeded, errored)
    write_summary(succeeded, errored)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Python Script to resync all specs and generate summary for PR.")
    parser.add_argument("filename", help="Name of file for the summary to be written into.")
    args = parser.parse_args()
    main()

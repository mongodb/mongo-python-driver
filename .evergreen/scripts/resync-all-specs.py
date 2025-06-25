import os
import pathlib
import subprocess

print("in resync all specs py")
directory = pathlib.Path("./test")
succeeded: list[str] = []
errored: dict[str, str] = {}
print("right before for loop?")
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
pr_body = ""
if len(succeeded) == 0 and len(errored.keys()) == 0:
    # no changes made and no errors
    pass
if len(succeeded) > 0:
    pr_body += "The following specs were changed:\n- "
    pr_body += "\n- ".join(succeeded)
    pr_body += "\n"
if len(errored) > 0:
    pr_body += "\n\nThe following spec syncs encountered errors:"
    for k, v in errored.items():
        # pr_body += f"\n- {k}\n{v}"
        pr_body += f"\n- {k}\n```{v}\n```"

# pr_body = pr_body.replace("\n", "\\n")  # for valid json
# pr_body = pr_body.replace("	", "   ")  # old whitespace is invalid json, new is literal spaces

with open("spec_sync.txt", "w") as f:
    # f.write(pr_body.replace("\n", "\\n").replace("	", "   "))
    f.write(pr_body.replace("\n", "\\n").replace("\t", "\\t"))
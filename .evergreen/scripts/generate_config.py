# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "shrub.py>=3.2.0",
#   "pyyaml>=6.0.2"
# ]
# ///

# Note: Run this file with `hatch run`, `pipx run`, or `uv run`.
from __future__ import annotations

from itertools import product

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_project import EvgProject
from shrub.v3.evg_task import EvgTaskRef
from shrub.v3.shrub_service import ShrubService

# Top level variables.
ALL_VERSIONS = ["4.0", "4.4", "5.0", "6.0", "7.0", "8.0", "rapid", "latest"]
CPYTHONS = ["py3.9", "py3.10", "py3.11", "py3.12", "py3.13"]
PYPYS = ["pypy3.9", "pypy3.10"]
ALL_PYTHONS = CPYTHONS + PYPYS
ALL_WIN_PYTHONS = CPYTHONS.copy()
ALL_WIN_PYTHONS = ALL_WIN_PYTHONS + [f"32-bit {p}" for p in ALL_WIN_PYTHONS]
AUTHS = ["noauth", "auth"]
SSLS = ["nossl", "ssl"]
AUTH_SSLS = list(product(AUTHS, SSLS))
TOPOLOGIES = ["standalone", "replica_set", "sharded_cluster"]
C_EXTS = ["without c extensions", "with c extensions"]
BATCHTIME_WEEK = 10080
HOSTS = dict(rhel8="rhel87-small", Win64="windows-64-vsMulti-small", macOS="macos-14")


# Helper functions.
def create_variant(task_names, display_name, *, python=None, host=None, **kwargs):
    task_refs = [EvgTaskRef(name=n) for n in task_names]
    kwargs.setdefault("expansions", dict())
    expansions = kwargs.pop("expansions")
    host = host or "rhel8"
    run_on = [HOSTS[host]]
    name = display_name.replace(" ", "-").lower()
    if python:
        expansions["PYTHON_BINARY"] = get_python_binary(python, host)
    expansions = expansions or None
    return BuildVariant(
        name=name,
        display_name=display_name,
        tasks=task_refs,
        expansions=expansions,
        run_on=run_on,
        **kwargs,
    )


def get_python_binary(python, host):
    if host.lower() == "win64":
        is_32 = python.startswith("32-bit")
        if is_32:
            _, python = python.split()
            base = "C:/python/32/"
        else:
            base = "C:/python/"
        middle = python.replace("py", "Python").replace(".", "")
        return base + middle + "/python.exe"

    if host.lower() == "rhel8":
        if python.startswith("pypy"):
            return f"/opt/python/{python}/bin/python3"
        return f"/opt/python/{python[2:]}/bin/python3"

    if host.lower() == "macos":
        ver = python.replace("py", "")
        return f"/Library/Frameworks/Python.Framework/Versions/{ver}/bin/python3"

    raise ValueError(f"no match found for {python} on {host}")


##############
# OCSP
##############

# Create OCSP build variants.
variants = []

# OCSP tests on rhel8 with rotating CPython versions.
for version in ALL_VERSIONS:
    task_refs = [EvgTaskRef(name=".ocsp")]
    expansions = dict(VERSION=version, AUTH="noauth", SSL="ssl", TOPOLOGY="server")
    batchtime = BATCHTIME_WEEK * 2
    python = ALL_PYTHONS[len(variants) % len(ALL_PYTHONS)]
    host = "rhel8"
    if version in ["rapid", "latest"]:
        display_name = f"OCSP test RHEL8 {version} {python}"
    else:
        display_name = f"OCSP test RHEL8 v{version} {python}"
    variant = create_variant(
        [".ocsp"],
        display_name,
        python=python,
        batchtime=batchtime,
        host=host,
        expansions=expansions,
    )
    variants.append(variant)

# OCSP tests on Windows and MacOS with lowest CPython version.
for host, version in product(["Win64", "macOS"], ["4.4", "8.0"]):
    # MongoDB servers do not staple OCSP responses and only support RSA.
    task_names = [".ocsp-rsa !.ocsp-staple"]
    expansions = dict(VERSION=version, AUTH="noauth", SSL="ssl", TOPOLOGY="server")
    batchtime = BATCHTIME_WEEK * 2
    python = CPYTHONS[0]
    display_name = f"OCSP test {host} v{version} {python}"
    variant = create_variant(
        task_names,
        display_name,
        python=python,
        host=host,
        expansions=expansions,
        batchtime=batchtime,
    )
    variants.append(variant)

# Generate OCSP config.
project = EvgProject(tasks=None, buildvariants=variants)
print(ShrubService.generate_yaml(project))  # noqa: T201

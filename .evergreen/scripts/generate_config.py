# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "shrub.py>=3.2.0",
#   "pyyaml>=6.0.2"
# ]
# ///

# Note: Run this file with `hatch run`, `pipx run`, or `uv run`.
from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle, product, zip_longest
from typing import Any

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_project import EvgProject
from shrub.v3.evg_task import EvgTaskRef
from shrub.v3.shrub_service import ShrubService

# Top level variables.
ALL_VERSIONS = ["4.0", "4.4", "5.0", "6.0", "7.0", "8.0", "rapid", "latest"]
CPYTHONS = ["3.9", "3.10", "3.11", "3.12", "3.13"]
PYPYS = ["pypy3.9", "pypy3.10"]
ALL_PYTHONS = CPYTHONS + PYPYS
BATCHTIME_WEEK = 10080
HOSTS = dict()


@dataclass
class Host:
    name: str
    run_on: str
    display_name: str


HOSTS["rhel8"] = Host("rhel8", "rhel87-small", "RHEL8")
HOSTS["win64"] = Host("win64", "windows-64-vsMulti-small", "Win64")
HOSTS["macos"] = Host("macos", "macos-14", "macOS")


# Helper functions.
def create_variant(
    task_names: list[str],
    display_name: str,
    *,
    python: str | None = None,
    host: str | None = None,
    **kwargs: Any,
) -> BuildVariant:
    """Create a build variant for the given inputs."""
    task_refs = [EvgTaskRef(name=n) for n in task_names]
    kwargs.setdefault("expansions", dict())
    expansions = kwargs.pop("expansions")
    host = host or "rhel8"
    run_on = [HOSTS[host].run_on]
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


def get_python_binary(python: str, host: str) -> str:
    """Get the appropriate python binary given a python version and host."""
    if host == "win64":
        is_32 = python.startswith("32-bit")
        if is_32:
            _, python = python.split()
            base = "C:/python/32"
        else:
            base = "C:/python"
        python = python.replace(".", "")
        return f"{base}/Python{python}/python.exe"

    if host == "rhel8":
        return f"/opt/python/{python}/bin/python3"

    if host == "macos":
        return f"/Library/Frameworks/Python.Framework/Versions/{python}/bin/python3"

    raise ValueError(f"no match found for python {python} on {host}")


def get_display_name(base: str, host: str, version: str, python: str) -> str:
    """Get the display name of a variant."""
    if version not in ["rapid", "latest"]:
        version = f"v{version}"
    if not python.startswith("pypy"):
        python = f"py{python}"
    return f"{base} {HOSTS[host].display_name} {version} {python}"


def zip_cycle(*iterables, empty_default=None):
    """Get all combinations of the inputs, cycling over the shorter list(s)."""
    cycles = [cycle(i) for i in iterables]
    for _ in zip_longest(*iterables):
        yield tuple(next(i, empty_default) for i in cycles)


##############
# OCSP
##############


# Create OCSP build variants.
def create_ocsp_variants() -> list[BuildVariant]:
    variants = []
    batchtime = BATCHTIME_WEEK * 2
    base_expansions = dict(AUTH="noauth", SSL="ssl", TOPOLOGY="server")
    base_display = "OCSP test"

    # OCSP tests on rhel8 with all server v4.4+ and python versions.
    versions = [v for v in ALL_VERSIONS if v != "4.0"]
    for version, python in zip_cycle(versions, ALL_PYTHONS):
        expansions = base_expansions.copy()
        expansions["VERSION"] = version
        host = "rhel8"
        variant = create_variant(
            [".ocsp"],
            get_display_name(base_display, host, version, python),
            python=python,
            batchtime=batchtime,
            host=host,
            expansions=expansions,
        )
        variants.append(variant)

    # OCSP tests on Windows and MacOS.
    # MongoDB servers on these hosts do not staple OCSP responses and only support RSA.
    for host, version in product(["win64", "macos"], ["4.4", "8.0"]):
        expansions = base_expansions.copy()
        expansions["VERSION"] = version
        if version == "4.4":
            python = CPYTHONS[0]
        else:
            python = CPYTHONS[-1]
        variant = create_variant(
            [".ocsp-rsa !.ocsp-staple"],
            get_display_name(base_display, host, version, python),
            python=python,
            host=host,
            expansions=expansions,
            batchtime=batchtime,
        )
        variants.append(variant)

    return variants


# Generate OCSP config.
project = EvgProject(tasks=None, buildvariants=create_ocsp_variants())
print(ShrubService.generate_yaml(project))  # noqa: T201

from __future__ import annotations

from dataclasses import dataclass
from inspect import getmembers, isfunction
from itertools import cycle, zip_longest
from pathlib import Path
from typing import Any

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_command import (
    EvgCommandType,
    ec2_assume_role,
    s3_put,
    subprocess_exec,
)
from shrub.v3.evg_project import EvgProject
from shrub.v3.evg_task import EvgTaskRef
from shrub.v3.shrub_service import ShrubService

##############
# Globals
##############

ALL_VERSIONS = ["4.2", "4.4", "5.0", "6.0", "7.0", "8.0", "rapid", "latest"]
CPYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14t", "3.14"]
PYPYS = ["pypy3.11"]
MIN_SUPPORT_VERSIONS = ["3.9", "pypy3.9", "pypy3.10"]
ALL_PYTHONS = CPYTHONS + PYPYS
MIN_MAX_PYTHON = [CPYTHONS[0], CPYTHONS[-1]]
BATCHTIME_WEEK = 10080
BATCHTIME_DAY = 1440
AUTH_SSLS = [("auth", "ssl"), ("noauth", "ssl"), ("noauth", "nossl")]
TOPOLOGIES = ["standalone", "replica_set", "sharded_cluster"]
C_EXTS = ["without_ext", "with_ext"]
SYNCS = ["sync", "async"]
DISPLAY_LOOKUP = dict(
    ssl=dict(ssl="SSL", nossl="NoSSL"),
    auth=dict(auth="Auth", noauth="NoAuth"),
    topology=dict(
        standalone="Standalone", replica_set="Replica Set", sharded_cluster="Sharded Cluster"
    ),
    test_suites=dict(default="Sync", default_async="Async"),
    sync={"sync": "Sync", "async": "Async"},
    coverage={"1": "cov"},
    no_ext={"1": "No C"},
    test_min_deps={"1": "Min Deps"},
)
HOSTS = dict()


@dataclass
class Host:
    name: str
    run_on: str
    display_name: str
    variables: dict[str, str] | None


# Hosts with toolchains.
HOSTS["rhel8"] = Host("rhel8", "rhel87-small", "RHEL8", dict())
HOSTS["win64"] = Host("win64", "windows-64-vsMulti-small", "Win64", dict())
HOSTS["win32"] = Host("win32", "windows-64-vsMulti-small", "Win32", dict())
HOSTS["macos"] = Host("macos", "macos-14", "macOS", dict())
HOSTS["macos-arm64"] = Host("macos-arm64", "macos-14-arm64", "macOS Arm64", dict())
HOSTS["ubuntu20"] = Host("ubuntu20", "ubuntu2004-small", "Ubuntu-20", dict())
HOSTS["ubuntu22"] = Host("ubuntu22", "ubuntu2204-small", "Ubuntu-22", dict())
HOSTS["perf"] = Host("perf", "rhel90-dbx-perf-large", "", dict())
HOSTS["debian11"] = Host("debian11", "debian11-small", "Debian11", dict())
DEFAULT_HOST = HOSTS["rhel8"]

# Other hosts
OTHER_HOSTS = ["RHEL9-FIPS", "RHEL8-zseries", "RHEL8-POWER8", "RHEL8-arm64", "Amazon2023"]
for name, run_on in zip(
    OTHER_HOSTS,
    [
        "rhel92-fips",
        "rhel8-zseries-small",
        "rhel8-power-small",
        "rhel82-arm64-small",
        "amazon2023-arm64-latest-large-m8g",
    ],
):
    HOSTS[name] = Host(name, run_on, name, dict())

##############
# Helpers
##############


def create_variant_generic(
    tasks: list[str | EvgTaskRef],
    display_name: str,
    *,
    host: Host | str | None = None,
    default_run_on="rhel87-small",
    expansions: dict | None = None,
    **kwargs: Any,
) -> BuildVariant:
    """Create a build variant for the given inputs."""
    task_refs = []
    if isinstance(host, str):
        host = HOSTS[host]
    for t in tasks:
        if isinstance(t, EvgTaskRef):
            task_refs.append(t)
        else:
            task_refs.append(EvgTaskRef(name=t))
    expansions = expansions and expansions.copy() or dict()
    if "run_on" in kwargs:
        run_on = kwargs.pop("run_on")
    elif host:
        run_on = [host.run_on]
        if host.variables:
            expansions.update(host.variables)
    else:
        run_on = [default_run_on]
    if isinstance(run_on, str):
        run_on = [run_on]
    name = display_name.replace(" ", "-").replace("*-", "").lower()
    return BuildVariant(
        name=name,
        display_name=display_name,
        tasks=task_refs,
        expansions=expansions or None,
        run_on=run_on,
        **kwargs,
    )


def create_variant(
    tasks: list[str | EvgTaskRef],
    display_name: str,
    *,
    version: str | None = None,
    host: Host | str | None = None,
    expansions: dict | None = None,
    **kwargs: Any,
) -> BuildVariant:
    expansions = expansions and expansions.copy() or dict()
    if version:
        expansions["VERSION"] = version
    return create_variant_generic(
        tasks, display_name, version=version, host=host, expansions=expansions, **kwargs
    )


def get_versions_from(min_version: str) -> list[str]:
    """Get all server versions starting from a minimum version."""
    min_version_float = float(min_version)
    rapid_latest = ["rapid", "latest"]
    versions = [v for v in ALL_VERSIONS if v not in rapid_latest]
    return [v for v in versions if float(v) >= min_version_float] + rapid_latest


def get_versions_until(max_version: str) -> list[str]:
    """Get all server version up to a max version."""
    max_version_float = float(max_version)
    versions = [v for v in ALL_VERSIONS if v not in ["rapid", "latest"]]
    versions = [v for v in versions if float(v) <= max_version_float]
    if not len(versions):
        raise ValueError(f"No server versions found less <= {max_version}")
    return versions


def get_common_name(base: str, sep: str, **kwargs) -> str:
    display_name = base
    version = kwargs.pop("VERSION", None)
    version = version or kwargs.pop("version", None)
    if version:
        if version not in ["rapid", "latest"]:
            version = f"v{version}"
        display_name = f"{display_name}{sep}{version}"
    for key, value in kwargs.items():
        name = value
        if key.lower() in ["python", "toolchain_version"]:
            if not value.startswith("pypy"):
                name = f"Python{value}"
            else:
                name = f"PyPy{value.replace('pypy', '')}"
        elif key.lower() in DISPLAY_LOOKUP and value in DISPLAY_LOOKUP[key.lower()]:
            name = DISPLAY_LOOKUP[key.lower()][value]
        else:
            continue
        display_name = f"{display_name}{sep}{name}"
    return display_name


def get_variant_name(base: str, host: str | Host | None = None, **kwargs) -> str:
    """Get the display name of a variant."""
    display_name = base
    if isinstance(host, str):
        host = HOSTS[host]
    if host is not None:
        display_name += f" {host.display_name}"
    return get_common_name(display_name, " ", **kwargs)


def get_task_name(base: str, **kwargs):
    return get_common_name(base, "-", **kwargs).replace(" ", "-").lower()


def zip_cycle(*iterables, empty_default=None):
    """Get all combinations of the inputs, cycling over the shorter list(s)."""
    cycles = [cycle(i) for i in iterables]
    for _ in zip_longest(*iterables):
        yield tuple(next(i, empty_default) for i in cycles)


def handle_c_ext(c_ext, expansions) -> None:
    """Handle c extension option."""
    if c_ext == C_EXTS[0]:
        expansions["NO_EXT"] = "1"


def get_standard_auth_ssl(topology):
    auth = "auth" if topology == "sharded_cluster" else "noauth"
    ssl = "nossl" if topology == "standalone" else "ssl"
    return auth, ssl


def get_assume_role(**kwargs):
    kwargs.setdefault("command_type", EvgCommandType.SETUP)
    kwargs.setdefault("role_arn", "${assume_role_arn}")
    return ec2_assume_role(**kwargs)


def get_subprocess_exec(**kwargs):
    kwargs.setdefault("binary", "bash")
    kwargs.setdefault("working_dir", "src")
    kwargs.setdefault("command_type", EvgCommandType.TEST)
    return subprocess_exec(**kwargs)


def get_s3_put(**kwargs):
    kwargs["aws_key"] = "${AWS_ACCESS_KEY_ID}"
    kwargs["aws_secret"] = "${AWS_SECRET_ACCESS_KEY}"  # noqa:S105
    kwargs["aws_session_token"] = "${AWS_SESSION_TOKEN}"  # noqa:S105
    kwargs["bucket"] = "${bucket_name}"
    kwargs.setdefault("optional", "true")
    kwargs.setdefault("permissions", "public-read")
    kwargs.setdefault("content_type", "${content_type|application/x-gzip}")
    kwargs.setdefault("command_type", EvgCommandType.SETUP)
    return s3_put(**kwargs)


def generate_yaml(tasks=None, variants=None):
    """Generate the yaml for a given set of tasks and variants."""
    project = EvgProject(tasks=tasks, buildvariants=variants)
    out = ShrubService.generate_yaml(project)
    # Dedent by two spaces to match what we use in config.yml
    lines = [line[2:] for line in out.splitlines()]
    print("\n".join(lines))


##################
# Generate Config
##################


def write_variants_to_file(mod):
    here = Path(__file__).absolute().parent
    target = here.parent / "generated_configs" / "variants.yml"
    if target.exists():
        target.unlink()
    with target.open("w") as fid:
        fid.write("buildvariants:\n")

    for name, func in sorted(getmembers(mod, isfunction)):
        if not name.endswith("_variants"):
            continue
        if not name.startswith("create_"):
            raise ValueError("Variant creators must start with create_")
        title = name.replace("create_", "").replace("_variants", "").replace("_", " ").capitalize()
        project = EvgProject(tasks=None, buildvariants=func())
        out = ShrubService.generate_yaml(project).splitlines()
        with target.open("a") as fid:
            fid.write(f"  # {title} tests\n")
            for line in out[1:]:
                fid.write(f"{line}\n")
            fid.write("\n")

    # Remove extra trailing newline:
    data = target.read_text().splitlines()
    with target.open("w") as fid:
        for line in data[:-1]:
            fid.write(f"{line}\n")


def write_tasks_to_file(mod):
    here = Path(__file__).absolute().parent
    target = here.parent / "generated_configs" / "tasks.yml"
    if target.exists():
        target.unlink()
    with target.open("w") as fid:
        fid.write("tasks:\n")

    for name, func in sorted(getmembers(mod, isfunction)):
        if name.startswith("_") or not name.endswith("_tasks"):
            continue
        if not name.startswith("create_"):
            raise ValueError("Task creators must start with create_")
        title = name.replace("create_", "").replace("_tasks", "").replace("_", " ").capitalize()
        project = EvgProject(tasks=func(), buildvariants=None)
        out = ShrubService.generate_yaml(project).splitlines()
        with target.open("a") as fid:
            fid.write(f"  # {title} tests\n")
            for line in out[1:]:
                fid.write(f"{line}\n")
            fid.write("\n")

    # Remove extra trailing newline:
    data = target.read_text().splitlines()
    with target.open("w") as fid:
        for line in data[:-1]:
            fid.write(f"{line}\n")


def write_functions_to_file(mod):
    here = Path(__file__).absolute().parent
    target = here.parent / "generated_configs" / "functions.yml"
    if target.exists():
        target.unlink()
    with target.open("w") as fid:
        fid.write("functions:\n")

    functions = dict()
    for name, func in sorted(getmembers(mod, isfunction)):
        if name.startswith("_") or not name.endswith("_func"):
            continue
        if not name.startswith("create_"):
            raise ValueError("Function creators must start with create_")
        title = name.replace("create_", "").replace("_func", "").replace("_", " ").capitalize()
        func_name, cmds = func()
        functions = dict()
        functions[func_name] = cmds
        project = EvgProject(functions=functions, tasks=None, buildvariants=None)
        out = ShrubService.generate_yaml(project).splitlines()
        with target.open("a") as fid:
            fid.write(f"  # {title}\n")
            for line in out[1:]:
                fid.write(f"{line}\n")
            fid.write("\n")

    # Remove extra trailing newline:
    data = target.read_text().splitlines()
    with target.open("w") as fid:
        for line in data[:-1]:
            fid.write(f"{line}\n")

from __future__ import annotations

import dataclasses
import logging
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).absolute().parent
ROOT = HERE.parent.parent
DRIVERS_TOOLS = os.environ.get("DRIVERS_TOOLS", "").replace(os.sep, "/")

LOGGER = logging.getLogger("test")
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")
ENV_FILE = HERE / "test-env.sh"
PLATFORM = "windows" if os.name == "nt" else sys.platform.lower()


@dataclasses.dataclass
class Distro:
    name: str
    version_id: str
    arch: str


def read_env(path: Path | str) -> dict[str, Any]:
    config = dict()
    with Path(path).open() as fid:
        for line in fid.readlines():
            if "=" not in line:
                continue
            name, _, value = line.strip().partition("=")
            if value.startswith(('"', "'")):
                value = value[1:-1]
            name = name.replace("export ", "")
            config[name] = value
    return config


def write_env(name: str, value: Any = "1") -> None:
    with ENV_FILE.open("a", newline="\n") as fid:
        # Remove any existing quote chars.
        value = str(value).replace('"', "")
        fid.write(f'export {name}="{value}"\n')


def run_command(cmd: str, **kwargs: Any) -> None:
    LOGGER.info("Running command %s...", cmd)
    kwargs.setdefault("check", True)
    subprocess.run(shlex.split(cmd), **kwargs)  # noqa: PLW1510, S603
    LOGGER.info("Running command %s... done.", cmd)

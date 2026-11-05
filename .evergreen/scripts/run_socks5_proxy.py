from __future__ import annotations

import argparse
import os
import runpy
import shlex
import signal
import subprocess
import sys
from pathlib import Path
from shutil import which
from urllib.parse import urlsplit

from utils import LOGGER


def _first_seed(uri: str) -> str:
    authority = urlsplit(uri).netloc.rsplit("@", 1)[-1]
    seed = authority.split(",", 1)[0]
    if seed.startswith("["):
        closing_bracket = seed.find("]")
        if closing_bracket < 0:
            raise ValueError(f"Invalid IPv6 seed in MONGODB_URI: {seed}")
        host = seed[1:closing_bracket]
        suffix = seed[closing_bracket + 1 :]
        if suffix and not suffix.startswith(":"):
            raise ValueError(f"Invalid seed in MONGODB_URI: {seed}")
        port = suffix[1:] if suffix else "27017"
    else:
        host, separator, port = seed.rpartition(":")
        if not separator:
            host, port = seed, "27017"
    if not host or not port.isdecimal():
        raise ValueError(f"Invalid seed in MONGODB_URI: {seed}")
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def stop_socks5_proxies(pids: list[int]) -> None:
    """Stop the proxy processes started for this checkout.

    :param pids: Process IDs recorded by the test setup.
    :raises ValueError: If a process ID is not positive.
    :raises RuntimeError: If ``ps`` is missing or a process is unrelated.
    """
    ps = which("ps")
    if ps is None:
        raise RuntimeError("Cannot locate ps to verify SOCKS5 proxy process ownership")
    script = str(Path(__file__).absolute())
    for pid in pids:
        if pid <= 0:
            raise ValueError(f"Invalid SOCKS5 proxy PID: {pid}")
        result = subprocess.run(  # noqa: S603
            [ps, "-ww", "-p", str(pid), "-o", "args="],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 1:
            LOGGER.info("SOCKS5 proxy PID %s already exited", pid)
            continue
        result.check_returncode()
        if script not in shlex.split(result.stdout):
            raise RuntimeError(f"Refusing to stop unrelated process with PID {pid}")
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            LOGGER.info("SOCKS5 proxy PID %s already exited", pid)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--auth")
    args = parser.parse_args()

    script = Path(os.environ["DRIVERS_TOOLS"]) / ".evergreen" / "socks5srv.py"
    sys.argv = [
        str(script),
        "--port",
        str(args.port),
        "--map",
        f"localhost:12345 to {_first_seed(os.environ['MONGODB_URI'])}",
    ]
    if args.auth:
        sys.argv.extend(("--auth", args.auth))
    runpy.run_path(str(script), run_name="__main__")


if __name__ == "__main__":
    main()

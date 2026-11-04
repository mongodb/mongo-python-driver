from __future__ import annotations

import argparse
import os
import runpy
import sys
from pathlib import Path
from urllib.parse import urlsplit


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

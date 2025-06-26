from __future__ import annotations

import argparse
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description="Generate a flamegraph of a given Python script.")

    parser.add_argument(
        "--output",
        default="profile",
        help="Output filename (default: 'profile')",
    )
    parser.add_argument(
        "--sampling_rate",
        default="2000",
        help="Sampling rate in samples/sec (default: 2000)",
    )
    parser.add_argument(
        "--native",
        default=False,
        action=argparse.BooleanOptionalAction,
        help="Whether to profile native extensions (default: False)",
    )
    parser.add_argument(
        "--script_path",
        required=True,
        help="Path to the Python script to be profiled (required)",
    )

    args = parser.parse_args()

    bash_command = [
        "py-spy",
        "record",
        "-o",
        f"{args.output}.svg",
        "-r",
        f"{args.sampling_rate}",
        "--",
        "python",
        f"{args.script_path}",
    ]

    if args.native:
        # Insert --native option at the correct position
        bash_command.insert(6, "--native")

    try:
        subprocess.run(bash_command, check=True)  # noqa: S603
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()

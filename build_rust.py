#!/usr/bin/env python3
"""Build script for the Rust extension module."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path


def check_rust_installed() -> bool:
    """Check if Rust toolchain is installed."""
    try:
        result = subprocess.run(
            ["cargo", "--version"],
            capture_output=True,
            text=True,
            check=False,
        )
        return result.returncode == 0
    except FileNotFoundError:
        return False


def build_rust_extension() -> None:
    """Build the Rust extension using cargo."""
    here = Path(__file__).parent.resolve()
    rust_dir = here / "rust"

    if not rust_dir.exists():
        print("Rust directory not found. Skipping Rust build.")
        return

    if not check_rust_installed():
        print("=" * 80)
        print("WARNING: Rust toolchain not found.")
        print("To build the Rust extension, you need to install Rust:")
        print("  https://rustup.rs/")
        print("")
        print("This is optional - the C extensions will still work.")
        print("=" * 80)
        return

    print("Building Rust extension...")
    os.chdir(rust_dir)

    # Build in release mode for best performance
    try:
        subprocess.run(
            ["cargo", "build", "--release"],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Failed to build Rust extension: {e}")
        print("Continuing without Rust extension...")
        return

    # Copy the built library to the project root
    target_dir = rust_dir / "target" / "release"

    # Find the library file (platform-dependent)
    lib_patterns = [
        "libpymongo_rust.so",  # Linux
        "libpymongo_rust.dylib",  # macOS
        "pymongo_rust.dll",  # Windows
        "pymongo_rust.pyd",  # Windows Python
    ]

    for pattern in lib_patterns:
        lib_files = list(target_dir.glob(pattern))
        if lib_files:
            src = lib_files[0]
            # For Python, we need to rename the library
            if sys.platform == "win32":
                dest_name = "pymongo_rust.pyd"
            else:
                dest_name = f"pymongo_rust{'.so' if sys.platform != 'darwin' else '.so'}"

            dest = here / dest_name
            print(f"Copying {src} to {dest}")
            shutil.copy2(src, dest)
            print("Rust extension built successfully!")
            return

    print("Warning: Could not find built Rust library")


def main():
    """Main entry point."""
    if "--help" in sys.argv:
        print("Usage: python build_rust.py")
        print("Builds the Rust extension module for pymongo")
        return

    build_rust_extension()


if __name__ == "__main__":
    main()

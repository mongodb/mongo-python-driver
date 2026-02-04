"""A custom hatch build hook for pymongo."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import warnings
import zipfile
from pathlib import Path

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomHook(BuildHookInterface):
    """The pymongo build hook."""

    def _build_rust_extension(self, here: Path) -> bool:
        """Build the Rust BSON extension if Rust toolchain is available.

        Returns True if built successfully, False otherwise.
        """
        # Check if Rust is available
        if not shutil.which("cargo"):
            warnings.warn(
                "Rust toolchain not found. Skipping Rust extension build. "
                "Install Rust from https://rustup.rs/ to enable the Rust extension.",
                stacklevel=2,
            )
            return False

        # Check if maturin is available
        if not shutil.which("maturin"):
            try:
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "maturin"],
                    check=True,
                    capture_output=True,
                )
            except subprocess.CalledProcessError:
                warnings.warn(
                    "Failed to install maturin. Skipping Rust extension build.",
                    stacklevel=2,
                )
                return False

        # Build the Rust extension
        rust_dir = here / "bson" / "_rbson"
        if not rust_dir.exists():
            return False

        try:
            # Build the wheel to a temporary directory
            with tempfile.TemporaryDirectory() as tmpdir:
                subprocess.run(
                    [
                        "maturin",
                        "build",
                        "--release",
                        "--out",
                        tmpdir,
                        "--manifest-path",
                        str(rust_dir / "Cargo.toml"),
                    ],
                    check=True,
                    cwd=str(rust_dir),
                )

                # Extract the .so file from the wheel
                # Find the wheel file
                wheel_files = list(Path(tmpdir).glob("*.whl"))
                if not wheel_files:
                    return False

                # Extract the .so file from the wheel
                # The wheel contains _rbson/_rbson.abi3.so, we want bson/_rbson.abi3.so
                with zipfile.ZipFile(wheel_files[0], "r") as whl:
                    for name in whl.namelist():
                        if name.endswith((".so", ".pyd")) and "_rbson" in name:
                            # Extract to bson/ directory
                            so_data = whl.read(name)
                            so_name = Path(name).name  # Just the filename, e.g., _rbson.abi3.so
                            dest = here / "bson" / so_name
                            dest.write_bytes(so_data)
                            return True

                return False

        except (subprocess.CalledProcessError, Exception) as e:
            warnings.warn(
                f"Failed to build Rust extension: {e}. " "The C extension will be used instead.",
                stacklevel=2,
            )
            return False

    def initialize(self, version, build_data):
        """Initialize the hook."""
        if self.target_name == "sdist":
            return
        here = Path(__file__).parent.resolve()
        sys.path.insert(0, str(here))

        # Build C extensions
        subprocess.run([sys.executable, "_setup.py", "build_ext", "-i"], check=True)

        # Build Rust extension (optional)
        # Only build if PYMONGO_BUILD_RUST is set or Rust is available
        build_rust = os.environ.get("PYMONGO_BUILD_RUST", "").lower() in ("1", "true", "yes")
        if build_rust or shutil.which("cargo"):
            self._build_rust_extension(here)

        # Ensure wheel is marked as binary and contains the binary files.
        build_data["infer_tag"] = True
        build_data["pure_python"] = False
        if os.name == "nt":
            patt = ".pyd"
        else:
            patt = ".so"
        for pkg in ["bson", "pymongo"]:
            dpath = here / pkg
            for fpath in dpath.glob(f"*{patt}"):
                relpath = os.path.relpath(fpath, here)
                build_data["artifacts"].append(relpath)
                build_data["force_include"][relpath] = relpath

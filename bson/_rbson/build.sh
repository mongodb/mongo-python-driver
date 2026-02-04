#!/bin/bash
# Build script for Rust BSON extension POC
#
# This script builds the Rust extension and makes it available for testing
# alongside the existing C extension.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
HERE="$( cd -- "$HERE" > /dev/null 2>&1 && pwd )"
BSON_DIR=$(dirname "$HERE")

echo "=== Building Rust BSON Extension POC ==="
echo ""

# Check if Rust is installed
if ! command -v cargo &>/dev/null; then
    echo "Error: Rust is not installed"
    echo ""
    echo "Install Rust with:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    echo ""
    exit 1
fi

echo "Rust toolchain found: $(rustc --version)"

# Check if maturin is installed
if ! command -v maturin &>/dev/null; then
    echo "maturin not found, installing..."
    pip install maturin
fi

echo "maturin found: $(maturin --version)"
echo ""

# Build the extension
echo "Building Rust extension..."
cd "$HERE"

# Build wheel to a temporary directory
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

maturin build --release --out "$TEMP_DIR"

# Extract the .so file from the wheel
echo "Extracting extension from wheel..."
WHEEL_FILE=$(ls "$TEMP_DIR"/*.whl | head -1)

if [ -z "$WHEEL_FILE" ]; then
    echo "Error: No wheel file found"
    exit 1
fi

# Wheels are zip files - extract the .so file
python -c "
import zipfile
import sys
from pathlib import Path

wheel_path = Path(sys.argv[1])
bson_dir = Path(sys.argv[2])

with zipfile.ZipFile(wheel_path, 'r') as whl:
    for name in whl.namelist():
        if name.endswith(('.so', '.pyd')) and '_rbson' in name:
            # Extract to bson/ directory
            so_data = whl.read(name)
            so_name = Path(name).name
            target = bson_dir / so_name
            target.write_bytes(so_data)
            print(f'Installed to {target}')
            sys.exit(0)

print('Error: Could not find .so file in wheel')
sys.exit(1)
" "$WHEEL_FILE" "$BSON_DIR"

echo ""
echo "Build complete!"
echo ""
echo "Test the extension with:"
echo "  python -c 'from bson import _rbson; print(_rbson._test_rust_extension())'"
echo ""

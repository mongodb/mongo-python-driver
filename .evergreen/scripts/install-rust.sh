#!/bin/bash
# Install Rust toolchain for building the Rust BSON extension.
set -eu

echo "Installing Rust toolchain..."

# Check if Rust is already installed
if command -v cargo &> /dev/null; then
    echo "Rust is already installed:"
    rustc --version
    cargo --version
    echo "Updating Rust toolchain..."
    rustup update stable
else
    echo "Rust not found. Installing Rust..."

    # Install Rust using rustup
    if [ "Windows_NT" = "${OS:-}" ]; then
        # Windows installation
        curl --proto '=https' --tlsv1.2 -sSf https://win.rustup.rs/x86_64 -o rustup-init.exe
        ./rustup-init.exe -y --default-toolchain stable
        rm rustup-init.exe

        # Add to PATH for current session
        export PATH="$HOME/.cargo/bin:$PATH"
    else
        # Unix-like installation (Linux, macOS)
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable

        # Source cargo env
        source "$HOME/.cargo/env"
    fi

    echo "Rust installation complete:"
    rustc --version
    cargo --version
fi

# Install maturin if not already installed
if ! command -v maturin &> /dev/null; then
    echo "Installing maturin..."
    cargo install maturin
    echo "maturin installation complete:"
    maturin --version
else
    echo "maturin is already installed:"
    maturin --version
fi

echo "Rust toolchain setup complete."

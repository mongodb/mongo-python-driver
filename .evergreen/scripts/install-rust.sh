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
        # Ensure CARGO_HOME is exported so rustup uses it
        export CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
        export RUSTUP_HOME="${RUSTUP_HOME:-${CARGO_HOME}}"

        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable

        # Source cargo env from the installation location
        # On CI, CARGO_HOME is set to ${DRIVERS_TOOLS}/.cargo by configure-env.sh
        CARGO_ENV_PATH="${CARGO_HOME}/env"

        if [ -f "${CARGO_ENV_PATH}" ]; then
            source "${CARGO_ENV_PATH}"
        else
            echo "Error: Cargo env file not found at ${CARGO_ENV_PATH}"
            echo "CARGO_HOME=${CARGO_HOME}"
            echo "RUSTUP_HOME=${RUSTUP_HOME}"
            echo "HOME=${HOME}"
            exit 1
        fi
    fi

    echo "Rust installation complete:"
    rustc --version
    cargo --version
fi

# Install maturin if not already installed
if ! command -v maturin &> /dev/null; then
    echo "Installing maturin..."
    # Use pip instead of cargo to avoid yanked dependency issues
    # (e.g., maturin 1.12.2 depends on cargo-xwin which has yanked xwin versions)
    pip install maturin
    echo "maturin installation complete:"
    maturin --version
else
    echo "maturin is already installed:"
    maturin --version
fi

echo "Rust toolchain setup complete."

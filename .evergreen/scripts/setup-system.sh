#!/bin/bash
# Set up the system on an evergreen host.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")"
echo "Setting up system..."
bash .evergreen/scripts/configure-env.sh
source .evergreen/scripts/env.sh
bash $DRIVERS_TOOLS/.evergreen/setup.sh
popd

# Run spawn host-specific tasks.
if [ -z "${CI:-}" ]; then
  bash $HERE/setup-dev-env.sh
fi

# On non-CI hosts (spawn hosts, VMs such as GCP/Azure, and local dev) the pinned
# uv and just live in the sourced install dir (env.sh's PYMONGO_BIN_DIR), so make
# sure a login shell finds them by adding it to .bashrc if it is not already
# there. env.sh's PATH does not persist past this SSH session.
if [ "${CI:-}" != "true" ] && [ "${GITHUB_ACTIONS:-}" != "true" ]; then
  _bin="${PYMONGO_BIN_DIR:-$HOME/.local/bin}"
  grep -qF 'export PATH="'"$_bin"':$PATH"' "$HOME/.bashrc" 2>/dev/null || \
    printf 'export PATH="%s:$PATH"\n' "$_bin" >> "$HOME/.bashrc"
fi

# Enable core dumps if enabled on the machine
# Copied from https://github.com/mongodb/mongo/blob/master/etc/evergreen.yml
if [ -f /proc/self/coredump_filter ]; then
    # Set the shell process (and its children processes) to dump ELF headers (bit 4),
    # anonymous shared mappings (bit 1), and anonymous private mappings (bit 0).
    echo 0x13 >/proc/self/coredump_filter

    if [ -f /sbin/sysctl ]; then
        # Check that the core pattern is set explicitly on our distro image instead
        # of being the OS's default value. This ensures that coredump names are consistent
        # across distros and can be picked up by Evergreen.
        core_pattern=$(/sbin/sysctl -n "kernel.core_pattern")
        if [ "$core_pattern" = "dump_%e.%p.core" ]; then
            echo "Enabling coredumps"
            ulimit -c unlimited
        fi
    fi
fi

if [ "$(uname -s)" = "Darwin" ]; then
    core_pattern_mac=$(/usr/sbin/sysctl -n "kern.corefile")
    if [ "$core_pattern_mac" = "dump_%N.%P.core" ]; then
        echo "Enabling coredumps"
        ulimit -c unlimited
    fi
fi

if [ -w /etc/hosts ]; then
  SUDO=""
else
  SUDO="sudo"
fi

# Add 'server' and 'hostname_not_in_cert' as a hostnames
echo "127.0.0.1 server" | $SUDO tee -a /etc/hosts
echo "127.0.0.1 hostname_not_in_cert" | $SUDO tee -a /etc/hosts

echo "Setting up system... done."

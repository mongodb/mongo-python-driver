#!/bin/bash
# Set up the system on an evergreen host.
set -eu

HERE=$(dirname ${BASH_SOURCE:-$0})
pushd "$(dirname "$(dirname $HERE)")"
echo "Setting up system..."
bash .evergreen/scripts/configure-env.sh
source .evergreen/scripts/env.sh
bash $DRIVERS_TOOLS/.evergreen/setup.sh
bash .evergreen/scripts/install-dependencies.sh
popd

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

echo "Setting up system... done."

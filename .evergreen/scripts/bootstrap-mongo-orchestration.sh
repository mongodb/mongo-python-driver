#!/bin/bash

set -eu


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

if [ -z "${TEST_CRYPT_SHARED:-}" ]; then
    export SKIP_CRYPT_SHARED=1
fi

MONGODB_VERSION=${VERSION:-} \
    TOPOLOGY=${TOPOLOGY:-} \
    AUTH=${AUTH:-} \
    SSL=${SSL:-} \
    STORAGE_ENGINE=${STORAGE_ENGINE:-} \
    DISABLE_TEST_COMMANDS=${DISABLE_TEST_COMMANDS:-} \
    ORCHESTRATION_FILE=${ORCHESTRATION_FILE:-} \
    REQUIRE_API_VERSION=${REQUIRE_API_VERSION:-} \
    LOAD_BALANCER=${LOAD_BALANCER:-} \
    bash ${DRIVERS_TOOLS}/.evergreen/run-orchestration.sh
# run-orchestration generates expansion file with the MONGODB_URI for the cluster

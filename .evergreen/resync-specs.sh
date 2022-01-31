#!/bin/bash
# exit when any command fails
set -e
PYMONGO=~/Work/mongo-python-driver
SPECS=~/Work/specifications

help (){
echo "Usage: resync_specs.sh [-bcsp] spec"
echo "Required arguments:"
echo " spec     determines which folder the spec tests will be copied from."
echo "Optional flags:"
echo " -b  is used to add a string to the blocklist for that next run. Can be used"
echo "     any number of times on a single command to block multiple patterns."
echo "     You can use any regex pattern (it is passed to 'grep -Ev')."
echo " -c  is used to set a branch or commit that will be checked out in the"
echo "     specifications repo before copying."
echo " -s  is used to set a unique path to the specs repo for that specific"
echo "     run."
echo " -p  does the same thing but for the pymongo repo."
}

# Parse flag args
BRANCH=""
BLOCKLIST='.*\.yml'
while getopts 'b:c:s:p:' flag; do
  case "${flag}" in
    b) BLOCKLIST+="|"$OPTARG""
      ;;
    c) BRANCH="${OPTARG}"
      ;;
    s) SPECS="${OPTARG}"
      ;;
    p) PYMONGO="${OPTARG}"
      ;;
    *) help; exit 0
      ;;
  esac
done
shift $((OPTIND-1))

if [ -z $BRANCH ]
then
git -C $SPECS checkout $BRANCH
fi

# Ensure the JSON files are up to date.
cd $SPECS/source
make
cd -
# cpjson2 unified-test-format/tests/invalid unified-test-format/invalid
# * param1: Path to spec tests dir in specifications repo
# * param2: Path to where the corresponding tests live in Python.
cpjson2 () {
    find "$PYMONGO"/test/$2 -type f -delete
    cd "$SPECS"/source/$1
    find . -name '*.json' | grep -Ev "${BLOCKLIST}" | cpio -pdm \
    $PYMONGO/test/$2
    printf "\nIgnored files for ${PWD}"
    printf "\n%s\n" "$(diff <(find . -name '*.json' | sort) \
    <(find . -name '*.json' | grep -Ev "${BLOCKLIST}" | sort))" | \
    sed -e '/^[0-9]/d' | sed -e 's|< ./||g'
}

for spec in "$@"
do
  case "$spec" in
    bson*corpus)
      cpjson2 bson-corpus/tests/ bson_corpus/
      ;;
    max*staleness)
      cpjson2 max-staleness/tests/ max_staleness/
      ;;
    connection*string)
      cpjson2 connection-string/tests/ connection_string/test
      ;;
    change*streams)
      cpjson2 change-streams/tests/ change_streams/
      ;;
    cmap|CMAP)
      cpjson2 connection-monitoring-and-pooling/tests cmap
      ;;
    command*monitoring)
      cpjson2 command-monitoring/tests command_monitoring/
      ;;
    crud|CRUD)
      cpjson2 crud/tests/ crud/
      ;;
    load*balancer)
      cpjson2 load-balancers/tests load_balancer/
      ;;
    initial-dns-seedlist-discovery|srv_seedlist)
      cpjson2 initial-dns-seedlist-discovery/tests/ srv_seedlist/
      ;;
    old_srv_seedlist)
      cpjson2 initial-dns-seedlist-discovery/tests srv_seedlist
      ;;
    retryable*reads)
      cpjson2 retryable-reads/tests/ retryable_reads
      ;;
    retryable*writes)
      cpjson2 retryable-writes/tests/ retryable_writes
      ;;
    sdam|SDAM)
      cpjson2 server-discovery-and-monitoring/tests/errors \
      discovery_and_monitoring/errors
      cpjson2 server-discovery-and-monitoring/tests/rs \
      discovery_and_monitoring/rs
      cpjson2 server-discovery-and-monitoring/tests/sharded \
      discovery_and_monitoring/sharded
      cpjson2 server-discovery-and-monitoring/tests/single \
      discovery_and_monitoring/single
      cpjson2 server-discovery-and-monitoring/tests/integration \
      discovery_and_monitoring_integration
      cpjson2 server-discovery-and-monitoring/tests/load-balanced \
      discovery_and_monitoring/load-balanced
      ;;
    sdam*monitoring)
      cpjson2 server-discovery-and-monitoring/tests/monitoring sdam_monitoring
      ;;
    server*selection)
      cpjson2 server-selection/tests/ server_selection/
      ;;
    sessions)
      cpjson2 sessions/tests/ sessions/
      ;;
    transactions|transactions-convenient-api)
      cpjson2 transactions/tests/ transactions/
      cpjson2 transactions-convenient-api/tests/ transactions-convenient-api
      ;;
    unified)
      cpjson2 unified-test-format/tests/ unified-test-format/
      ;;
    uri|uri*options)
      cpjson2 uri-options/tests uri_options
      ;;
    versioned-api)
      cpjson2 versioned-api/tests versioned-api
      ;;
    *)
      echo "Do not know how to resync spec tests for '${spec}'"
      help
      ;;
  esac
done

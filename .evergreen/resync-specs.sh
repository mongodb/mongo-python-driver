#!/bin/bash
# exit when any command fails
set -ex
PYMONGO=/dev/null
SPECS=/dev/null
# Usage: resync_specs.sh [-b|c] spec
# Required arguments:
#  spec     determines which folder the spec tests will be copied from.
# Optional flags:
# -b  is used to add a string to the blocklist for that next run. Can be used
#     any number of times on a single command to block multiple patterns. It
#     ONLY filters based on the name of the FILE. See notes on blacklist
#     feature below.
#
# -c  is used to set a branch or commit that will be checked out in the
#     specifications repo before copying.
#
# -s  is used to set a unique path to the specs repo for that specific run
#
# -p  does the same thing but for the pymongo repo
#
# Notes on blocklist feature:
# It is a string of space separated flags and values. To successfully filter
# commands using this you must specify asterisks before and after a string
# if you want to match it in the middle of the string. It also only searches
# the filename. To illustrate this, if given this path:
# specifications/source/connection-monitoring-and-pooling/tests/pool-create-min-size-error.json
# The following would exclude it:
#   -b "pool-create-min-size-error.json"
#   -b "*create-min-size-error.json"
#   -b "*min-size-error.json"
#   -b "*size-error.json"
# You get the idea.
# These would NOT exclude that file:
#   -b "*tests/pool-create-min-size-error.json"
#   -b "*connection-monitoring-and-pooling/tests/pool-create-min-size-error"
# Etc.

# Parse flag args
#BLACKLIST="-not -name *wait-queue-fairness.json" # PYTHON-2511
BLACKLIST=""
while getopts 'b:c:s:p:' flag; do
  echo "${flag} ${OPTARG}"
  case "${flag}" in
    b) BLACKLIST+=" -not -iname $OPTARG"
      ;;
    c) git -C $SPECS checkout "${OPTARG}"
      ;;
    s) SPECS="${OPTARG}"
      ;;
    p) PYMONGO="${OPTARG}"
      ;;
    *) echo "{$USAGE}"; exit 0
      ;;
  esac
done
shift $((OPTIND-1))
echo $BLACKLIST

# Ensure the JSON files are up to date.
cd $SPECS/source
make

# Usage:
# cpjson unified-test-format/tests/invalid unified-test-format/invalid
# * param1: Path to spec tests dir in specifications repo
# * param2: Path to where the corresponding tests live in Python.
cpjson () {
    rm -f $PYMONGO/test/$2/*.json
    cp $SPECS/source/$1/*.json $PYMONGO/test/$2
}

cpjson2 () {
    find $PYMONGO/test/$2 -name '*.json' -type f -delete
    find $SPECS/source/$1 \
                  -name '*.json'\
                  $BLACKLIST \
                  | cpio -pdmv $PYMONGO/test/$2
}

echo "$@"
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
      cpjson load-balancers/tests load_balancer/
      ;;
    initial-dns-seedlist-discovery|srv_seedlist)
      cpjson2 initial-dns-seedlist-discovery/tests/ srv_seedlist/
      ;;
    old_srv_seedlist)
      cpjson initial-dns-seedlist-discovery/tests srv_seedlist
      ;;
    retryable*reads)
      cpjson retryable-reads/tests/ retryable_reads
      ;;
    retryable*writes)
      cpjson2 retryable-writes/tests/ retryable_writes
      ;;
    sdam|SDAM)
      cpjson server-discovery-and-monitoring/tests/errors discovery_and_monitoring/errors
      cpjson server-discovery-and-monitoring/tests/rs discovery_and_monitoring/rs
      cpjson server-discovery-and-monitoring/tests/sharded discovery_and_monitoring/sharded
      cpjson server-discovery-and-monitoring/tests/single discovery_and_monitoring/single
      cpjson server-discovery-and-monitoring/tests/integration discovery_and_monitoring_integration
      cpjson server-discovery-and-monitoring/tests/load-balanced discovery_and_monitoring/load-balanced
      ;;
    sdam*monitoring)
      cpjson server-discovery-and-monitoring/tests/monitoring sdam_monitoring
      ;;
    server*selection)
      cpjson2 server-selection/tests/ server_selection/
      ;;
    sessions)
      cpjson2 sessions/tests/ sessions/
      ;;
    transactions|transactions-convenient-api)
      cpjson2 transactions/tests/ transactions/
      cpjson transactions-convenient-api/tests/ transactions-convenient-api
      rm $PYMONGO/test/transactions/legacy/errors-client.json  # PYTHON-1894
      ;;
    unified)
      cpjson2 unified-test-format/tests/ unified-test-format/
      #rm $PYMONGO/test/unified-test-format/*/*storeEventsAsEntities*.json
      ;;
    uri|uri*options)
      cpjson uri-options/tests uri_options
      ;;
    versioned-api)
      cpjson versioned-api/tests versioned-api
      ;;
    *)
      echo "Do not know how to resync spec tests for '${spec}'"
      exit 1
      ;;
  esac
done

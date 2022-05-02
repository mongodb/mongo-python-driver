#!/bin/bash
# exit when any command fails
set -e
PYMONGO=$(dirname "$(cd "$(dirname "$0")"; pwd)")
SPECS=${MDB_SPECS:-~/Work/specifications}

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
  echo "Notes:"
  echo "You can export the environment variable MDB_SPECS to set the specs"
  echo " repo similar to -s, but this will persist between runs until you "
  echo "unset it."
}

# Parse flag args
BRANCH=''
BLOCKLIST='.*\.yml'
while getopts 'b:c:s:' flag; do
  case "${flag}" in
    b) BLOCKLIST+="|$OPTARG"
      ;;
    c) BRANCH="${OPTARG}"
      ;;
    s) SPECS="${OPTARG}"
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
# cpjson unified-test-format/tests/invalid unified-test-format/invalid
# * param1: Path to spec tests dir in specifications repo
# * param2: Path to where the corresponding tests live in Python.
cpjson () {
    find "$PYMONGO"/test/$2 -type f -delete
    cd "$SPECS"/source/$1
    find . -name '*.json' | grep -Ev "${BLOCKLIST}" | cpio -pdm \
    $PYMONGO/test/$2
    printf "\nIgnored files for ${PWD}\n"
    IGNORED_FILES="$(printf "\n%s\n" "$(diff <(find . -name '*.json' | sort) \
    <(find . -name '*.json' | grep -Ev "${BLOCKLIST}" | sort))" | \
    sed -e '/^[0-9]/d' | sed -e 's|< ./||g' )"
    printf "%s\n" $IGNORED_FILES
    cd "$PYMONGO"/test/$2
    printf "%s\n" $IGNORED_FILES | xargs git checkout master

}

for spec in "$@"
do
  # Match the spec dir name, the python test dir name, and/or common abbreviations.
  case "$spec" in
    atlas-data-lake-testing|data_lake)
      cpjson atlas-data-lake-testing/tests/ data_lake
      ;;
    bson-corpus|bson_corpus)
      cpjson bson-corpus/tests/ bson_corpus
      ;;
    max-staleness|max_staleness)
      cpjson max-staleness/tests/ max_staleness
      ;;
    connection-string|connection_string)
      cpjson connection-string/tests/ connection_string/test
      ;;
    change-streams|change_streams)
      cpjson change-streams/tests/ change_streams/
      ;;
    client-side-encryption|csfle|fle)
      cpjson client-side-encryption/tests/ client-side-encryption/spec
      cpjson client-side-encryption/corpus/ client-side-encryption/corpus
      cpjson client-side-encryption/external/ client-side-encryption/external
      cpjson client-side-encryption/limits/ client-side-encryption/limits
      ;;
    cmap|CMAP|connection-monitoring-and-pooling)
      cpjson connection-monitoring-and-pooling/tests cmap
      rm $PYMONGO/test/cmap/wait-queue-fairness.json  # PYTHON-1873
      ;;
    apm|APM|command-monitoring|command_monitoring)
      cpjson command-monitoring/tests command_monitoring
      ;;
    crud|CRUD)
      cpjson crud/tests/ crud
      ;;
    load-balancers|load_balancer)
      cpjson load-balancers/tests load_balancer
      ;;
    srv|SRV|initial-dns-seedlist-discovery|srv_seedlist)
      cpjson initial-dns-seedlist-discovery/tests/ srv_seedlist
      ;;
    retryable-reads|retryable_reads)
      cpjson retryable-reads/tests/ retryable_reads
      ;;
    retryable-writes|retryable_writes)
      cpjson retryable-writes/tests/ retryable_writes
      ;;
    sdam|SDAM|server-discovery-and-monitoring|discovery_and_monitoring)
      cpjson server-discovery-and-monitoring/tests/errors \
      discovery_and_monitoring/errors
      cpjson server-discovery-and-monitoring/tests/rs \
      discovery_and_monitoring/rs
      cpjson server-discovery-and-monitoring/tests/sharded \
      discovery_and_monitoring/sharded
      cpjson server-discovery-and-monitoring/tests/single \
      discovery_and_monitoring/single
      cpjson server-discovery-and-monitoring/tests/integration \
      discovery_and_monitoring_integration
      cpjson server-discovery-and-monitoring/tests/load-balanced \
      discovery_and_monitoring/load-balanced
      ;;
    sdam-monitoring|sdam_monitoring)
      cpjson server-discovery-and-monitoring/tests/monitoring sdam_monitoring
      ;;
    server-selection|server_selection)
      cpjson server-selection/tests/ server_selection
      ;;
    sessions)
      cpjson sessions/tests/ sessions
      ;;
    transactions|transactions-convenient-api)
      cpjson transactions/tests/ transactions
      cpjson transactions-convenient-api/tests/ transactions-convenient-api
      rm $PYMONGO/test/transactions/legacy/errors-client.json  # PYTHON-1894
      ;;
    unified|unified-test-format)
      cpjson unified-test-format/tests/ unified-test-format/
      ;;
    uri|uri-options|uri_options)
      cpjson uri-options/tests uri_options
      ;;
    stable-api|versioned-api)
      cpjson versioned-api/tests versioned-api
      ;;
    *)
      echo "Do not know how to resync spec tests for '${spec}'"
      help
      ;;
  esac
done

#!/bin/bash
PYMONGO=$(dirname "$(cd "$(dirname "$0")"; pwd)")

rm -rf $PYMONGO/test/server_selection/logging
rm $PYMONGO/test/transactions/legacy/errors-client.json  # PYTHON-1894
rm $PYMONGO/test/connection_monitoring/wait-queue-fairness.json  # PYTHON-1873
rm $PYMONGO/test/client-side-encryption/spec/unified/fle2v2-BypassQueryAnalysis.json  # PYTHON-5143
rm $PYMONGO/test/client-side-encryption/spec/unified/fle2v2-EncryptedFields-vs-EncryptedFieldsMap.json  # PYTHON-5143
rm $PYMONGO/test/client-side-encryption/spec/unified/localSchema.json  # PYTHON-5143
rm $PYMONGO/test/client-side-encryption/spec/unified/maxWireVersion.json  # PYTHON-5143
rm $PYMONGO/test/unified-test-format/valid-pass/poc-queryable-encryption.json  # PYTHON-5143
rm $PYMONGO/test/gridfs/rename.json  # PYTHON-4931
rm $PYMONGO/test/discovery_and_monitoring/unified/pool-clear-application-error.json  # PYTHON-4918
rm $PYMONGO/test/discovery_and_monitoring/unified/pool-clear-checkout-error.json  # PYTHON-4918
rm $PYMONGO/test/discovery_and_monitoring/unified/pool-clear-min-pool-size-error.json  # PYTHON-4918

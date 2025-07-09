#!/bin/bash
PYMONGO=$(dirname "$(cd "$(dirname "$0")" || exit; pwd)")

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

# Python doesn't implement DRIVERS-3064
rm $PYMONGO/test/collection_management/listCollections-rawdata.json
rm $PYMONGO/test/crud/unified/aggregate-rawdata.json
rm $PYMONGO/test/crud/unified/bulkWrite-deleteMany-rawdata.json
rm $PYMONGO/test/crud/unified/bulkWrite-deleteOne-rawdata.json
rm $PYMONGO/test/crud/unified/bulkWrite-replaceOne-rawdata.json
rm $PYMONGO/test/crud/unified/bulkWrite-updateMany-rawdata.json
rm $PYMONGO/test/crud/unified/bulkWrite-updateOne-rawdata.json
rm $PYMONGO/test/crud/unified/client-bulkWrite-delete-rawdata.json
rm $PYMONGO/test/crud/unified/client-bulkWrite-replaceOne-rawdata.json
rm $PYMONGO/test/crud/unified/client-bulkWrite-update-rawdata.json
rm $PYMONGO/test/crud/unified/count-rawdata.json
rm $PYMONGO/test/crud/unified/countDocuments-rawdata.json
rm $PYMONGO/test/crud/unified/db-aggregate-rawdata.json
rm $PYMONGO/test/crud/unified/deleteMany-rawdata.json
rm $PYMONGO/test/crud/unified/deleteOne-rawdata.json
rm $PYMONGO/test/crud/unified/distinct-rawdata.json
rm $PYMONGO/test/crud/unified/estimatedDocumentCount-rawdata.json
rm $PYMONGO/test/crud/unified/find-rawdata.json
rm $PYMONGO/test/crud/unified/findOneAndDelete-rawdata.json
rm $PYMONGO/test/crud/unified/findOneAndReplace-rawdata.json
rm $PYMONGO/test/crud/unified/findOneAndUpdate-rawdata.json
rm $PYMONGO/test/crud/unified/insertMany-rawdata.json
rm $PYMONGO/test/crud/unified/insertOne-rawdata.json
rm $PYMONGO/test/crud/unified/replaceOne-rawdata.json
rm $PYMONGO/test/crud/unified/updateMany-rawdata.json
rm $PYMONGO/test/crud/unified/updateOne-rawdata.json
rm $PYMONGO/test/index_management/index-rawdata.json

echo "done removing unimplemented tests"

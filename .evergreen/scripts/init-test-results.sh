#!/bin/bash

set +x
. src/.evergreen/scripts/env.sh
echo '{"results": [{ "status": "FAIL", "test_file": "Build", "log_raw": "No test-results.json found was created"  } ]}' >$PROJECT_DIRECTORY/test-results.json

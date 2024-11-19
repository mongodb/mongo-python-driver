#!/bin/bash -eux

PROJECT_DIRECTORY="$(pwd)"
SCRIPT_DIR="$PROJECT_DIRECTORY/.evergreen/scripts"

if [ -f "$SCRIPT_DIR/test-env.sh" ]; then
  echo "Reading $SCRIPT_DIR/test-env.sh file"
  . "$SCRIPT_DIR/test-env.sh"
  exit 0
fi

cat <<EOT > "$SCRIPT_DIR"/test-env.sh
export test_encryption="${test_encryption:-}"
export test_encryption_pyopenssl="${test_encryption_pyopenssl:-}"
export test_crypt_shared="${test_crypt_shared:-}"
export test_pyopenssl="${test_pyopenssl:-}"
export test_loadbalancer="${test_loadbalancer:-}"
export test_serverless="${test_serverless:-}"
export TEST_INDEX_MANAGEMENT="${TEST_INDEX_MANAGEMENT:-}"
export TEST_DATA_LAKE="${TEST_DATA_LAKE:-}"
export ORCHESTRATION_FILE="${ORCHESTRATION_FILE:-}"
export AUTH="${AUTH:-noauth}"
export SSL="${SSL:-nossl}"
EOT

chmod +x "$SCRIPT_DIR"/test-env.sh

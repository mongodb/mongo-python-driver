#!/bin/bash

cd "$DRIVERS_TOOLS"/.evergreen/csfle || exit
. ./activate-kmstlsvenv.sh

# The -u options forces the stdout and stderr streams to be unbuffered.
# TMPDIR is required to avoid "AF_UNIX path too long" errors.
TMPDIR="$(dirname "$DRIVERS_TOOLS")" python -u kms_kmip_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem  --port 5698 &
python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/expired.pem --port 8000 &
python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/wrong-host.pem --port 8001 &
python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem --port 8002 --require_client_cert &

pgrep -f "kms_...._server.py" > kmip_pids.pid

for _ in $(seq 1 1 10); do
   sleep 1
   if python -u kms_kmip_client.py; then
      echo 'KMS KMIP server started!'
      exit 0
   fi
done
echo 'Failed to start KMIP server!'
exit 1

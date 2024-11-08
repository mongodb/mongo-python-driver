set -e
for i in {1..30}; do
  bash .evergreen/run-mongodb-oidc-remote-test.sh
done

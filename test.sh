set -e
for i in {1..10}; do
  bash .evergreen/run-mongodb-oidc-remote-test.sh
done

# export AZUREKMS_RESOURCEGROUP=${testazurekms_resourcegroup}
# export AZUREKMS_VMNAME=${AZUREKMS_VMNAME}
# export AZUREKMS_PRIVATEKEYPATH=/tmp/testazurekms_privatekey
set -eux
source $HOME/azure_creds.sh
export DRIVERS_TOOLS=$HOME/workspace/drivers-evergreen-tools/

export AZUREOIDC_DRIVERS_TAR_FILE=/tmp/mongo-python-driver.tgz
tar czf $AZUREOIDC_DRIVERS_TAR_FILE .

export AZUREOIDC_TEST_CMD="source ./env.sh && PROVIDER_NAME=azure ./.evergreen/run-mongodb-oidc-test.sh"

. $DRIVERS_TOOLS/.evergreen/auth_oidc/azure/create-and-start-vm.sh

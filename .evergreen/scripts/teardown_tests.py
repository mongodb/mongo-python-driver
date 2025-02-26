from __future__ import annotations

import os

from utils import DRIVERS_TOOLS, LOGGER, run_command

TEST_NAME = os.environ.get("TEST_NAME", "unconfigured")
SUB_TEST_NAME = os.environ.get("SUB_TEST_NAME")

LOGGER.info(f"Tearing down tests of type '{TEST_NAME}'...")

# Shut down csfle servers if applicable.
if TEST_NAME == "encryption":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/stop-servers.sh")

# Shut down load balancer if applicable.
elif TEST_NAME == "load-balancer":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/run-load-balancer.sh stop")

# Tear down kms VM if applicable.
elif TEST_NAME == "kms" and SUB_TEST_NAME in ["azure", "gcp"]:
    from kms_tester import teardown_kms

    teardown_kms(SUB_TEST_NAME)

# Tear down auth_aws if applicable.
elif TEST_NAME == "auth_aws":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/auth_aws/teardown.sh")

LOGGER.info(f"Tearing down tests of type '{TEST_NAME}'... done.")

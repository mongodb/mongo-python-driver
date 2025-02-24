from __future__ import annotations

import os

from utils import DRIVERS_TOOLS, run_command

TEST_NAME = os.environ["TEST_NAME"]
SUB_TEST_NAME = os.environ["SUB_TEST_NAME"]

# Shut down csfle servers if applicable
if TEST_NAME == "encryption":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/stop-servers.sh")

# Shut down load balancer if applicable.
elif TEST_NAME == "load-balancer":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/run-load-balancer.sh stop")

# Tear down kms VM if applicable.
elif TEST_NAME == "kms" and SUB_TEST_NAME in ["azure", "gcp"]:
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/csfle/{SUB_TEST_NAME}kms/teardown.sh")

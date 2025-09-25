from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

from utils import DRIVERS_TOOLS, LOGGER, ROOT, run_command

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

# Tear down OIDC if applicable.
elif TEST_NAME == "auth_oidc":
    from oidc_tester import teardown_oidc

    teardown_oidc(SUB_TEST_NAME)

# Tear down ocsp if applicable.
elif TEST_NAME == "ocsp":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/ocsp/teardown.sh")

# Tear down atlas cluster if applicable.
if TEST_NAME in ["aws_lambda", "search_index"]:
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/atlas/teardown-atlas-cluster.sh")

# Tear down auth_aws if applicable.
# We do not run web-identity hosts on macos, because the hosts lack permissions,
# so there is no reason to run the teardown, which would error with a 401.
elif TEST_NAME == "auth_aws" and sys.platform != "darwin":
    run_command(f"bash {DRIVERS_TOOLS}/.evergreen/auth_aws/teardown.sh")

# Tear down perf if applicable.
elif TEST_NAME == "perf":
    shutil.rmtree(ROOT / "specifications", ignore_errors=True)
    Path(os.environ["OUTPUT_FILE"]).unlink(missing_ok=True)

# Tear down mog_wsgi if applicable.
elif TEST_NAME == "mod_wsgi":
    from mod_wsgi_tester import teardown_mod_wsgi

    teardown_mod_wsgi()

# Tear down coverage if applicable.
if os.environ.get("COVERAGE"):
    shutil.rmtree(".pytest_cache", ignore_errors=True)

LOGGER.info(f"Tearing down tests of type '{TEST_NAME}'... done.")

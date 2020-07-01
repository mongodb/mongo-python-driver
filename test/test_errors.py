from pymongo.errors import (AutoReconnect,
                            NotMasterError,
                            OperationFailure)
from test import (client_context,
                  client_knobs,
                  PyMongoTestCase,
                  sanitize_cmd,
                  unittest)
from test.utils import (EventListener,
                        get_pool,
                        rs_or_single_client,
                        single_client,
                        wait_until)

class TestCommandMonitoring(PyMongoTestCase):
    def test_not_master_error(self):
        exc = NotMasterError("not master test", "details")
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except NotMasterError:
            import traceback
            self.assertIn("full error", traceback.format_exc())
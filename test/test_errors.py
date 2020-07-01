from pymongo.errors import (AutoReconnect,
                            NotMasterError,
                            OperationFailure)
from test import (client_context,
                  client_knobs,
                  PyMongoTestCase,
                  sanitize_cmd,
                  unittest)

class TestErrors(PyMongoTestCase):
    def test_not_master_error(self):
        exc = NotMasterError("not master test", "details")
        self.assertIn("full error", str(exc))
        try:
            raise exc
        except NotMasterError:
            import traceback
            self.assertIn("full error", traceback.format_exc())

from test import unittest
from test.test_client import IntegrationTest
from unittest.mock import patch

from bson import json_util
from pymongo.errors import OperationFailure
from pymongo.logger import DEFAULT_DOCUMENT_LENGTH


class TestLogger(IntegrationTest):
    def test_default_truncation_limit(self):
        docs = [{"x": "y"} for _ in range(100)]
        db = self.db

        with self.assertLogs("pymongo.command", level="DEBUG") as cm:
            db.test.insert_many(docs)

            cmd_started_log = json_util.loads((cm.output[0].replace("DEBUG:pymongo.command:", "")))
            self.assertEqual(len(cmd_started_log["command"]), DEFAULT_DOCUMENT_LENGTH + 3)

            cmd_succeeded_log = json_util.loads(
                (cm.output[1].replace("DEBUG:pymongo.command:", ""))
            )
            self.assertLessEqual(len(cmd_succeeded_log["reply"]), DEFAULT_DOCUMENT_LENGTH + 3)

        with self.assertLogs("pymongo.command", level="DEBUG") as cm:
            db.test.find({}).next()
            cmd_succeeded_log = json_util.loads(
                (cm.output[1].replace("DEBUG:pymongo.command:", ""))
            )
            self.assertEqual(len(cmd_succeeded_log["reply"]), DEFAULT_DOCUMENT_LENGTH + 3)

    def test_configured_truncation_limit(self):
        cmd = {"hello": True}
        db = self.db
        with patch.dict("os.environ", {"MONGOB_LOG_MAX_DOCUMENT_LENGTH": "5"}):
            with self.assertLogs("pymongo.command", level="DEBUG") as cm:
                db.command(cmd)

                cmd_started_log = json_util.loads(
                    (cm.output[0].replace("DEBUG:pymongo.command:", ""))
                )
                self.assertEqual(len(cmd_started_log["command"]), 5 + 3)

                cmd_succeeded_log = json_util.loads(
                    (cm.output[1].replace("DEBUG:pymongo.command:", ""))
                )
                self.assertLessEqual(len(cmd_succeeded_log["reply"]), 5 + 3)
                with self.assertRaises(OperationFailure):
                    db.command({"notARealCommand": True})
                cmd_failed_log = json_util.loads(
                    (cm.output[-1].replace("DEBUG:pymongo.command:", ""))
                )
                self.assertEqual(len(cmd_failed_log["reply"]), 5 + 3)

    # def test_truncation_multi_byte_codepoints(self):
    #     ...


if __name__ == "__main__":
    unittest.main()

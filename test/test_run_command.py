from __future__ import annotations

import os
import unittest
from test.unified_format import generate_test_classes

_TEST_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "run_command")


globals().update(
    generate_test_classes(
        os.path.join(_TEST_PATH, "unified"),
        module=__name__,
    )
)


if __name__ == "__main__":
    unittest.main()

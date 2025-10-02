# Integration Tests

A set of tests that verify the usage of PyMongo with downstream packages or frameworks.

Each test uses [PEP 723 inline metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/) and can be run using `pipx` or `uv`.

The `run.sh` convenience script can be used to run all of the files using `uv`.

Here is an example header for the script with the inline dependencies:

```python
# /// script
# dependencies = [
#   "uvloop>=0.18"
# ]
# requires-python = ">=3.10"
# ///
```

Here is an example of using the test helper function to create a configured client for the test:


```python
import asyncio
import sys
from pathlib import Path

# Use pymongo from parent directory.
root = Path(__file__).parent.parent
sys.path.insert(0, str(root))

from test.asynchronous import async_simple_test_client  # noqa: E402


async def main():
    async with async_simple_test_client() as client:
        result = await client.admin.command("ping")
        assert result["ok"]


asyncio.run(main())
```

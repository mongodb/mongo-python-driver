# Integration Tests

A set of tests that verify the usage of PyMongo with downstream packages or frameworks.

Each test uses [PEP 723 inline metadata](https://packaging.python.org/en/latest/specifications/inline-script-metadata/) and can be run using `pipx` or `uv`.

The `run.sh` convenience script can be used to run all of the files using `uv`.

When creating a new script, use the following snippet to ensure that the local version of PyMongo is used:


```python
# Use pymongo from parent directory.
import sys
from pathlib import Path

root = Path(__file__).parent.parent
sys.path.insert(0, str(root))
```

from __future__ import annotations

msg = "PyMongo>=4.8 no longer supports building via setup.py, use python -m pip install <path/to/pymongo> instead"

raise RuntimeError(msg)

from __future__ import annotations

msg = (
    "PyMongo>=4.8 no longer supports building via setup.py, use python -m pip install <path/to/pymongo> instead. If "
    "this is an editable install (-e) please upgrade to pip>=21.3 first: python -m pip install --upgrade pip"
)

raise RuntimeError(msg)

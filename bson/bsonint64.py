from bson.py3compat import PY3

if PY3:
    long = int


class BSONInt64(long):
    """Representation of the BSON int64 type.

    This is necessary because every integral number is an :class:`int` in
    Python 3. Small integral numbers are encoded to BSON int32 by default,
    but BSONInt64 numbers will always be encoded to BSON int64.

    :Parameters:
      - `value`: the numeric value to represent
    """

    _type_marker = 18

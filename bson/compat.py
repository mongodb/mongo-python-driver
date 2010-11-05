import sys


if sys.version_info[0] < 3:
    b = lambda x: x
else:
    b = lambda x: x.encode()

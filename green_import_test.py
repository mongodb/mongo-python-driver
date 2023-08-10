import inspect
import time

import green_test


def inspect_source(method):
    try:
        print(f"Source for {method.__name__}: {inspect.getfile(method)}")
    except TypeError:
        print(f"{method.__name__} is builtin")


inspect_source(time.sleep)
inspect_source(green_test.time.sleep)

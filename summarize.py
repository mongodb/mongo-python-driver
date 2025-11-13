from __future__ import annotations

import csv
import pprint
import re
from collections import defaultdict


def testing_n_threads(f_in, data):
    threads = re.match(r"Testing (?P<n_threads>.*) threads", f_in.readline()).group("n_threads")
    seconds = re.match(
        r"All threads completed after (?P<n_seconds>.*) seconds", f_in.readline()
    ).group("n_seconds")
    tries = re.match(r"Total number of retry attempts: (?P<n_tries>.*)", f_in.readline()).group(
        "n_tries"
    )
    data[f"{threads}_sec"].append(float(seconds))
    data[f"{threads}_try"].append(int(tries))
    return data


def read_table(f_in, data):
    # Initialize the CSV reader with the pipe '|' as the delimiter
    reader = csv.reader(f_in, delimiter="|")
    next(reader)  # skip header

    for row in reader:
        if "threads " in row:
            continue
        row = [col.strip() for col in row]  # noqa:PLW2901
        if row == []:
            continue
        # Convert numbers to appropriate types (int for threads, float for statistics)
        threads = int(row[0])
        avg, p50, p90, p99, p100 = map(float, row[1:])
        # Append the parsed row to the list
        data[f"{threads}_avg"].append(avg)
        data[f"{threads}_p50"].append(p50)
        data[f"{threads}_p90"].append(p90)
        data[f"{threads}_p99"].append(p99)
        data[f"{threads}_p100"].append(p100)
    return data


path = "/Users/iris.ho/Github/backpressure/final"
files = ["main", "local_original_1.5", "local_original_2", "local_server_algo"]
print_data = {}
pp = pprint.PrettyPrinter(width=80)
THREADS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
for f in files:
    data = defaultdict(list)
    with open(f"{path}/{f}.txt") as f_in:
        for _ in THREADS:
            data = testing_n_threads(f_in, data)
            f_in.readline()
        f_in.readline()
        data = read_table(f_in, data)
    print_data[f] = {
        "avg": [data[f"{thread}_avg"] for thread in THREADS],
        "p50": [data[f"{thread}_p50"] for thread in THREADS],
        "p90": [data[f"{thread}_p90"] for thread in THREADS],
        "p99": [data[f"{thread}_p99"] for thread in THREADS],
        "p100": [data[f"{thread}_p100"] for thread in THREADS],
    }
print(print_data)  # noqa: T201

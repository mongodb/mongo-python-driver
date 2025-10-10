from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor

from pymongo import MongoClient


class RunOrderTransaction:
    def __init__(self, client):
        super(RunOrderTransaction, self).__init__()  # noqa:UP008
        self.retry_attempts = -1
        self.time = 0
        self.client = client

    def run(self):
        start = time.time()
        with self.client.start_session() as session:
            try:
                session.with_transaction(self.callback)
            finally:
                self.time = time.time() - start
        return self

    def callback(self, session):
        self.retry_attempts += 1
        return callback(session, self.client)


def callback(session, client):
    order_id = client.test.orders1.insert_one({"sku": "foo", "qty": 1}, session=session).inserted_id
    res = client.test.inventory1.update_one(
        {"sku": "foo", "qty": {"$gte": 1}}, {"$inc": {"qty": -1}}, session=session
    )
    if not res.modified_count:
        raise TypeError("Insufficient inventory count")

    return order_id


def run(num_threads: int, local: bool):
    if local:
        client = MongoClient()
    else:
        client = MongoClient(os.getenv("ATLAS_URI"))
    try:
        client.drop_database("test")
    except Exception:  # noqa: S110
        # fails on atlas?
        pass
    db = client.test
    db.drop_collection("orders1")
    db.create_collection("orders1")
    db.drop_collection("inventory1")
    inventory = db.create_collection("inventory1")
    inventory.insert_one({"sku": "foo", "qty": 1000000})

    f.write("Testing %s threads\n" % num_threads)
    start = time.time()
    N_TXNS = 512
    results = []
    ops = [RunOrderTransaction(client) for _ in range(N_TXNS)]
    with ThreadPoolExecutor(max_workers=num_threads) as exc:
        futures = [exc.submit(op.run) for op in ops]
        for future in futures:
            result = future.result()
            results.append(result)

    end = time.time()
    total_time = end - start
    total_attempts = sum(r.retry_attempts for r in results)

    f.write("All threads completed after %s seconds\n" % (end - start))
    f.write(f"Total number of retry attempts: {total_attempts}\n")
    client.close()

    latencies = sorted(r.time for r in results)
    avg_latency = sum(latencies) / N_TXNS
    p50 = latencies[int(N_TXNS * 0.5)]
    p90 = latencies[int(N_TXNS * 0.9)]
    p99 = latencies[int(N_TXNS * 0.99)]
    p100 = latencies[int(N_TXNS * 1.0) - 1]
    # print(f'avg latency: {avg_latency:.2f}s p50: {p50:.2f}s p90: {p90:.2f}s p99: {p99:.2f}s p100: {p100:.2f}s')
    return total_time, total_attempts, avg_latency, p50, p90, p99, p100


def main(f, local=True):
    NUM_THREADS = [1, 2, 4, 8, 16, 32, 64, 128, 256]
    data = {}
    for num in NUM_THREADS:
        times, attempts, avg_latency, p50, p90, p99, p100 = run(num, local)
        data[num] = {
            "avg": avg_latency,
            "p50": p50,
            "p90": p90,
            "p99": p99,
            "p100": p100,
        }
        f.write("\n")
        time.sleep(10)
    f.write("\nthreads |  avg  |  p50  |  p90  |  p99  |  p100\n")
    for num in NUM_THREADS:
        f.write(
            f"{num:7} | {data[num]['avg']:5.2f} | {data[num]['p50']:5.2f} | {data[num]['p90']:5.2f} | {data[num]['p90']:5.2f} | {data[num]['p100']:5.2f}\n"
        )


if __name__ == "__main__":
    with open("/Users/iris.ho/Github/backpressure/final/local_original_1.5.txt", "w") as f:
        main(f, local=True)

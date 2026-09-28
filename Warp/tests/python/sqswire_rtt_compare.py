#!/usr/bin/env python3
"""Client-observed sqswire latency for SendMessage / ReceiveMessage / DeleteMessage against one Warp jar.

  WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=<jar> python3 sqswire_rtt_compare.py [N]

Run it once per jar (e.g. before/after a change) and compare; the Warp metrics endpoint only reports whole
milliseconds, so this measures from the client (loopback, boto3 with keep-alive) instead. Not a pytest test.
"""
import statistics
import sys
import time

import boto3
from botocore.config import Config

from warp_test_support import RealPostgres, WarpProcess


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))]


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 400
    pg = RealPostgres()
    warp = WarpProcess(pg, "WARP_SQSWIRE_PORT", frontend_name="sqswire")
    try:
        c = boto3.client("sqs", endpoint_url=f"http://localhost:{warp.frontend_port}", region_name="us-east-1",
                         aws_access_key_id="t", aws_secret_access_key="t", config=Config(retries={"max_attempts": 0}))
        url = c.create_queue(QueueName="rtt-compare")["QueueUrl"]
        for i in range(30):  # warm-up
            c.send_message(QueueUrl=url, MessageBody="w")
            c.delete_message(QueueUrl=url, ReceiptHandle=c.receive_message(QueueUrl=url)["Messages"][0]["ReceiptHandle"])
        send, recv, empty, delete = [], [], [], []
        for i in range(n):
            t = time.perf_counter(); c.send_message(QueueUrl=url, MessageBody=f"m{i}"); send.append((time.perf_counter() - t) * 1000)
            t = time.perf_counter(); m = c.receive_message(QueueUrl=url)["Messages"][0]; recv.append((time.perf_counter() - t) * 1000)
            t = time.perf_counter(); c.delete_message(QueueUrl=url, ReceiptHandle=m["ReceiptHandle"]); delete.append((time.perf_counter() - t) * 1000)
            t = time.perf_counter(); c.receive_message(QueueUrl=url); empty.append((time.perf_counter() - t) * 1000)
        for name, xs in (("SendMessage", send), ("ReceiveMessage", recv), ("ReceiveMessage(empty)", empty), ("DeleteMessage", delete)):
            print(f"{name:22s} p50={statistics.median(xs):.3f}ms p90={pct(xs, .9):.3f}ms mean={statistics.mean(xs):.3f}ms (n={len(xs)})")
    finally:
        warp.close()
        pg.close()


if __name__ == "__main__":
    main()

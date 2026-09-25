"""High-sample RTT harness for dynamowire (not a pytest test):
     python3 dynamowire_rtt_bench.py <jar> [n] [label]

Starts a disposable Postgres and one Warp on <jar>, creates a table and times raw DynamoDB-protocol requests over one
keep-alive HTTP connection (no SDK, no signing: dynamowire runs unauthenticated here), after 300 warm-up requests:
PutItem (plain), PutItem with a ConditionExpression, UpdateItem, GetItem (Postgres read, then cache hit) and a
small Query. Prints p50/p90/mean in ms per operation. Alternate two jars (before/after a change) over several rounds
and compare the medians, exactly like rtt_bench.py; set WARP_CLUSTER_SEED_NODES (and WARP_CLUSTER_ENABLED/DISCOVERY)
if stray Warp JVMs hold the default Ignite discovery ports.
"""
import http.client
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import warp_test_support as w  # noqa: E402

jar = sys.argv[1]
n = int(sys.argv[2]) if len(sys.argv) > 2 else 1500
label = sys.argv[3] if len(sys.argv) > 3 else os.path.basename(jar)
w.JAR_PATH = jar
pg = w.RealPostgres()
extra = w.isolated_ports(exclude="WARP_DYNAMOWIRE_PORT")
extra.update({"WARP_OTEL_ENDPOINT": "disabled", "WARP_QOS_RATE_PER_SEC": "100000", "WARP_QOS_BURST": "100000"})
for k in ("WARP_CLUSTER_ENABLED", "WARP_CLUSTER_DISCOVERY", "WARP_CLUSTER_SEED_NODES"):
    if os.environ.get(k):
        extra[k] = os.environ[k]
warp = w.WarpProcess(pg, "WARP_DYNAMOWIRE_PORT", frontend_name="dynamowire", extra_env=extra)
try:
    conn = http.client.HTTPConnection("localhost", warp.frontend_port)

    def call(op, body):
        conn.request("POST", "/", json.dumps(body), {"X-Amz-Target": "DynamoDB_20120810." + op,
                                                      "Content-Type": "application/x-amz-json-1.0"})
        r = conn.getresponse()
        data = r.read()
        if r.status != 200:
            raise RuntimeError(f"{op}: {r.status} {data[:200]}")
        return data

    call("CreateTable", {"TableName": "rttbench", "BillingMode": "PAY_PER_REQUEST",
                         "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}, {"AttributeName": "ts", "KeyType": "RANGE"}],
                         "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"},
                                                  {"AttributeName": "ts", "AttributeType": "N"}]})

    def measure(name, fn, count):
        for i in range(300):
            fn(-1 - i)
        times = []
        for i in range(count):
            t0 = time.perf_counter()
            fn(i)
            times.append((time.perf_counter() - t0) * 1000)
        times.sort()
        print(f"{label:>10} {name:<22} p50={times[len(times) // 2]:.3f} p90={times[int(len(times) * .9)]:.3f} "
              f"mean={statistics.mean(times):.3f} ms  (n={count})", flush=True)

    item = lambda i: {"id": {"S": f"k{i}"}, "ts": {"N": "1"}, "val": {"N": str(i)}, "name": {"S": "x" * 40}}
    measure("PutItem", lambda i: call("PutItem", {"TableName": "rttbench", "Item": item(i)}), n)
    measure("PutItem conditional", lambda i: call("PutItem", {
        "TableName": "rttbench", "Item": {**item(i), "id": {"S": f"c{i}"}},
        "ConditionExpression": "attribute_not_exists(id)"}), n)
    measure("UpdateItem", lambda i: call("UpdateItem", {
        "TableName": "rttbench", "Key": {"id": {"S": f"k{i % 200}"}, "ts": {"N": "1"}},
        "UpdateExpression": "SET val = if_not_exists(val, :z) + :o",
        "ExpressionAttributeValues": {":z": {"N": "0"}, ":o": {"N": "1"}}}), n)
    measure("GetItem (pg read)", lambda i: call("GetItem", {"TableName": "rttbench", "Key": {"id": {"S": f"k{i}"}, "ts": {"N": "1"}}}), n)
    measure("GetItem (cache hit)", lambda i: call("GetItem", {"TableName": "rttbench", "Key": {"id": {"S": "k5"}, "ts": {"N": "1"}}}), n)
    measure("Query (1 item)", lambda i: call("Query", {
        "TableName": "rttbench", "KeyConditionExpression": "id = :i",
        "ExpressionAttributeValues": {":i": {"S": f"k{i % 200}"}}}), n)
finally:
    warp.close()
    pg.close()

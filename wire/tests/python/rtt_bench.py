"""High-sample A/B write-RTT harness (not a pytest test):  python3 rtt_bench.py <jar> <pg|my|mssql|ora|bolt> [n] [label]

Starts a disposable Postgres (a container, or a local server process with WARP_TEST_PG_LOCAL=1) and one Warp on
<jar>, then runs the same autocommit single-row INSERT the per-protocol test_write_rtt_baseline tests run -- but
300 warm-up + n (default 1500) timed statements instead of 40, and prints client p50/p90/mean in ms. Alternate two
jars (before/after a change) over several rounds and compare the medians: the 40-sample pytest RTT tests swing
+-0.15 ms run to run on a busy machine.  Set WARP_CLUSTER_SEED_NODES (and WARP_CLUSTER_ENABLED/DISCOVERY) if stray
Warp JVMs hold the default Ignite discovery ports, exactly as for the pytest suite.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import warp_test_support as w  # noqa: E402

jar, proto = sys.argv[1], sys.argv[2]
n = int(sys.argv[3]) if len(sys.argv) > 3 else 1500
label = sys.argv[4] if len(sys.argv) > 4 else os.path.basename(jar)

VARS = {"pg": "WARP_PGWIRE_PORT", "my": "WARP_MYWIRE_PORT", "mssql": "WARP_MSSQLWIRE_PORT",
        "ora": "WARP_ORAWIRE_PORT", "bolt": "WARP_BOLTWIRE_PORT"}
w.JAR_PATH = jar
pg = w.RealPostgres()
extra = w.isolated_ports(exclude=VARS[proto])
extra.update({"WARP_OTEL_ENDPOINT": "disabled", "WARP_QOS_RATE_PER_SEC": "100000", "WARP_QOS_BURST": "100000"})
for k in ("WARP_CLUSTER_ENABLED", "WARP_CLUSTER_DISCOVERY", "WARP_CLUSTER_SEED_NODES"):
    if os.environ.get(k):
        extra[k] = os.environ[k]
warp = w.WarpProcess(pg, VARS[proto], frontend_name=proto, extra_env=extra)
try:
    port = warp.frontend_port
    if proto == "pg":
        import psycopg2
        c = psycopg2.connect(host="localhost", port=port, user="postgres", password="postgres", dbname="postgres")
        c.autocommit = True
        cur = c.cursor()
        cur.execute("CREATE TABLE rb (id INT PRIMARY KEY, val INT)")
        sql = "INSERT INTO rb (id, val) VALUES (%s, %s) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"
        run = lambda i: cur.execute(sql, (i, i))
    elif proto == "my":
        import pymysql
        c = pymysql.connect(host="localhost", port=port, user="postgres", password="postgres", database="postgres",
                            autocommit=True)
        cur = c.cursor()
        cur.execute("CREATE TABLE rb (id INT PRIMARY KEY, val INT)")
        run = lambda i: cur.execute("INSERT INTO rb (id, val) VALUES (%s, %s)", (i, i))
    elif proto == "mssql":
        import pymssql
        c = pymssql.connect(server="localhost", port=port, user="postgres", password="postgres")
        c.autocommit(True)
        cur = c.cursor()
        cur.execute("CREATE TABLE rb (id INT PRIMARY KEY, val INT)")
        run = lambda i: cur.execute("INSERT INTO rb (id, val) VALUES (%s, %s)", (i, i))
    elif proto == "ora":
        import oracledb
        c = oracledb.connect(user="postgres", password="postgres", dsn=f"localhost:{port}/anything", disable_oob=True)
        c.autocommit = True
        cur = c.cursor()
        cur.execute("CREATE TABLE rb (id NUMBER PRIMARY KEY, val NUMBER)")
        run = lambda i: cur.execute("INSERT INTO rb (id, val) VALUES (:1, :2)", [i, i])
    else:
        from neo4j import GraphDatabase
        d = GraphDatabase.driver(f"bolt://localhost:{port}", auth=("postgres", "postgres"))
        s = d.session()
        run = lambda i: s.run(f"CREATE (n:RttNode {{v: {i}}}) RETURN n.v AS v").single()
    for i in range(300):
        run(i)
    times = []
    for i in range(300, 300 + n):
        t0 = time.perf_counter()
        run(i)
        times.append((time.perf_counter() - t0) * 1000.0)
    times.sort()
    print(f"RESULT {proto} {label} p50={times[len(times) // 2]:.3f} p90={times[int(len(times) * .9)]:.3f} "
          f"mean={sum(times) / len(times):.3f}")
finally:
    warp.close()
    pg.close()

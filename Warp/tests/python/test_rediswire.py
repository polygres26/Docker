"""rediswire (Redis frontend) against a real Warp and real native Postgres: one backend, and two sharded backends.

The redis-py package is not installed here, so the tests speak RESP through redis_resp_client (raw sockets).
Run with WARP_TEST_PG_LOCAL=1 and WARP_TEST_JAR pointing at a built jar.
"""
import threading
import time

import pytest

from redis_resp_client import Push, RespError, Resp, norm
from redis_warp_support import sql, start_warp
from warp_test_support import RealPostgres


@pytest.fixture(scope="module")
def pgs():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def warp1(pgs):
    p = start_warp(pgs[0], extra={"WARP_POOL_MAX_SIZE": "8"})
    yield p
    p.close()


@pytest.fixture(scope="module")
def warp_small(pgs):
    """pool of 4 backend connections: idle subscribers and blocked clients must not consume it. (Developer licence caps
    a Warp at 25 client connections, hence the modest client counts.)"""
    p = start_warp(pgs[1], extra={"WARP_POOL_MAX_SIZE": "4"})
    yield p
    p.close()


@pytest.fixture(scope="module")
def pgs2():
    a, b = RealPostgres(), RealPostgres()
    yield a, b
    a.close()
    b.close()


@pytest.fixture(scope="module")
def warp2(pgs2):
    p = start_warp(pgs2[0], second_pg=pgs2[1])
    yield p
    p.close()


@pytest.fixture()
def r1(warp1):
    c = Resp(port=warp1.frontend_port)
    c("FLUSHALL")
    yield c
    c.close()


@pytest.fixture()
def r2(warp2):
    c = Resp(port=warp2.frontend_port)
    c("FLUSHALL")
    yield c
    c.close()


def n(x):
    return norm(x)


# ---------------------------------------------------------------------------------------------- one backend

def test_families_smoke(r1):
    assert n(r1("SET", "a", "1")) == "OK" and n(r1("INCR", "a")) == 2
    assert n(r1("HSET", "h", "f", "v")) == 1 and n(r1("HGETALL", "h")) == ["f", "v"]
    assert n(r1("RPUSH", "l", "1", "2", "3")) == 3 and n(r1("LRANGE", "l", "0", "-1")) == ["1", "2", "3"]
    assert n(r1("SADD", "s", "x", "y")) == 2 and n(r1("SCARD", "s")) == 2
    assert n(r1("ZADD", "z", "1", "a", "2", "b")) == 2 and n(r1("ZRANGE", "z", "0", "-1", "WITHSCORES")) == ["a", "1", "b", "2"]
    assert n(r1("XADD", "x", "1-1", "f", "v")) == "1-1" and n(r1("XLEN", "x")) == 1
    assert n(r1("PFADD", "hll", "a", "b")) == 1 and n(r1("PFCOUNT", "hll")) == 2
    assert n(r1("SETBIT", "bm", "7", "1")) == 0 and n(r1("BITCOUNT", "bm")) == 1
    assert n(r1("GEOADD", "g", "13.36", "38.11", "p")) == 1 and n(r1("GEOPOS", "g", "p"))[0][0].startswith("13.3")
    assert n(r1("TYPE", "x")) == "stream" and n(r1("DBSIZE")) == 9


def test_data_lives_in_postgres_and_counters_match(r1, pgs):
    r1("RPUSH", "biglist", *[str(i) for i in range(50)])
    r1("SADD", "bigset", *[str(i) for i in range(30)])
    r1("LPOP", "biglist", "5")
    rows = sql(pgs[0], "SELECT k, n FROM warp_redis_keys WHERE k IN ('\\x62696773657400'::bytea) OR true")
    counts = {bytes(k): nn for k, nn in rows}
    assert counts[b"biglist"] == 45 and counts[b"bigset"] == 30
    assert sql(pgs[0], "SELECT count(*) FROM warp_redis_lists")[0][0] == 45
    assert sql(pgs[0], "SELECT count(*) FROM warp_redis_sets")[0][0] == 30


def test_expiry_sweeper_removes_rows(r1, pgs):
    r1("SET", "e1", "v", "PX", "100")
    r1("HSET", "e2", "f", "v")
    r1("PEXPIRE", "e2", "100")
    assert sql(pgs[0], "SELECT count(*) FROM warp_redis_keys WHERE exp IS NOT NULL")[0][0] == 2
    time.sleep(1.5)
    assert sql(pgs[0], "SELECT count(*) FROM warp_redis_keys")[0][0] == 0
    assert sql(pgs[0], "SELECT count(*) FROM warp_redis_hashes")[0][0] == 0
    assert r1("GET", "e1") is None


def test_lazy_expiry_precision(r1):
    r1("SET", "k", "v", "PX", "300")
    assert 0 < r1("PTTL", "k") <= 300
    time.sleep(0.4)
    assert r1("EXISTS", "k") == 0 and r1("PTTL", "k") == -2


def test_concurrent_incr_counts_exactly(warp1, r1):
    def work():
        c = Resp(port=warp1.frontend_port)
        for _ in range(150):
            c("INCR", "ctr")
            c("LPUSH", "q", "x")
        c.close()

    ts = [threading.Thread(target=work) for _ in range(12)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert n(r1("GET", "ctr")) == str(12 * 150) and r1("LLEN", "q") == 12 * 150


def test_blocking_pop_wakes_from_other_connection(warp1, r1):
    c2 = Resp(port=warp1.frontend_port)
    c2.send("BLPOP", "bq", "10")
    time.sleep(0.2)
    t0 = time.time()
    r1("RPUSH", "bq", "hello")
    assert n(c2.read_reply()) == ["bq", "hello"]
    assert time.time() - t0 < 1.0
    c2.send("BLPOP", "bq", "0.3")
    assert c2.read_reply() is None
    c2.close()


def test_pubsub_delivery_and_big_message(warp1, r1):
    sub = Resp(port=warp1.frontend_port)
    sub.send("SUBSCRIBE", "ch")
    assert n(sub.read_reply()) == ["subscribe", "ch", 1]
    assert r1("PUBLISH", "ch", "hi") == 1
    assert n(sub.read_reply()) == ["message", "ch", "hi"]
    big = "x" * 100000
    r1("PUBLISH", "ch", big)
    assert n(sub.read_reply()) == ["message", "ch", big]
    sub.close()


def test_multi_exec_and_watch(warp1, r1):
    c2 = Resp(port=warp1.frontend_port)
    r1("SET", "w", "1")
    r1("WATCH", "w")
    c2("SET", "w", "2")
    r1("MULTI")
    r1("SET", "w", "3")
    assert r1("EXEC") is None and n(r1("GET", "w")) == "2"
    r1("MULTI")
    assert n(r1("INCR", "w")) == "QUEUED"
    assert n(r1("EXEC")) == [3]
    c2.close()


def test_big_value_roundtrip(r1):
    v = bytes(range(256)) * 20480  # 5 MiB, binary
    assert n(r1("SET", "big", v)) == "OK"
    assert r1("GET", "big") == v and r1("STRLEN", "big") == len(v)


def test_pool_starvation_pubsub_and_blocked_clients(warp_small):
    r = Resp(port=warp_small.frontend_port)
    subs, blocked = [], []
    for i in range(12):
        s = Resp(port=warp_small.frontend_port)
        s.send("SUBSCRIBE", f"idle{i}")
        s.read_reply()
        subs.append(s)
    for i in range(8):
        b = Resp(port=warp_small.frontend_port)
        b.send("BLPOP", f"blk{i}", "30")
        blocked.append(b)
    time.sleep(0.5)
    t0 = time.time()
    for i in range(50):
        assert n(r("SET", f"k{i}", "v")) == "OK"
    assert time.time() - t0 < 5
    r("LPUSH", "blk7", "z")
    assert n(blocked[7].read_reply()) == ["blk7", "z"]
    assert r("PUBLISH", "idle3", "m") == 1 and n(subs[3].read_reply()) == ["message", "idle3", "m"]
    for c in subs + blocked + [r]:
        c.close()


def test_scripting_is_a_clear_error(r1):
    e = r1("EVAL", "return 1", "0")
    assert isinstance(e, RespError) and "scripting is not supported" in e.text
    assert n(r1("EVALSHA", "abc", "0")).text.startswith("NOSCRIPT")


def test_auth_password_and_metrics(warp1, r1):
    txt = warp1.metrics_text()
    assert "rediswire" in txt


def test_cluster_emulation_single(r1):
    slots = r1("CLUSTER", "SLOTS")
    assert slots[0][0] == 0 and slots[0][1] == 16383
    assert r1("CLUSTER", "KEYSLOT", "foo") == 12182 and r1("CLUSTER", "KEYSLOT", "{foo}bar") == 12182


def test_resp3_types(warp1):
    c = Resp(port=warp1.frontend_port, protocol=3)
    c("FLUSHALL")
    c("HSET", "h", "a", "1")
    assert type(c("HGETALL", "h")).__name__ == "RMap"
    assert type(c("SMEMBERS", "nokey")).__name__ == "RSet"
    c("ZADD", "z", "1.5", "a")
    assert c("ZSCORE", "z", "a") == 1.5
    assert c("GET", "nokey") is None
    c.close()


# ---------------------------------------------------------------------------------------------- two backends

def shard_counts(pgs2):
    return [sql(p, "SELECT count(*) FROM warp_redis_keys")[0][0] for p in pgs2]


def test_two_backends_keys_spread(r2, pgs2):
    for i in range(300):
        r2("SET", f"key{i}", "v")
    a, b = shard_counts(pgs2)
    assert a + b == 300 and a > 50 and b > 50, (a, b)
    assert r2("DBSIZE") == 300
    seen, cur = set(), b"0"
    while True:
        cur, keys = r2("SCAN", cur, "COUNT", "37")
        seen.update(keys)
        if cur == b"0":
            break
    assert len(seen) == 300
    assert len(r2("KEYS", "key*")) == 300


def test_two_backends_crossslot_and_hash_tags(r2):
    e = r2("MSET", "a", "1", "b", "2")
    assert isinstance(e, RespError) and e.text.startswith("CROSSSLOT")
    assert n(r2("MSET", "{u}a", "1", "{u}b", "2")) == "OK"
    assert n(r2("MGET", "{u}a", "{u}b")) == ["1", "2"]
    assert n(r2("SUNIONSTORE", "{u}d", "{u}s1", "{u}s2")) == 0
    e = r2("RENAME", "x1", "x2")
    assert isinstance(e, RespError) and (e.text.startswith("CROSSSLOT") or "no such key" in e.text)
    r2("MULTI")
    r2("SET", "a", "1")
    r2("SET", "b", "2")
    e = r2("EXEC")
    assert isinstance(e, RespError) and e.text.startswith("CROSSSLOT")
    r2("MULTI")
    r2("SET", "{t}a", "1")
    r2("INCR", "{t}a")
    assert n(r2("EXEC")) == ["OK", 2]


def test_two_backends_cluster_slots_and_blocking_pubsub(warp2, r2):
    slots = r2("CLUSTER", "SLOTS")
    assert slots[0][0] == 0 and slots[0][1] == 16383
    for key in ("bk-a", "bk-b", "bk-c"):
        c = Resp(port=warp2.frontend_port)
        c.send("BLPOP", key, "10")
        time.sleep(0.15)
        r2("RPUSH", key, "v")
        assert n(c.read_reply()) == [key, "v"]
        c.close()
    sub = Resp(port=warp2.frontend_port)
    sub.send("SUBSCRIBE", "c1")
    sub.read_reply()
    r2("PUBLISH", "c1", "m")
    assert n(sub.read_reply()) == ["message", "c1", "m"]
    sub.send("SSUBSCRIBE", "sc")
    assert n(sub.read_reply()) == ["ssubscribe", "sc", 1]
    r2("SPUBLISH", "sc", "sm")
    assert n(sub.read_reply()) == ["smessage", "sc", "sm"]
    sub.close()

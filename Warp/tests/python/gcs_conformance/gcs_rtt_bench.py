"""Round-trip times of small GCS operations: fake-gcs-server (oracle, Docker) vs Warp (native Postgres).

  python3 gcs_rtt_bench.py --oracle http://localhost:PORT [--warp-shards 1|2] [--n 300]

Every request goes over one keep-alive connection, sequentially, from the same client for both sides (anonymous access).
Prints median / p95 in milliseconds per operation.
"""
import argparse
import json
import os
import statistics
import sys
import time
import urllib.parse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import requests  # noqa: E402

J = "/storage/v1/b"
U = "/upload/storage/v1/b"


def bench(base, n, tag):
    s = requests.Session()
    run = f"{tag}{int(time.time()) % 100000}"
    b = f"rtt{run}"
    assert s.post(base + J, params={"project": "p"}, json={"name": b}).status_code == 200
    res = {}

    def t(name, fn):
        xs = []
        for i in range(n):
            st = time.perf_counter()
            r = fn(i)
            xs.append((time.perf_counter() - st) * 1000)
            assert r.status_code < 300, (name, r.status_code, r.text[:200])
        xs.sort()
        res[name] = (statistics.median(xs), xs[int(len(xs) * 0.95) - 1])

    body = b"x" * 1024
    for i in range(20):  # warm-up
        s.post(f"{base}{U}/{b}/o", params={"uploadType": "media", "name": f"w{i}"}, data=body)
    t("object insert 1 KiB (media)", lambda i: s.post(f"{base}{U}/{b}/o", params={"uploadType": "media", "name": f"d/o{i}"}, data=body))
    t("object get metadata", lambda i: s.get(f"{base}{J}/{b}/o/{urllib.parse.quote(f'd/o{i}', safe='')}"))
    t("object get media 1 KiB", lambda i: s.get(f"{base}{J}/{b}/o/{urllib.parse.quote(f'd/o{i}', safe='')}", params={"alt": "media"}))
    t("list objects (prefix, max 20)", lambda i: s.get(f"{base}{J}/{b}/o", params={"prefix": "d/o", "maxResults": "20"}))
    t("object patch metadata", lambda i: s.patch(f"{base}{J}/{b}/o/{urllib.parse.quote(f'd/o{i}', safe='')}", json={"metadata": {"k": str(i)}}))
    t("object delete", lambda i: s.delete(f"{base}{J}/{b}/o/{urllib.parse.quote(f'd/o{i}', safe='')}"))
    return res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--oracle")
    ap.add_argument("--warp-shards", type=int, default=1)
    ap.add_argument("--n", type=int, default=300)
    a = ap.parse_args()
    out = {}
    if a.oracle:
        out["fake-gcs-server (Docker)"] = bench(a.oracle, a.n, "o")
    import gcs_launch_warp
    w = gcs_launch_warp.GcsWarp(a.warp_shards)
    try:
        out[f"Warp ({a.warp_shards} native Postgres)"] = bench(w.url, a.n, "w")
    finally:
        w.close()
    names = list(next(iter(out.values())))
    print(f"{'operation':34}" + "".join(f"{k:>36}" for k in out))
    for nm in names:
        print(f"{nm:34}" + "".join(f"{v[nm][0]:>20.2f} ms med /{v[nm][1]:>6.2f} p95" for v in out.values()))


if __name__ == "__main__":
    main()

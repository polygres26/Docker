"""A tiny signed HTTP client for the Cosmos DB REST API (master key), used where the SDK hides the wire (headers, status codes, plan)."""
import base64
import hashlib
import hmac
import json
import urllib.parse
from email.utils import formatdate

import requests

EMULATOR_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="


def resource_of(path):
    p = path.strip("/")
    if not p:
        return "", ""
    seg = p.split("/")
    if len(seg) % 2 == 1:
        return seg[-1], "/".join(seg[:-1])
    return seg[-2], p


def auth_header(key, verb, path, date):
    rtype, link = resource_of(path)
    payload = f"{verb.lower()}\n{rtype.lower()}\n{link}\n{date.lower()}\n\n"
    sig = base64.b64encode(hmac.new(base64.b64decode(key), payload.encode(), hashlib.sha256).digest()).decode()
    return urllib.parse.quote(f"type=master&ver=1.0&sig={sig}", safe="")


class Raw:
    def __init__(self, port, key=EMULATOR_KEY):
        self.base = f"http://localhost:{port}"
        self.key = key
        self.s = requests.Session()

    def req(self, verb, path, body=None, headers=None, raw_body=None, key=None):
        date = formatdate(usegmt=True)
        h = {"x-ms-date": date, "x-ms-version": "2018-12-31", "Authorization": auth_header(key or self.key, verb, path, date),
             "Content-Type": "application/json", "Accept": "application/json"}
        h.update(headers or {})
        data = raw_body if raw_body is not None else (json.dumps(body) if body is not None else None)
        return self.s.request(verb, self.base + path, data=data, headers=h, timeout=60)

    def query(self, coll_path, sql, params=None, pk=None, max_items=None, cross=True, extra=None):
        """Runs a query following continuation tokens; returns (all rows, list of page sizes, last response)."""
        rows, pages, cont = [], [], None
        while True:
            h = {"Content-Type": "application/query+json", "x-ms-documentdb-isquery": "True",
                 "x-ms-documentdb-query-enablecrosspartition": str(cross)}
            if pk is not None:
                h["x-ms-documentdb-partitionkey"] = json.dumps(pk)
            if max_items:
                h["x-ms-max-item-count"] = str(max_items)
            if cont:
                h["x-ms-continuation"] = cont
            h.update(extra or {})
            r = self.req("POST", coll_path + "/docs", {"query": sql, "parameters": params or []}, h)
            assert r.status_code == 200, (r.status_code, r.text)
            d = r.json()["Documents"]
            rows += d
            pages.append(len(d))
            cont = r.headers.get("x-ms-continuation")
            if not cont:
                return rows, pages, r

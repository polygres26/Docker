"""Raw REST client for the three Azure Storage services (SharedKey / SharedKeyLite / SAS / anonymous)."""
import urllib.parse

import requests

import az_signer as S

VERSION = "2021-08-06"


class Endpoint:
    def __init__(self, blob, queue, table):
        self.base = {"blob": blob.rstrip("/"), "queue": queue.rstrip("/"), "table": table.rstrip("/")}


def do(ep, service, method, path, query=None, headers=None, body=b"", auth="key", account=S.DEV_ACCOUNT,
       key=S.DEV_KEY, sas=None, stream=False, version=VERSION, timeout=60):
    """path is relative to the account ('/container/blob'); path-style addressing (account first in the URL path)."""
    query = list(query or [])
    if isinstance(body, str):
        body = body.encode()
    h = {"x-ms-version": version, "x-ms-date": S.rfc1123()}
    if service == "table":
        h.setdefault("Accept", "application/json;odata=minimalmetadata")
        h.setdefault("DataServiceVersion", "3.0;NetFx")
        h.setdefault("MaxDataServiceVersion", "3.0;NetFx")
    h.update(headers or {})
    h = {k: v for k, v in h.items() if v is not None}
    full_path = "/" + account + (path if path.startswith("/") or path == "" else "/" + path)
    if sas:
        query = query + list(sas.items())
    if body or method in ("PUT", "POST", "MERGE", "PATCH"):
        h["Content-Length"] = str(len(body))
    if auth in ("key", "lite", "badkey"):
        signing_headers = dict(h)
        k = key if auth != "badkey" else "d3Jvbmcta2V5LXdyb25nLWtleS13cm9uZy1rZXktd3Jvbmc="
        h["Authorization"] = S.sign(service, method, account, k, urllib.parse.quote(full_path, safe="/~%"),
                                    query, signing_headers, lite=(auth == "lite"))
    qs = urllib.parse.urlencode(query, quote_via=urllib.parse.quote, safe="")
    url = ep.base[service] + urllib.parse.quote(full_path, safe="/~%") + ("?" + qs if qs else "")
    h.pop("Content-Length", None)
    r = requests.request(method, url, headers=h, data=body, timeout=timeout, stream=stream)
    return r

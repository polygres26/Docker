"""Raw REST client for the GCS JSON / XML APIs (requests only: the Google Cloud SDKs are not installed here)."""
import requests

_S = requests.Session()


def do(base, method, path, query=None, headers=None, body=b"", auth=None, timeout=120, stream=False):
    """`auth`: None (anonymous) or a bearer token string."""
    h = dict(headers or {})
    if auth:
        h["Authorization"] = "Bearer " + auth
    return requests.request(method, base + path, params=[tuple(q) for q in (query or [])], headers=h, data=body,
                            timeout=timeout, stream=stream)

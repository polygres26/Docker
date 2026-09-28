"""Azure Storage request signing + SAS generation for the harness (SharedKey, SharedKeyLite, service/account SAS).

Implemented from the public REST documentation and verified against Azurite: Azurite rejects a wrong signature, so a
request that Azurite accepts proves the string-to-sign here is right.
"""
import base64
import datetime
import hashlib
import hmac
import urllib.parse

DEV_ACCOUNT = "devstoreaccount1"
DEV_KEY = ("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
API_VERSION = "2021-08-06"


def rfc1123(now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return now.strftime("%a, %d %b %Y %H:%M:%S GMT")


def _hmac(key_b64, s):
    return base64.b64encode(hmac.new(base64.b64decode(key_b64), s.encode("utf-8"), hashlib.sha256).digest()).decode()


def _canon_headers(h):
    items = sorted((k.lower().strip(), " ".join(str(v).split())) for k, v in h.items() if k.lower().startswith("x-ms-"))
    return "".join(f"{k}:{v}\n" for k, v in items)


def _canon_resource(account, path, query, lite=False, table=False):
    res = "/" + account + path
    params = {}
    for k, v in query:
        params.setdefault(k.lower(), []).append(v)
    if lite or table:
        if "comp" in params:
            res += "?comp=" + params["comp"][0]
        return res
    for k in sorted(params):
        res += "\n" + k + ":" + ",".join(sorted(params[k]))
    return res


def string_to_sign(service, method, account, path, query, headers, lite=False):
    h = {k.lower(): v for k, v in headers.items()}
    clen = h.get("content-length", "")
    if clen == "0":
        clen = ""
    if service == "table":
        date = h.get("x-ms-date") or h.get("date", "")
        if lite:
            return date + "\n" + _canon_resource(account, path, query, table=True)
        return "\n".join([method, h.get("content-md5", ""), h.get("content-type", ""), date,
                          _canon_resource(account, path, query, table=True)])
    if lite:
        return "\n".join([method, h.get("content-md5", ""), h.get("content-type", ""), h.get("date", ""),
                          _canon_headers(h) + _canon_resource(account, path, query, lite=True)])
    return "\n".join([method, h.get("content-encoding", ""), h.get("content-language", ""), clen,
                      h.get("content-md5", ""), h.get("content-type", ""), h.get("date", ""),
                      h.get("if-modified-since", ""), h.get("if-match", ""), h.get("if-none-match", ""),
                      h.get("if-unmodified-since", ""), h.get("range", ""),
                      _canon_headers(h) + _canon_resource(account, path, query)])


def sign(service, method, account, key, path, query, headers, lite=False):
    """Returns the Authorization header value. `path` is the URL path AFTER the host (path-style: /account/...)."""
    sts = string_to_sign(service, method, account, path, query, headers, lite)
    return ("SharedKeyLite " if lite else "SharedKey ") + account + ":" + _hmac(key, sts)


# ---- SAS ------------------------------------------------------------------------------------------------------

def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def service_sas(service, account, key, resource_path, permissions, expiry, start=None, ip=None, protocol=None,
                version="2021-08-06", sr=None, identifier=None, snapshot="", cache_control="", content_disposition="",
                content_encoding="", content_language="", content_type="", table_range=None):
    """resource_path: canonical name without leading '/account': e.g. 'container' or 'container/blob' or 'queue' or
    'table'. sr: 'b' or 'c' (blob); table range = (spk, srk, epk, erk). Returns a dict of query params."""
    start = start or ""
    ip = ip or ""
    protocol = protocol or ""
    identifier = identifier or ""
    if service == "blob":
        canon = f"/blob/{account}/{resource_path}"
        sr = sr or ("b" if "/" in resource_path else "c")
        fields = [permissions, start, expiry, canon, identifier, ip, protocol, version, sr, snapshot, "",
                  cache_control, content_disposition, content_encoding, content_language, content_type]
        if version >= "2020-12-06":
            fields = [permissions, start, expiry, canon, identifier, ip, protocol, version, sr, snapshot, "",
                      cache_control, content_disposition, content_encoding, content_language, content_type]
        sts = "\n".join(fields)
    elif service == "queue":
        canon = f"/queue/{account}/{resource_path}"
        sts = "\n".join([permissions, start, expiry, canon, identifier, ip, protocol, version])
    else:
        canon = f"/table/{account}/{resource_path}"
        spk, srk, epk, erk = table_range or ("", "", "", "")
        sts = "\n".join([permissions, start, expiry, canon, identifier, ip, protocol, version, spk, srk, epk, erk])
    q = {"sv": version, "sp": permissions, "se": expiry, "sig": _hmac(key, sts)}
    if service == "blob":
        q["sr"] = sr
    if service == "table":
        q["tn"] = resource_path
        if table_range:
            q["spk"], q["srk"], q["epk"], q["erk"] = table_range
    if start:
        q["st"] = start
    if ip:
        q["sip"] = ip
    if protocol:
        q["spr"] = protocol
    if identifier:
        q["si"] = identifier
    if service == "blob":
        for k, v in (("rscc", cache_control), ("rscd", content_disposition), ("rsce", content_encoding),
                     ("rscl", content_language), ("rsct", content_type)):
            if v:
                q[k] = v
    return q


def account_sas(account, key, services, resource_types, permissions, expiry, start=None, ip=None, protocol=None,
                version="2021-08-06"):
    sts = "\n".join([account, permissions, services, resource_types, start or "", expiry, ip or "", protocol or "",
                     version, ""]) + "\n"
    q = {"sv": version, "ss": services, "srt": resource_types, "sp": permissions, "se": expiry, "sig": _hmac(key, sts)}
    if start:
        q["st"] = start
    if ip:
        q["sip"] = ip
    if protocol:
        q["spr"] = protocol
    return q

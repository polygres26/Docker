"""Signers for the GCS XML API and signed URLs: SigV4 (AWS4-HMAC-SHA256 / GOOG4-HMAC-SHA256 with HMAC keys), V4 signed URLs
(GOOG4-RSA-SHA256) and V2 signed URLs (RSA-SHA256), the way the Google client libraries and boto3 produce them."""
import base64
import datetime
import hashlib
import hmac
import urllib.parse

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def _q(s, safe=""):
    return urllib.parse.quote(s, safe=safe + "~")


def _canon_query(pairs):
    return "&".join(f"{_q(k)}={_q(v)}" for k, v in sorted((k, v) for k, v in pairs))


def _canon_request(method, path, query, headers, signed, payload):
    ch = "".join(f"{h}:{' '.join(str(headers[h]).split())}\n" for h in signed)
    return "\n".join([method, _q(urllib.parse.unquote(path), "/"), _canon_query(query), ch, ";".join(signed), payload])


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def sigv4(method, host, path, query, headers, body_hash, access, secret, algo="AWS4-HMAC-SHA256", now=None):
    """Returns the headers (incl. Authorization) to send. algo AWS4-HMAC-SHA256 (region auto, service s3) or
    GOOG4-HMAC-SHA256 (region auto, service storage, goog4_request)."""
    now = now or _now()
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    date = stamp[:8]
    goog = algo.startswith("GOOG4")
    service, term, prefix = ("storage", "goog4_request", "GOOG4") if goog else ("s3", "aws4_request", "AWS4")
    h = {**headers, "host": host, ("x-goog-date" if goog else "x-amz-date"): stamp,
         ("x-goog-content-sha256" if goog else "x-amz-content-sha256"): body_hash}
    h = {k.lower(): v for k, v in h.items()}
    signed = sorted(h)
    canon = _canon_request(method, path, query, h, signed, body_hash)
    scope = f"{date}/auto/{service}/{term}"
    sts = f"{algo}\n{stamp}\n{scope}\n{hashlib.sha256(canon.encode()).hexdigest()}"
    k = hmac.new((prefix + secret).encode(), date.encode(), hashlib.sha256).digest()
    for part in ("auto", service, term):
        k = hmac.new(k, part.encode(), hashlib.sha256).digest()
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    h.pop("host")
    h["authorization"] = f"{algo} Credential={access}/{scope}, SignedHeaders={';'.join(signed)}, Signature={sig}"
    return h


def new_rsa():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    pub = key.public_key().public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
    return key, pem, pub


def signed_url_v4(key, email, method, host, path, expires=3600, query=None, headers=None, now=None):
    now = now or _now()
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    h = {"host": host, **{k.lower(): v for k, v in (headers or {}).items()}}
    signed = sorted(h)
    q = list(query or []) + [("X-Goog-Algorithm", "GOOG4-RSA-SHA256"), ("X-Goog-Credential", f"{email}/{stamp[:8]}/auto/storage/goog4_request"),
                             ("X-Goog-Date", stamp), ("X-Goog-Expires", str(expires)), ("X-Goog-SignedHeaders", ";".join(signed))]
    canon = _canon_request(method, path, q, h, signed, "UNSIGNED-PAYLOAD")
    sts = f"GOOG4-RSA-SHA256\n{stamp}\n{stamp[:8]}/auto/storage/goog4_request\n{hashlib.sha256(canon.encode()).hexdigest()}"
    sig = key.sign(sts.encode(), padding.PKCS1v15(), hashes.SHA256()).hex()
    return path + "?" + _canon_query(q) + "&X-Goog-Signature=" + sig


def signed_url_v2(key, email, method, path, expires_epoch, content_type="", content_md5=""):
    sts = f"{method}\n{content_md5}\n{content_type}\n{expires_epoch}\n{urllib.parse.unquote(path)}"
    sig = base64.b64encode(key.sign(sts.encode(), padding.PKCS1v15(), hashes.SHA256())).decode()
    return path + "?" + urllib.parse.urlencode([("GoogleAccessId", email), ("Expires", str(expires_epoch)), ("Signature", sig)])

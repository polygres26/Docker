"""The differential corpus: named cases, each a list of REST steps replayed identically against fake-gcs-server and Warp.

Templates: @R@ = per-run token (letters, used in bucket names), <<name>> = a value captured by an earlier step
(cap={"name": "h:Header" | "j:json.path" | "x:regex"}). Step keys: method, path (below the host), query [(k, v)], headers,
body (bytes or a zero-arg callable), json (dict, sent as application/json), raw (compare body bytes exactly),
hash (compare only sha1 of the body), known_diff.
"""
import hashlib
import json
import urllib.parse

CASES = {}
B = "bk@R@"
J = "/storage/v1/b"
U = "/upload/storage/v1/b"
P = [("project", "warp-test-project")]


def on(name):
    """URL-encode an object name for a path segment."""
    return urllib.parse.quote(name, safe="")


class K:
    def __init__(self, name):
        self.steps = []
        CASES[name] = self.steps

    def r(self, method, path, q=None, h=None, body=b"", **kw):
        self.steps.append(dict(method=method, path=path, query=list(q or []), headers=dict(h or {}), body=body, **kw))
        return self

    def js(self, method, path, obj, q=None, h=None, **kw):
        return self.r(method, path, q, {"Content-Type": "application/json", **(h or {})}, json.dumps(obj), **kw)

    # bucket shortcuts
    def mkb(self, name=B, **body):
        return self.js("POST", J, {"name": name, **body}, q=P)

    def up(self, name, data=b"hello", ctype="text/plain", bucket=B, q=None, h=None, **kw):
        return self.r("POST", f"{U}/{bucket}/o", [("uploadType", "media"), ("name", name)] + list(q or []),
                      {"Content-Type": ctype, **(h or {})}, data, **kw)

    def get(self, name, bucket=B, q=None, h=None, **kw):
        return self.r("GET", f"{J}/{bucket}/o/{on(name)}", q, h, **kw)

    def media(self, name, bucket=B, q=None, h=None, **kw):
        return self.r("GET", f"{J}/{bucket}/o/{on(name)}", [("alt", "media")] + list(q or []), h, **kw)

    def rm(self, name, bucket=B, q=None, **kw):
        return self.r("DELETE", f"{J}/{bucket}/o/{on(name)}", q, **kw)

    def ls(self, bucket=B, q=None, **kw):
        return self.r("GET", f"{J}/{bucket}/o", q, **kw)


def mp_related(meta, data, ctype="application/octet-stream"):
    b = "xxBOUNDARYxx"
    body = (f"--{b}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".encode() + json.dumps(meta).encode()
            + f"\r\n--{b}\r\nContent-Type: {ctype}\r\n\r\n".encode() + data + f"\r\n--{b}--".encode())
    return {"Content-Type": f"multipart/related; boundary={b}"}, body


def blob(n, seed=7):
    x = (seed * 2654435761) & 0xFFFFFFFF
    out = bytearray()
    while len(out) < n:
        x = (x * 1103515245 + 12345) & 0x7FFFFFFF
        out += x.to_bytes(4, "big")
    return bytes(out[:n])


KIB = 1024
CHUNK = 256 * KIB

# ------------------------------------------------------------------------------------------------ buckets

K("bucket_lifecycle").mkb().mkb().r("GET", f"{J}/{B}").r("GET", f"{J}/nosuch@R@").r("GET", J, P) \
    .js("PATCH", f"{J}/{B}", {"labels": {"env": "test", "team": "x"}}).r("GET", f"{J}/{B}") \
    .js("PATCH", f"{J}/{B}", {"labels": {"team": None}}).r("GET", f"{J}/{B}") \
    .r("DELETE", f"{J}/{B}").r("DELETE", f"{J}/{B}").r("GET", f"{J}/{B}")

K("bucket_create_fields").mkb(location="EU", storageClass="NEARLINE", labels={"a": "b"}).r("GET", f"{J}/{B}") \
    .mkb("bkb@R@", versioning={"enabled": True}).r("GET", f"{J}/bkb@R@").r("DELETE", f"{J}/bkb@R@")

K("bucket_names").js("POST", J, {"name": "AB"}, q=P).js("POST", J, {"name": "a"}, q=P) \
    .js("POST", J, {"name": "-bad@R@"}, q=P).js("POST", J, {"name": "goog-x@R@"}, q=P) \
    .js("POST", J, {"name": "has space"}, q=P).js("POST", J, {}, q=P).r("POST", J, P, {"Content-Type": "application/json"}, b"{not json")

K("bucket_missing_project").js("POST", J, {"name": B}).r("GET", J, [("prefix", B)])

K("bucket_delete_nonempty").mkb().up("o1").r("DELETE", f"{J}/{B}").rm("o1").r("DELETE", f"{J}/{B}")

K("bucket_metageneration_preconditions").mkb().r("GET", f"{J}/{B}", [("ifMetagenerationMatch", "1")]) \
    .r("GET", f"{J}/{B}", [("ifMetagenerationMatch", "9")]) \
    .js("PATCH", f"{J}/{B}", {"labels": {"a": "1"}}, q=[("ifMetagenerationMatch", "9")]) \
    .js("PATCH", f"{J}/{B}", {"labels": {"a": "1"}}, q=[("ifMetagenerationMatch", "1")]).r("GET", f"{J}/{B}") \
    .r("DELETE", f"{J}/{B}", [("ifMetagenerationMatch", "1")]).r("DELETE", f"{J}/{B}", [("ifMetagenerationMatch", "2")])

K("bucket_versioning").mkb().js("PATCH", f"{J}/{B}", {"versioning": {"enabled": True}}).r("GET", f"{J}/{B}") \
    .js("PATCH", f"{J}/{B}", {"versioning": {"enabled": False}}).r("GET", f"{J}/{B}")

K("bucket_settings_stored").mkb().js("PATCH", f"{J}/{B}", {
    "cors": [{"origin": ["*"], "method": ["GET"], "maxAgeSeconds": 3600}],
    "lifecycle": {"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30}}]},
    "retentionPolicy": {"retentionPeriod": "60"}, "iamConfiguration": {"uniformBucketLevelAccess": {"enabled": True}}}) \
    .r("GET", f"{J}/{B}")

K("bucket_list").mkb().mkb("bl2@R@").mkb("bl3@R@").r("GET", J, P + [("prefix", "bl")]) \
    .r("GET", J, P + [("prefix", "bl"), ("maxResults", "2")]).r("GET", J, P + [("prefix", "zz@R@")]) \
    .r("DELETE", f"{J}/bl2@R@").r("DELETE", f"{J}/bl3@R@")

# ------------------------------------------------------------------------------------------------ object upload / read

K("obj_media_roundtrip").mkb().up("a/x.txt", b"hello world", "text/plain") \
    .get("a/x.txt").media("a/x.txt").r("HEAD", f"{J}/{B}/o/{on('a/x.txt')}", [("alt", "media")]) \
    .media("a/x.txt", h={"Range": "bytes=0-4"}).media("a/x.txt", h={"Range": "bytes=6-"}) \
    .media("a/x.txt", h={"Range": "bytes=-5"}).media("a/x.txt", h={"Range": "bytes=50-60"}) \
    .media("a/x.txt", h={"Range": "bytes=2-1000"}).media("nope").get("nope") \
    .r("GET", f"/download/storage/v1/b/{B}/o/{on('a/x.txt')}", [("alt", "media")])

K("obj_media_empty_and_binary").mkb().up("empty", b"", "application/octet-stream").get("empty").media("empty") \
    .up("bin", bytes(range(256)) * 40, "application/octet-stream").get("bin").media("bin", raw=True) \
    .up("noctype", b"abc", "").get("noctype")

K("obj_media_1mib").mkb().up("big", blob(1024 * KIB), "application/octet-stream").get("big") \
    .media("big", hash=True).media("big", h={"Range": "bytes=1000-2000000"}, hash=True) \
    .media("big", h={"Range": "bytes=-100"}, hash=True)

K("obj_name_validation").mkb().up("", b"x").r("POST", f"{U}/{B}/o", [("uploadType", "media")], {}, b"x") \
    .up(".", b"x").up("..", b"x").up("a\nb", b"x").r("POST", f"{U}/{B}/o", [("uploadType", "bogus"), ("name", "x")], {}, b"x") \
    .r("POST", f"{U}/{B}/o", [("name", "x")], {}, b"x")

K("obj_names_special").mkb().up("with space.txt").up("uni-é-日本.txt").up("plus+sign").up("a/b/c/d.txt").up("percent%20name") \
    .up("q?mark").up("hash#tag").ls().get("with space.txt").get("uni-é-日本.txt").get("plus+sign").get("percent%20name") \
    .get("q?mark").get("hash#tag").get("a/b/c/d.txt")

K("obj_upload_no_bucket").up("x", bucket="nosuch@R@").get("x", bucket="nosuch@R@").ls(bucket="nosuch@R@")

_h, _b = mp_related({"name": "mp/one.txt", "contentType": "text/csv", "metadata": {"k1": "v1", "k2": "v2"},
                     "cacheControl": "no-cache", "contentDisposition": "attachment; filename=a.csv",
                     "contentLanguage": "en"}, b"a,b,c\n1,2,3\n", "text/csv")
K("obj_multipart_upload").mkb().r("POST", f"{U}/{B}/o", [("uploadType", "multipart")], _h, _b).get("mp/one.txt") \
    .media("mp/one.txt")
_h2, _b2 = mp_related({"contentType": "text/plain"}, b"noname")
K("obj_multipart_name_in_query").mkb().r("POST", f"{U}/{B}/o", [("uploadType", "multipart"), ("name", "qn.txt")], _h2, _b2) \
    .get("qn.txt").r("POST", f"{U}/{B}/o", [("uploadType", "multipart")], _h2, _b2)
_h3, _b3 = mp_related({"name": "hash.txt", "crc32c": "AAAAAA=="}, b"hello")
K("obj_hash_validation").mkb().r("POST", f"{U}/{B}/o", [("uploadType", "multipart")], _h3, _b3).get("hash.txt") \
    .up("crc.txt", b"hello", h={"X-Goog-Hash": "crc32c=mnG7TA=="}).get("crc.txt") \
    .up("crc2.txt", b"hello", h={"X-Goog-Hash": "crc32c=AAAAAA=="}).get("crc2.txt")

K("obj_content_encoding").mkb().up("z.txt", b"plain", "text/plain", q=[("contentEncoding", "identity")]).get("z.txt")

# ------------------------------------------------------------------------------------------------ resumable

K("resumable_single_request").mkb() \
    .js("POST", f"{U}/{B}/o", {"contentType": "text/plain", "metadata": {"m": "1"}}, q=[("uploadType", "resumable"), ("name", "rs.txt")],
        cap={"loc": "h:Location"}) \
    .r("PUT", "<<loc>>", body=b"resumable body", known_diff=True).get("rs.txt").media("rs.txt")

K("resumable_chunked").mkb() \
    .js("POST", f"{U}/{B}/o", {"contentType": "application/octet-stream", "metadata": {"m": "1"}},
        q=[("uploadType", "resumable"), ("name", "rc.bin")], h={"X-Upload-Content-Length": str(3 * CHUNK + 100)},
        cap={"loc": "h:Location"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": "bytes */*"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes 0-{CHUNK - 1}/*"}, body=lambda: blob(CHUNK, 1)) \
    .r("PUT", "<<loc>>", h={"Content-Range": "bytes */*"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes {CHUNK}-{2 * CHUNK - 1}/*"}, body=lambda: blob(CHUNK, 2)) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes {2 * CHUNK}-{3 * CHUNK + 99}/{3 * CHUNK + 100}"},
       body=lambda: blob(CHUNK + 100, 3)) \
    .get("rc.bin").media("rc.bin", hash=True) \
    .r("PUT", "<<loc>>", h={"Content-Range": "bytes */*"})

K("resumable_query_and_resume").mkb() \
    .js("POST", f"{U}/{B}/o", {}, q=[("uploadType", "resumable"), ("name", "rq.bin")], cap={"loc": "h:Location"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes 0-{CHUNK - 1}/*"}, body=lambda: blob(CHUNK, 4)) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes */{CHUNK + 5}"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes {CHUNK}-{CHUNK + 4}/{CHUNK + 5}"}, body=b"12345") \
    .get("rq.bin")

K("resumable_cancel").mkb() \
    .js("POST", f"{U}/{B}/o", {}, q=[("uploadType", "resumable"), ("name", "rx.bin")], cap={"loc": "h:Location"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes 0-{CHUNK - 1}/*"}, body=lambda: blob(CHUNK, 5)) \
    .r("DELETE", "<<loc>>", known_diff=True).r("PUT", "<<loc>>", h={"Content-Range": "bytes */*"}, known_diff=True).get("rx.bin")

K("resumable_errors").mkb() \
    .js("POST", f"{U}/{B}/o", {}, q=[("uploadType", "resumable"), ("name", "re.bin")], cap={"loc": "h:Location"}) \
    .r("PUT", "<<loc>>", h={"Content-Range": f"bytes {CHUNK}-{2 * CHUNK - 1}/*"}, body=lambda: blob(CHUNK, 6)) \
    .r("PUT", "<<loc>>", h={"Content-Range": "bytes garbage"}, body=b"x") \
    .r("PUT", "<<loc>>", h={"Content-Range": "bytes 0-9/10"}, body=b"short") \
    .r("PUT", f"{U}/{B}/o", [("uploadType", "resumable"), ("upload_id", "does-not-exist")], {"Content-Range": "bytes */*"}) \
    .js("POST", f"{U}/nosuch@R@/o", {}, q=[("uploadType", "resumable"), ("name", "x")])

K("resumable_preconditions").mkb().up("exists.txt") \
    .js("POST", f"{U}/{B}/o", {}, q=[("uploadType", "resumable"), ("name", "exists.txt"), ("ifGenerationMatch", "0")]) \
    .js("POST", f"{U}/{B}/o", {}, q=[("uploadType", "resumable"), ("name", "fresh.txt"), ("ifGenerationMatch", "0")],
        cap={"loc": "h:Location"}).r("PUT", "<<loc>>", body=b"ok", known_diff=True).get("fresh.txt")

# ------------------------------------------------------------------------------------------------ metadata / preconditions

K("obj_patch_update").mkb().up("m.txt", b"meta", "text/plain", cap={"g": "j:generation"}).get("m.txt") \
    .js("PATCH", f"{J}/{B}/o/m.txt", {"contentType": "text/csv", "metadata": {"a": "1", "b": "2"}, "cacheControl": "no-store"}) \
    .get("m.txt").js("PATCH", f"{J}/{B}/o/m.txt", {"metadata": {"a": None, "c": "3"}, "contentLanguage": "fr"}) \
    .get("m.txt").js("PUT", f"{J}/{B}/o/m.txt", {"contentType": "text/plain", "metadata": {"only": "one"}}) \
    .get("m.txt").js("PATCH", f"{J}/{B}/o/nope.txt", {"contentType": "x/y"}).media("m.txt")

K("obj_preconditions").mkb().up("p.txt", q=[("ifGenerationMatch", "0")], cap={"g": "j:generation"}) \
    .up("p.txt", b"again", q=[("ifGenerationMatch", "0")]).up("p.txt", b"wrong", q=[("ifGenerationMatch", "1")]) \
    .up("p.txt", b"right", q=[("ifGenerationMatch", "<<g>>")], cap={"g2": "j:generation"}) \
    .up("p.txt", b"nm", q=[("ifGenerationNotMatch", "<<g2>>")]) \
    .js("PATCH", f"{J}/{B}/o/p.txt", {"contentType": "a/b"}, q=[("ifMetagenerationMatch", "5")]) \
    .js("PATCH", f"{J}/{B}/o/p.txt", {"contentType": "a/b"}, q=[("ifMetagenerationMatch", "1")]) \
    .js("PATCH", f"{J}/{B}/o/p.txt", {"contentType": "a/c"}, q=[("ifMetagenerationMatch", "1")]) \
    .rm("p.txt", q=[("ifGenerationMatch", "1")]).rm("p.txt", q=[("ifGenerationMatch", "<<g2>>")]) \
    .rm("p.txt").up("q.txt", b"x", q=[("ifGenerationMatch", "5")]).up("r.txt", b"x", q=[("ifGenerationNotMatch", "0")])

K("obj_get_preconditions").mkb().up("g.txt", cap={"g": "j:generation"}) \
    .get("g.txt", q=[("ifGenerationMatch", "<<g>>")]).get("g.txt", q=[("ifGenerationMatch", "1")]) \
    .get("g.txt", q=[("ifMetagenerationMatch", "1")]).get("g.txt", q=[("ifMetagenerationMatch", "2")]) \
    .get("g.txt", q=[("ifGenerationNotMatch", "<<g>>")]).media("g.txt", q=[("ifGenerationMatch", "1")])

K("obj_projection_fields").mkb().up("f.txt", b"abc").get("f.txt", q=[("fields", "name,size")]) \
    .get("f.txt", q=[("fields", "name,metadata/x")]).ls(q=[("fields", "items(name,size)")]) \
    .ls(q=[("fields", "kind")]).get("f.txt", q=[("alt", "bogus")])

# ------------------------------------------------------------------------------------------------ versioning

K("obj_versioning").mkb(versioning={"enabled": True}) \
    .up("v.txt", b"one", cap={"g1": "j:generation"}).up("v.txt", b"two", cap={"g2": "j:generation"}) \
    .up("v.txt", b"three", cap={"g3": "j:generation"}).ls(q=[("versions", "true")]).ls() \
    .media("v.txt").media("v.txt", q=[("generation", "<<g1>>")]).get("v.txt", q=[("generation", "<<g2>>")]) \
    .rm("v.txt").get("v.txt").media("v.txt", q=[("generation", "<<g3>>")]).ls(q=[("versions", "true")]).ls() \
    .rm("v.txt", q=[("generation", "<<g1>>")]).media("v.txt", q=[("generation", "<<g1>>")]).ls(q=[("versions", "true")]) \
    .up("v.txt", b"four").media("v.txt").rm("v.txt", q=[("generation", "999")])

K("obj_unversioned_overwrite").mkb().up("o.txt", b"one", cap={"g1": "j:generation"}).up("o.txt", b"two", cap={"g2": "j:generation"}) \
    .ls(q=[("versions", "true")]).media("o.txt").media("o.txt", q=[("generation", "<<g1>>")]) \
    .get("o.txt", q=[("generation", "<<g2>>")])

# ------------------------------------------------------------------------------------------------ listing

_names = ["a/1", "a/2", "a/sub/3", "b/1", "c", "d/e/f", "d/e/g", "top1", "top2", "z"]


def _listing(name):
    k = K(name).mkb()
    for n in _names:
        k.up(n, n.encode())
    return k


_listing("list_prefix_delimiter").ls() \
    .ls(q=[("delimiter", "/")]).ls(q=[("prefix", "a/"), ("delimiter", "/")]).ls(q=[("prefix", "a/")]) \
    .ls(q=[("prefix", "d/"), ("delimiter", "/")]).ls(q=[("prefix", "d/e/"), ("delimiter", "/")]) \
    .ls(q=[("prefix", "nothing")]).ls(q=[("delimiter", "e")]).ls(q=[("prefix", "a"), ("delimiter", "/")])

_listing("list_paging").ls(q=[("maxResults", "3")]).ls(q=[("maxResults", "3"), ("delimiter", "/")]) \
    .ls(q=[("maxResults", "1"), ("prefix", "a/")]).ls(q=[("maxResults", "0")]).ls(q=[("maxResults", "-1")]) \
    .ls(q=[("maxResults", "abc")]).ls(q=[("pageToken", "!!!bad")])

_listing("list_offsets").ls(q=[("startOffset", "b")]).ls(q=[("endOffset", "c")]).ls(q=[("startOffset", "a/2"), ("endOffset", "b/1")]) \
    .ls(q=[("startOffset", "zzz")]).ls(q=[("startOffset", "b"), ("endOffset", "b")])

_listing("list_options").ls(q=[("prefix", "d/"), ("delimiter", "/"), ("includeTrailingDelimiter", "true")]) \
    .ls(q=[("matchGlob", "a/*")]).ls(q=[("matchGlob", "**/1")]).ls(q=[("matchGlob", "top?")]).ls(q=[("matchGlob", "{c,z}")]) \
    .ls(q=[("prefix", "a/"), ("matchGlob", "a/[12]")])

# ------------------------------------------------------------------------------------------------ copy / rewrite / compose

K("obj_copy").mkb().mkb("cp2@R@").up("src.txt", b"copy me", "text/plain") \
    .js("PATCH", f"{J}/{B}/o/src.txt", {"metadata": {"k": "v"}, "cacheControl": "no-cache"}) \
    .r("POST", f"{J}/{B}/o/src.txt/copyTo/b/{B}/o/dst.txt").get("dst.txt").media("dst.txt") \
    .r("POST", f"{J}/{B}/o/src.txt/copyTo/b/cp2@R@/o/{on('other/dst.txt')}").get("other/dst.txt", bucket="cp2@R@") \
    .js("POST", f"{J}/{B}/o/src.txt/copyTo/b/{B}/o/repl.txt", {"contentType": "x/replaced", "metadata": {"only": "this"}}) \
    .get("repl.txt").r("POST", f"{J}/{B}/o/nosrc/copyTo/b/{B}/o/x").r("POST", f"{J}/{B}/o/src.txt/copyTo/b/nob@R@/o/x") \
    .r("POST", f"{J}/{B}/o/src.txt/copyTo/b/{B}/o/dst.txt", [("ifGenerationMatch", "0")]) \
    .r("POST", f"{J}/{B}/o/src.txt/copyTo/b/{B}/o/src.txt").ls(q=[("versions", "true")])

K("obj_rewrite").mkb().up("s.txt", b"rewrite me", "text/plain") \
    .r("POST", f"{J}/{B}/o/s.txt/rewriteTo/b/{B}/o/t.txt").get("t.txt").media("t.txt") \
    .js("POST", f"{J}/{B}/o/s.txt/rewriteTo/b/{B}/o/u.txt", {"contentType": "text/csv"}).get("u.txt") \
    .r("POST", f"{J}/{B}/o/s.txt/rewriteTo/b/{B}/o/t.txt", [("ifGenerationMatch", "0")]) \
    .r("POST", f"{J}/{B}/o/none/rewriteTo/b/{B}/o/v.txt")

K("obj_compose").mkb().up("c1", b"AAA").up("c2", b"BBB").up("c3", b"CCC") \
    .js("POST", f"{J}/{B}/o/all/compose", {"sourceObjects": [{"name": "c1"}, {"name": "c2"}, {"name": "c3"}],
                                            "destination": {"contentType": "text/plain", "metadata": {"m": "1"}}}) \
    .get("all").media("all").media("all", h={"Range": "bytes=2-6"}) \
    .js("POST", f"{J}/{B}/o/all2/compose", {"sourceObjects": [{"name": "all"}, {"name": "c1"}], "destination": {}}) \
    .get("all2").media("all2") \
    .js("POST", f"{J}/{B}/o/bad/compose", {"sourceObjects": [{"name": "c1"}, {"name": "missing"}], "destination": {}}) \
    .js("POST", f"{J}/{B}/o/bad/compose", {"sourceObjects": [], "destination": {}}) \
    .js("POST", f"{J}/{B}/o/bad/compose", {"sourceObjects": [{"name": "c1"}] * 33, "destination": {}}) \
    .js("POST", f"{J}/{B}/o/all/compose", {"sourceObjects": [{"name": "c1"}], "destination": {}}, q=[("ifGenerationMatch", "0")]) \
    .js("POST", f"{J}/{B}/o/x/compose", {"sourceObjects": [{"name": "c1", "generation": "1"}], "destination": {}}) \
    .get("bad")

K("obj_compose_32").mkb().up("s", b"x") \
    .js("POST", f"{J}/{B}/o/c32/compose", {"sourceObjects": [{"name": "s"}] * 32, "destination": {"contentType": "text/plain"}}) \
    .get("c32").media("c32")

# ------------------------------------------------------------------------------------------------ delete

K("obj_delete").mkb().up("d.txt").rm("d.txt").rm("d.txt").get("d.txt").media("d.txt").ls() \
    .up("d.txt", b"again").media("d.txt")

# ------------------------------------------------------------------------------------------------ ACLs (stored)

K("acl_bucket_object").mkb().up("acl.txt") \
    .js("POST", f"{J}/{B}/acl", {"entity": "allUsers", "role": "READER"}).r("GET", f"{J}/{B}/acl/allUsers") \
    .r("DELETE", f"{J}/{B}/acl/allUsers").r("GET", f"{J}/{B}/acl/allUsers") \
    .js("POST", f"{J}/{B}/o/acl.txt/acl", {"entity": "allUsers", "role": "READER"}).r("GET", f"{J}/{B}/o/acl.txt/acl/allUsers") \
    .r("DELETE", f"{J}/{B}/o/acl.txt/acl/allUsers").r("GET", f"{J}/{B}/o/acl.txt/acl/allUsers") \
    .up("pre.txt", q=[("predefinedAcl", "publicRead")]).up("bad.txt", q=[("predefinedAcl", "nonsense")])

# ------------------------------------------------------------------------------------------------ misc / errors

K("errors_misc").mkb().r("GET", "/storage/v1/nothing").r("POST", f"{J}/{B}").r("GET", f"{J}/{B}/nothing") \
    .r("PUT", f"{J}/{B}/o").r("PATCH", J).js("PATCH", f"{J}/{B}", {"storageClass": "BOGUS"})

K("xml_get_object").mkb().up("x/y.txt", b"xml read", "text/plain", h={}) \
    .r("GET", f"/{B}/x/y.txt", known_diff=True).r("HEAD", f"/{B}/x/y.txt", known_diff=True).r("GET", f"/{B}/none.txt") \
    .r("GET", f"/{B}/x/y.txt", h={"Range": "bytes=0-2"}, known_diff=True)

K("xml_list_objects").mkb().up("a/1").up("a/2").up("b") \
    .r("GET", f"/{B}", known_diff=True).r("GET", f"/{B}", [("list-type", "2"), ("delimiter", "/")], known_diff=True) \
    .r("GET", f"/{B}", [("prefix", "a/")], known_diff=True)

"""The differential corpus: named cases, each a list of REST steps replayed identically against Azurite and Warp.

Templates: @R@ = per-run token (letters), <<name>> = a value captured by an earlier step (cap={"name": "h:Header" |
"x:regex" | "j:json.path"}). Steps: svc blob|queue|table, method, path (below the account), query [(k, v)], headers, body,
auth key|lite|badkey|sas|anon|bearer, sas={...}, known_diff=True (only the status must match).
"""
import base64
import json

CASES = {}
CN = "/c@R@"
XML = {"Content-Type": "application/xml"}


class K:
    def __init__(self, name):
        self.steps = []
        CASES[name] = self.steps

    def _s(self, svc, method, path="", q=None, h=None, body=b"", **kw):
        self.steps.append(dict(svc=svc, method=method, path=path, query=list(q or []), headers=dict(h or {}), body=body, **kw))
        return self

    def blob(self, *a, **k):
        return self._s("blob", *a, **k)

    def queue(self, *a, **k):
        return self._s("queue", *a, **k)

    def table(self, *a, **k):
        return self._s("table", *a, **k)


def mk(name):
    return K(name).blob("PUT", CN, [("restype", "container")])


BT = {"x-ms-blob-type": "BlockBlob"}
CONT = [("restype", "container")]


def blocklist(*ids, kind="Latest"):
    return "<?xml version='1.0' encoding='utf-8'?><BlockList>" + "".join(f"<{kind}>{i}</{kind}>" for i in ids) + "</BlockList>"


def bid(s):
    return base64.b64encode(s.encode()).decode()



DEFAULT_PROPS = ("<StorageServiceProperties><Logging><Version>1.0</Version><Delete>true</Delete><Read>true</Read><Write>true</Write>"
                 "<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></Logging><HourMetrics><Version>1.0</Version><Enabled>false</Enabled>"
                 "<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></HourMetrics><MinuteMetrics><Version>1.0</Version><Enabled>false</Enabled>"
                 "<RetentionPolicy><Enabled>false</Enabled></RetentionPolicy></MinuteMetrics><Cors/></StorageServiceProperties>")
# ============================================================================================ BLOB: containers
k = K("blob_container_lifecycle")
k.blob("PUT", CN, CONT, {"x-ms-meta-a": "1", "x-ms-meta-B": "two"})
k.blob("PUT", CN, CONT)
k.blob("PUT", "/UPPER@R@", CONT)
k.blob("PUT", "/ab", CONT)
k.blob("GET", CN, CONT)
k.blob("HEAD", CN, CONT)
k.blob("GET", CN, CONT + [("comp", "metadata")])
k.blob("PUT", CN, CONT + [("comp", "metadata")], {"x-ms-meta-c": "3"})
k.blob("GET", CN, CONT + [("comp", "metadata")])
k.blob("PUT", CN, CONT + [("comp", "metadata")], {"x-ms-meta-1bad": "3"})
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@"), ("include", "metadata")])
k.blob("DELETE", CN, CONT)
k.blob("GET", CN, CONT)
k.blob("DELETE", CN, CONT)
k.blob("PUT", CN, CONT, {"x-ms-blob-public-access": "bogus"})

k = K("blob_container_list_paging")
for n in "abc":
    k.blob("PUT", f"/c@R@{n}", CONT)
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@"), ("maxresults", "2")], cap={"m": "x:<NextMarker>(.*?)</NextMarker>"})
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@"), ("maxresults", "2"), ("marker", "<<m>>")])
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@"), ("maxresults", "0")])
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@"), ("maxresults", "abc")])

k = mk("blob_container_acl_public")
k.blob("PUT", CN + "/pub", [], {**BT, "Content-Type": "text/plain"}, "hello")
k.blob("GET", CN + "/pub", [], auth="anon")
k.blob("GET", CN, CONT + [("comp", "list")], auth="anon")
k.blob("PUT", CN, CONT + [("comp", "acl")], {"x-ms-blob-public-access": "blob", **XML},
       "<?xml version=\"1.0\" encoding=\"utf-8\"?><SignedIdentifiers><SignedIdentifier><Id>pol1</Id><AccessPolicy>"
       "<Start>2020-01-01T00:00:00.0000000Z</Start><Expiry>2099-01-01T00:00:00.0000000Z</Expiry><Permission>rl</Permission>"
       "</AccessPolicy></SignedIdentifier></SignedIdentifiers>")
k.blob("GET", CN, CONT + [("comp", "acl")])
k.blob("GET", CN + "/pub", [], auth="anon")
k.blob("HEAD", CN + "/pub", [], auth="anon")
k.blob("GET", CN, CONT + [("comp", "list")], auth="anon")
k.blob("PUT", CN, CONT + [("comp", "acl")], {"x-ms-blob-public-access": "container"}, "")
k.blob("GET", CN, CONT + [("comp", "list")], auth="anon")
k.blob("GET", CN, CONT + [("comp", "acl")], auth="anon")

k = mk("blob_container_lease")
L = {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "-1", "x-ms-proposed-lease-id": "11111111-1111-1111-1111-111111111111"}
k.blob("PUT", CN, CONT + [("comp", "lease")], L)
k.blob("PUT", CN, CONT + [("comp", "lease")], {**L, "x-ms-proposed-lease-id": "22222222-2222-2222-2222-222222222222"})
k.blob("HEAD", CN, CONT)
k.blob("DELETE", CN, CONT)
k.blob("DELETE", CN, CONT, {"x-ms-lease-id": "33333333-3333-3333-3333-333333333333"})
k.blob("PUT", CN, CONT + [("comp", "metadata")], {"x-ms-meta-a": "b"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "renew", "x-ms-lease-id": "11111111-1111-1111-1111-111111111111"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "change", "x-ms-lease-id": "11111111-1111-1111-1111-111111111111",
                                                "x-ms-proposed-lease-id": "44444444-4444-4444-4444-444444444444"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "break", "x-ms-lease-break-period": "0"})
k.blob("HEAD", CN, CONT)
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "release", "x-ms-lease-id": "44444444-4444-4444-4444-444444444444"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "release", "x-ms-lease-id": "44444444-4444-4444-4444-444444444444"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "5"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "15"}, cap={"lid": "h:x-ms-lease-id"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {"x-ms-lease-action": "bogus"})
k.blob("PUT", CN, CONT + [("comp", "lease")], {})
k.blob("DELETE", CN, CONT, {"x-ms-lease-id": "<<lid>>"})

# ============================================================================================ BLOB: block blobs
k = mk("blob_put_get_basic")
k.blob("PUT", CN + "/b 1.txt", [], {**BT, "Content-Type": "text/plain", "x-ms-meta-k": "v", "Cache-Control": "no-cache",
       "Content-Disposition": "attachment", "Content-Encoding": "identity", "Content-Language": "en"}, "hello world")
k.blob("GET", CN + "/b 1.txt")
k.blob("HEAD", CN + "/b 1.txt")
k.blob("PUT", CN + "/dir/sub/é.bin", [], BT, b"\x00\x01\x02")
k.blob("GET", CN + "/dir/sub/é.bin", raw_body=True)
k.blob("PUT", CN + "/empty", [], BT, "")
k.blob("GET", CN + "/empty")
k.blob("GET", CN + "/missing")
k.blob("HEAD", CN + "/missing")
k.blob("GET", "/nosuch@R@/x")
k.blob("PUT", CN + "/notype", [], {}, "x")
k.blob("PUT", CN + "/badtype", [], {"x-ms-blob-type": "Nope"}, "x")
k.blob("PUT", "/nocontainer@R@/x", [], BT, "x")
k.blob("PUT", CN + "/md5", [], {**BT, "Content-MD5": base64.b64encode(b"0123456789abcdef").decode()}, "data")
k.blob("PUT", CN + "/md5ok", [], {**BT, "Content-MD5": "gdyb21LQTcIANtvYMT7QVQ=="}, "data")
k.blob("PUT", CN + "/x", [], {**BT, "x-ms-meta-bad-name": "v"}, "x")

k = mk("blob_get_ranges")
k.blob("PUT", CN + "/r", [], BT, "0123456789")
for rg in ("bytes=0-4", "bytes=2-", "bytes=-3", "bytes=5-100", "bytes=10-12", "bytes=9-9", "bytes=4-2", "items=0-1", "bytes=0-1,3-4"):
    k.blob("GET", CN + "/r", [], {"Range": rg})
k.blob("GET", CN + "/r", [], {"x-ms-range": "bytes=1-2", "Range": "bytes=5-6"})
k.blob("GET", CN + "/r", [], {"Range": "bytes=0-3", "x-ms-range-get-content-md5": "true"})
k.blob("HEAD", CN + "/r", [], {"Range": "bytes=0-3"})

k = mk("blob_conditions")
k.blob("PUT", CN + "/c", [], BT, "abc", cap={"e": "h:ETag"})
k.blob("GET", CN + "/c", [], {"If-Match": "<<e>>"})
k.blob("GET", CN + "/c", [], {"If-Match": '"0xDEAD"'})
k.blob("GET", CN + "/c", [], {"If-None-Match": "<<e>>"})
k.blob("GET", CN + "/c", [], {"If-None-Match": '"0xDEAD"'})
k.blob("GET", CN + "/c", [], {"If-Modified-Since": "Wed, 01 Jan 2099 00:00:00 GMT"})
k.blob("GET", CN + "/c", [], {"If-Modified-Since": "Wed, 01 Jan 2020 00:00:00 GMT"})
k.blob("GET", CN + "/c", [], {"If-Unmodified-Since": "Wed, 01 Jan 2020 00:00:00 GMT"})
k.blob("GET", CN + "/c", [], {"If-Unmodified-Since": "Wed, 01 Jan 2099 00:00:00 GMT"})
k.blob("PUT", CN + "/c", [], {**BT, "If-None-Match": "*"}, "again")
k.blob("PUT", CN + "/c", [], {**BT, "If-Match": '"0xDEAD"'}, "again")
k.blob("PUT", CN + "/c", [], {**BT, "If-Match": "<<e>>"}, "again")
k.blob("PUT", CN + "/new", [], {**BT, "If-None-Match": "*"}, "new")
k.blob("PUT", CN + "/nonexist", [], {**BT, "If-Match": "<<e>>"}, "x")
k.blob("GET", CN + "/c", [], {"If-Modified-Since": "garbage"})

k = mk("blob_properties_metadata")
k.blob("PUT", CN + "/p", [], {**BT, "Content-Type": "text/plain", "x-ms-meta-a": "1"}, "abc")
k.blob("PUT", CN + "/p", [("comp", "properties")], {"x-ms-blob-content-type": "application/json", "x-ms-blob-cache-control": "max-age=1"})
k.blob("HEAD", CN + "/p")
k.blob("PUT", CN + "/p", [("comp", "metadata")], {"x-ms-meta-z": "26"})
k.blob("HEAD", CN + "/p")
k.blob("GET", CN + "/p", [("comp", "metadata")])
k.blob("PUT", CN + "/p", [("comp", "metadata")], {})
k.blob("HEAD", CN + "/p")
k.blob("PUT", CN + "/missing", [("comp", "metadata")], {"x-ms-meta-z": "26"})
k.blob("PUT", CN + "/p", [("comp", "tier")], {"x-ms-access-tier": "Cool"})
k.blob("HEAD", CN + "/p")
k.blob("PUT", CN + "/p", [("comp", "tier")], {"x-ms-access-tier": "Nope"})
k.blob("PUT", CN + "/p", [("comp", "bogus")], {})

k = mk("blob_blocks")
k.blob("PUT", CN + "/bl", [("comp", "block"), ("blockid", bid("blk-0001"))], {}, "AAAA")
k.blob("PUT", CN + "/bl", [("comp", "block"), ("blockid", bid("blk-0002"))], {}, "BBBB")
k.blob("PUT", CN + "/bl", [("comp", "block"), ("blockid", bid("blk-0003"))], {}, "CCCC")
k.blob("PUT", CN + "/bl", [("comp", "block"), ("blockid", bid("short"))], {}, "DDDD")
k.blob("PUT", CN + "/bl", [("comp", "block"), ("blockid", "not base64!!")], {}, "DDDD")
k.blob("PUT", CN + "/bl", [("comp", "block")], {}, "DDDD")
k.blob("GET", CN + "/bl", [("comp", "blocklist"), ("blocklisttype", "all")])
k.blob("GET", CN + "/bl", [("comp", "blocklist"), ("blocklisttype", "committed")])
k.blob("GET", CN + "/bl")
k.blob("PUT", CN + "/bl", [("comp", "blocklist")], {**XML, "x-ms-blob-content-type": "text/x", "x-ms-meta-m": "1"},
       blocklist(bid("blk-0001"), bid("blk-0003"), bid("blk-0001")))
k.blob("GET", CN + "/bl")
k.blob("HEAD", CN + "/bl")
k.blob("GET", CN + "/bl", [("comp", "blocklist"), ("blocklisttype", "all")])
k.blob("PUT", CN + "/bl", [("comp", "blocklist")], XML, blocklist(bid("blk-0009")))
k.blob("PUT", CN + "/bl", [("comp", "blocklist")], XML, blocklist(bid("blk-0001"), kind="Committed"))
k.blob("GET", CN + "/bl")
k.blob("PUT", CN + "/bl", [("comp", "blocklist")], XML, "<notxml")
k.blob("PUT", CN + "/bl", [("comp", "blocklist")], XML, blocklist(bid("blk-0001"), kind="Uncommitted"))
k.blob("GET", CN + "/bl", [("comp", "blocklist"), ("blocklisttype", "bogus")])
k.blob("GET", CN + "/none", [("comp", "blocklist")])

# ============================================================================================ BLOB: append / page
k = mk("blob_append")
k.blob("PUT", CN + "/ap", [], {"x-ms-blob-type": "AppendBlob", "Content-Type": "text/plain"})
k.blob("PUT", CN + "/ap", [("comp", "appendblock")], {}, "one")
k.blob("PUT", CN + "/ap", [("comp", "appendblock")], {"x-ms-blob-condition-appendpos": "3"}, "two")
k.blob("PUT", CN + "/ap", [("comp", "appendblock")], {"x-ms-blob-condition-appendpos": "3"}, "bad")
k.blob("PUT", CN + "/ap", [("comp", "appendblock")], {"x-ms-blob-condition-maxsize": "7"}, "toolong")
k.blob("GET", CN + "/ap")
k.blob("HEAD", CN + "/ap")
k.blob("PUT", CN + "/ap", [("comp", "seal")])
k.blob("PUT", CN + "/ap", [("comp", "appendblock")], {}, "late")
k.blob("PUT", CN + "/ap", [("comp", "seal")])
k.blob("HEAD", CN + "/ap")
k.blob("PUT", CN + "/blockb", [], BT, "x")
k.blob("PUT", CN + "/blockb", [("comp", "appendblock")], {}, "x")
k.blob("PUT", CN + "/missingap", [("comp", "appendblock")], {}, "x")

k = mk("blob_page")
k.blob("PUT", CN + "/pg", [], {"x-ms-blob-type": "PageBlob", "x-ms-blob-content-length": "2048"})
k.blob("PUT", CN + "/pg2", [], {"x-ms-blob-type": "PageBlob", "x-ms-blob-content-length": "1000"})
k.blob("PUT", CN + "/pg3", [], {"x-ms-blob-type": "PageBlob"})
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "update", "x-ms-range": "bytes=0-511"}, "a" * 512)
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "update", "x-ms-range": "bytes=1024-1535"}, "b" * 512)
k.blob("GET", CN + "/pg", [("comp", "pagelist")])
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "update", "x-ms-range": "bytes=0-510"}, "a" * 511)
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "update", "x-ms-range": "bytes=2048-2559"}, "a" * 512)
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "update", "x-ms-range": "bytes=0-511"}, "a" * 100)
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "clear", "x-ms-range": "bytes=0-511"})
k.blob("GET", CN + "/pg", [("comp", "pagelist")])
k.blob("GET", CN + "/pg", [], {"Range": "bytes=1020-1030"}, skip_body=False)
k.blob("HEAD", CN + "/pg")
k.blob("PUT", CN + "/pg", [("comp", "properties")], {"x-ms-blob-content-length": "4096"})
k.blob("HEAD", CN + "/pg")
k.blob("PUT", CN + "/pg", [("comp", "properties")], {"x-ms-blob-content-length": "1024"})
k.blob("GET", CN + "/pg", [("comp", "pagelist")])
k.blob("PUT", CN + "/pg", [("comp", "page")], {"x-ms-page-write": "bogus", "x-ms-range": "bytes=0-511"}, "a" * 512)

# ============================================================================================ BLOB: snapshots, tags, copy, leases
k = mk("blob_snapshots")
k.blob("PUT", CN + "/s", [], {**BT, "x-ms-meta-a": "1"}, "v1")
k.blob("PUT", CN + "/s", [("comp", "snapshot")], {}, cap={"s1": "h:x-ms-snapshot"})
k.blob("PUT", CN + "/s", [], BT, "v2-longer")
k.blob("PUT", CN + "/s", [("comp", "snapshot")], {"x-ms-meta-b": "2"}, cap={"s2": "h:x-ms-snapshot"})
k.blob("GET", CN + "/s", [("snapshot", "<<s1>>")])
k.blob("GET", CN + "/s", [("snapshot", "<<s2>>")])
k.blob("HEAD", CN + "/s", [("snapshot", "<<s2>>")])
k.blob("GET", CN + "/s", [("snapshot", "2020-01-01T00:00:00.0000000Z")])
k.blob("GET", CN + "/s", [("snapshot", "garbage")])
k.blob("GET", CN, CONT + [("comp", "list"), ("include", "snapshots,metadata")])
k.blob("GET", CN, CONT + [("comp", "list")])
k.blob("DELETE", CN + "/s")
k.blob("DELETE", CN + "/s", [], {"x-ms-delete-snapshots": "bogus"})
k.blob("DELETE", CN + "/s", [("snapshot", "<<s1>>")])
k.blob("DELETE", CN + "/s", [], {"x-ms-delete-snapshots": "only"})
k.blob("GET", CN, CONT + [("comp", "list"), ("include", "snapshots")])
k.blob("PUT", CN + "/s", [("comp", "snapshot")], {}, cap={"s3": "h:x-ms-snapshot"})
k.blob("DELETE", CN + "/s", [], {"x-ms-delete-snapshots": "include"})
k.blob("GET", CN + "/s")
k.blob("PUT", CN + "/none", [("comp", "snapshot")])

k = mk("blob_tags")
k.blob("PUT", CN + "/t1", [], {**BT, "x-ms-tags": "env=prod"}, "a")
k.blob("PUT", CN + "/t2", [], {**BT, "x-ms-tags": "env=dev"}, "b")
k.blob("PUT", CN + "/t3", [], BT, "c")
k.blob("GET", CN + "/t1", [("comp", "tags")])
k.blob("HEAD", CN + "/t1")
k.blob("PUT", CN + "/t3", [("comp", "tags")], XML, "<Tags><TagSet><Tag><Key>k</Key><Value>v</Value></Tag><Tag><Key>z</Key><Value>1</Value></Tag></TagSet></Tags>")
k.blob("GET", CN + "/t3", [("comp", "tags")])
k.blob("PUT", CN + "/t3", [("comp", "tags")], XML, "<Tags><TagSet><Tag><Key>bad$</Key><Value>v</Value></Tag></TagSet></Tags>")
k.blob("GET", CN + "/t1", [("comp", "tags")], {"x-ms-if-tags": "\"env\" = 'prod'"})
k.blob("GET", CN + "/t1", [("comp", "tags")], {"x-ms-if-tags": "\"env\" = 'dev'"})
k.blob("GET", "", [("comp", "blobs"), ("where", "\"env\"='prod' AND @container='c@R@'")])
k.blob("GET", "", [("comp", "blobs"), ("where", "\"env\"='prod' AND @container='c@R@'")])
k.blob("GET", CN, [("comp", "blobs"), ("where", "\"env\">='d'")], known_diff=True)
k.blob("GET", "", [("comp", "blobs"), ("where", "env=prod")])
k.blob("GET", CN, CONT + [("comp", "list"), ("include", "tags")])
k.blob("PUT", CN + "/t3", [("comp", "tags")], XML, "<Tags><TagSet></TagSet></Tags>")
k.blob("GET", CN + "/t3", [("comp", "tags")])

k = mk("blob_copy")
k.blob("PUT", CN + "/src", [], {**BT, "Content-Type": "text/plain", "x-ms-meta-a": "1"}, "copy me")
k.blob("PUT", CN + "/dst", [], {"x-ms-copy-source": "http://localhost/devstoreaccount1/c@R@/src"}, known_diff=True)
k.blob("PUT", CN + "/dst2", [], {"x-ms-copy-source": "http://127.0.0.1:10000/devstoreaccount1/c@R@/src", "x-ms-meta-z": "9"})
k.blob("GET", CN + "/dst2")
k.blob("PUT", CN + "/dst3", [], {"x-ms-copy-source": "http://127.0.0.1:10000/devstoreaccount1/c@R@/nosrc"})
k.blob("PUT", CN + "/dst2", [("comp", "copy"), ("copyid", "abc")], {"x-ms-copy-action": "abort"})

k = mk("blob_leases")
k.blob("PUT", CN + "/l", [], BT, "lease me")
BL = {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "-1", "x-ms-proposed-lease-id": "aaaaaaaa-1111-2222-3333-444444444444"}
k.blob("PUT", CN + "/l", [("comp", "lease")], BL)
k.blob("PUT", CN + "/l", [("comp", "lease")], {**BL, "x-ms-proposed-lease-id": "bbbbbbbb-1111-2222-3333-444444444444"})
k.blob("HEAD", CN + "/l")
k.blob("PUT", CN + "/l", [], BT, "no lease id")
k.blob("PUT", CN + "/l", [], {**BT, "x-ms-lease-id": "cccccccc-1111-2222-3333-444444444444"}, "wrong id")
k.blob("PUT", CN + "/l", [], {**BT, "x-ms-lease-id": "aaaaaaaa-1111-2222-3333-444444444444"}, "right id")
k.blob("PUT", CN + "/l", [("comp", "metadata")], {"x-ms-meta-a": "b"})
k.blob("DELETE", CN + "/l")
k.blob("GET", CN + "/l")
k.blob("PUT", CN + "/l", [("comp", "lease")], {"x-ms-lease-action": "renew", "x-ms-lease-id": "aaaaaaaa-1111-2222-3333-444444444444"})
k.blob("PUT", CN + "/l", [("comp", "lease")], {"x-ms-lease-action": "change", "x-ms-lease-id": "aaaaaaaa-1111-2222-3333-444444444444",
                                                "x-ms-proposed-lease-id": "dddddddd-1111-2222-3333-444444444444"})
k.blob("PUT", CN + "/l", [("comp", "lease")], {"x-ms-lease-action": "break", "x-ms-lease-break-period": "0"})
k.blob("HEAD", CN + "/l")
k.blob("PUT", CN + "/l", [], BT, "after break")
k.blob("PUT", CN + "/l", [("comp", "lease")], {"x-ms-lease-action": "release", "x-ms-lease-id": "dddddddd-1111-2222-3333-444444444444"})
k.blob("PUT", CN + "/l", [("comp", "lease")], {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "15"})
k.blob("PUT", CN + "/l", [("comp", "lease")], {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "99"})
k.blob("PUT", CN + "/missing", [("comp", "lease")], {"x-ms-lease-action": "acquire", "x-ms-lease-duration": "15"})

# ============================================================================================ BLOB: listing
k = mk("blob_list")
for n in ("a", "b/1", "b/2", "b/3/x", "c", "d/e", "prefix1", "prefix2"):
    k.blob("PUT", f"{CN}/{n}", [], BT, n)
LB = CONT + [("comp", "list")]
k.blob("GET", CN, LB)
k.blob("GET", CN, LB + [("delimiter", "/")])
k.blob("GET", CN, LB + [("prefix", "b/"), ("delimiter", "/")])
k.blob("GET", CN, LB + [("prefix", "b/3")])
k.blob("GET", CN, LB + [("prefix", "pre")])
k.blob("GET", CN, LB + [("maxresults", "3")], cap={"m": "x:<NextMarker>(.*?)</NextMarker>"})
k.blob("GET", CN, LB + [("maxresults", "3"), ("marker", "<<m>>")], cap={"m2": "x:<NextMarker>(.*?)</NextMarker>"})
k.blob("GET", CN, LB + [("maxresults", "3"), ("marker", "<<m2>>")])
k.blob("GET", CN, LB + [("delimiter", "/"), ("maxresults", "2")], cap={"m3": "x:<NextMarker>(.*?)</NextMarker>"})
k.blob("GET", CN, LB + [("delimiter", "/"), ("maxresults", "2"), ("marker", "<<m3>>")])
k.blob("GET", CN, LB + [("marker", "garbage!")], known_diff=True)
k.blob("GET", CN, LB + [("include", "bogus")])
k.blob("GET", "/nosuch@R@", LB)

# ============================================================================================ BLOB: service
k = K("blob_service")
k.blob("PUT", "", [("restype", "service"), ("comp", "properties")], XML, DEFAULT_PROPS)
k.blob("GET", "", [("restype", "service"), ("comp", "properties")])
k.blob("PUT", "", [("restype", "service"), ("comp", "properties")], XML,
       "<?xml version=\"1.0\" encoding=\"utf-8\"?><StorageServiceProperties><Cors><CorsRule><AllowedOrigins>http://x.example</AllowedOrigins>"
       "<AllowedMethods>GET,PUT</AllowedMethods><AllowedHeaders>x-ms-*</AllowedHeaders><ExposedHeaders>x-ms-meta-*</ExposedHeaders>"
       "<MaxAgeInSeconds>60</MaxAgeInSeconds></CorsRule></Cors></StorageServiceProperties>")
k.blob("GET", "", [("restype", "service"), ("comp", "properties")])
k.blob("OPTIONS", "/c@R@/b", [], {"Origin": "http://x.example", "Access-Control-Request-Method": "GET"}, auth="anon")
k.blob("OPTIONS", "/c@R@/b", [], {"Origin": "http://evil.example", "Access-Control-Request-Method": "GET"}, auth="anon")
k.blob("GET", "", [("restype", "account"), ("comp", "properties")])
k.blob("GET", "", [("restype", "service"), ("comp", "stats")], known_diff=True)
k.blob("GET", "", [("restype", "service"), ("comp", "bogus")], known_diff=True)
k.blob("PUT", "", [("restype", "service"), ("comp", "properties")], XML, "<StorageServiceProperties><Cors></Cors></StorageServiceProperties>")

k = mk("blob_batch")
for n in ("d1", "d2"):
    k.blob("PUT", f"{CN}/{n}", [], BT, n)
BOUND = "batch_11111111-2222-3333-4444-555555555555"


def batch_body(lines):
    parts = []
    for i, l in enumerate(lines):
        parts.append(f"--{BOUND}\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\nContent-ID: {i}\r\n\r\n{l}\r\n"
                     "x-ms-date: Sat, 01 Jan 2050 00:00:00 GMT\r\nAuthorization: SharedKey x:y\r\nContent-Length: 0\r\n\r\n")
    return "".join(parts) + f"--{BOUND}--\r\n"


k.blob("POST", "", [("comp", "batch")], {"Content-Type": f"multipart/mixed; boundary={BOUND}"},
       batch_body(["DELETE /c@R@/d1 HTTP/1.1", "DELETE /c@R@/d2 HTTP/1.1", "DELETE /c@R@/nope HTTP/1.1"]), known_diff=True)
k.blob("GET", CN, LB)

# ============================================================================================ BLOB: auth
k = mk("blob_auth")
k.blob("PUT", CN + "/a", [], BT, "auth")
k.blob("GET", CN + "/a", [], auth="lite")
k.blob("GET", CN + "/a", [], auth="badkey", known_diff=True)
k.blob("GET", CN + "/a", [], auth="anon")
k.blob("GET", CN + "/a", [], auth="bearer", known_diff=True)
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@")], auth="lite")
k.blob("PUT", CN + "/a2", [("comp", "metadata")], {"x-ms-meta-a": "b"}, auth="lite")
SAS = dict(kind="service", resource="c@R@/a", sp="r")
k.blob("GET", CN + "/a", [], auth="sas", sas=SAS)
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(SAS, sp="w"))
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(SAS, exp=-3600), known_diff=True)
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(SAS, start=3600), known_diff=True)
k.blob("PUT", CN + "/a", [], BT, "w", auth="sas", sas=dict(SAS, sp="w"))
k.blob("PUT", CN + "/a", [], BT, "w", auth="sas", sas=dict(SAS, sp="r"))
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(kind="service", resource="c@R@", sp="r", sr="c"))
k.blob("GET", CN + "/other", [], auth="sas", sas=SAS)
k.blob("GET", CN, LB, auth="sas", sas=dict(kind="service", resource="c@R@", sp="l", sr="c"))
k.blob("GET", CN, LB, auth="sas", sas=dict(kind="service", resource="c@R@", sp="r", sr="c"))
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(kind="account", ss="b", srt="o", sp="r"))
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(kind="account", ss="q", srt="o", sp="r"))
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(kind="account", ss="b", srt="s", sp="r"))
k.blob("GET", "", [("comp", "list"), ("prefix", "c@R@")], auth="sas", sas=dict(kind="account", ss="b", srt="s", sp="l"))
k.blob("PUT", "/d@R@", CONT, auth="sas", sas=dict(kind="account", ss="b", srt="sco", sp="c"))
k.blob("GET", CN + "/a", [], auth="sas", sas=dict(kind="account", ss="b", srt="o", sp="r", spr="https"), known_diff=True)

# ============================================================================================ QUEUE
QN = "/q@R@"
QMSG = QN + "/messages"


def qbody(text):
    return f"<QueueMessage><MessageText>{text}</MessageText></QueueMessage>"


k = K("queue_lifecycle")
k.queue("PUT", QN, [], {"x-ms-meta-a": "1"})
k.queue("PUT", QN, [], {"x-ms-meta-a": "1"})
k.queue("PUT", QN, [], {"x-ms-meta-a": "2"})
k.queue("PUT", "/Bad_Name@R@")
k.queue("PUT", "/ab")
k.queue("PUT", "/a--b@R@")
k.queue("GET", QN, [("comp", "metadata")])
k.queue("HEAD", QN, [("comp", "metadata")])
k.queue("PUT", QN, [("comp", "metadata")], {"x-ms-meta-b": "2"})
k.queue("GET", QN, [("comp", "metadata")])
k.queue("PUT", QN, [("comp", "metadata")], {})
k.queue("GET", QN, [("comp", "metadata")])
k.queue("GET", "", [("comp", "list"), ("prefix", "q@R@"), ("include", "metadata")])
k.queue("DELETE", QN)
k.queue("DELETE", QN)
k.queue("GET", QN, [("comp", "metadata")])
k.queue("PUT", "/nosuch@R@", [("comp", "metadata")], {})

k = K("queue_list_paging")
for n in "abc":
    k.queue("PUT", f"/q@R@{n}")
k.queue("GET", "", [("comp", "list"), ("prefix", "q@R@"), ("maxresults", "2")], cap={"m": "x:<NextMarker>(.*?)</NextMarker>"})
k.queue("GET", "", [("comp", "list"), ("prefix", "q@R@"), ("maxresults", "2"), ("marker", "<<m>>")])
k.queue("GET", "", [("comp", "list"), ("prefix", "q@R@"), ("maxresults", "0")])

k = K("queue_messages")
k.queue("PUT", QN)
k.queue("POST", QMSG, [], {}, qbody("first"))
k.queue("POST", QMSG, [], {}, qbody("second"))
k.queue("POST", QMSG, [], {}, qbody("third"))
k.queue("GET", QN, [("comp", "metadata")])
k.queue("GET", QMSG, [("peekonly", "true")])
k.queue("GET", QMSG, [("peekonly", "true"), ("numofmessages", "2")])
k.queue("GET", QMSG, [("numofmessages", "2"), ("visibilitytimeout", "60")], cap={"id": "x:<MessageId>(.*?)</MessageId>", "pop": "x:<PopReceipt>(.*?)</PopReceipt>"})
k.queue("GET", QMSG, [("peekonly", "true")])
k.queue("GET", QMSG, [("numofmessages", "5")])
k.queue("GET", QMSG, [])
k.queue("PUT", QMSG + "/<<id>>", [("popreceipt", "<<pop>>"), ("visibilitytimeout", "30")], {}, qbody("updated"), cap={"pop2": "h:x-ms-popreceipt"})
k.queue("PUT", QMSG + "/<<id>>", [("popreceipt", "<<pop>>"), ("visibilitytimeout", "30")], {}, qbody("stale receipt"))
k.queue("PUT", QMSG + "/<<id>>", [("popreceipt", "<<pop2>>")], {}, "")
k.queue("PUT", QMSG + "/<<id>>", [("popreceipt", "<<pop2>>"), ("visibilitytimeout", "0")], {}, "")
k.queue("GET", QMSG, [("peekonly", "true"), ("numofmessages", "1")])
k.queue("DELETE", QMSG + "/<<id>>", [("popreceipt", "wrong")])
k.queue("DELETE", QMSG + "/<<id>>", [("popreceipt", "<<pop2>>")])
k.queue("DELETE", QMSG + "/<<id>>", [("popreceipt", "<<pop2>>")])
k.queue("DELETE", QMSG + "/<<id>>")
k.queue("DELETE", QMSG + "/00000000-0000-0000-0000-000000000000", [("popreceipt", "x")])
k.queue("DELETE", QMSG)
k.queue("GET", QMSG, [("peekonly", "true")])
k.queue("GET", QN, [("comp", "metadata")])

k = K("queue_errors_params")
k.queue("PUT", QN)
k.queue("POST", QMSG, [("messagettl", "0")], {}, qbody("x"))
k.queue("POST", QMSG, [("messagettl", "999999999")], {}, qbody("x"))
k.queue("POST", QMSG, [("messagettl", "-1")], {}, qbody("forever"))
k.queue("POST", QMSG, [("messagettl", "10"), ("visibilitytimeout", "10")], {}, qbody("x"))
k.queue("POST", QMSG, [("visibilitytimeout", "3")], {}, qbody("hidden"))
k.queue("GET", QMSG, [("peekonly", "true")])
k.queue("GET", QMSG, [("numofmessages", "33")])
k.queue("GET", QMSG, [("numofmessages", "0")])
k.queue("GET", QMSG, [("visibilitytimeout", "0")])
k.queue("POST", QMSG, [], {}, "not xml")
k.queue("POST", QMSG, [], {}, "<QueueMessage></QueueMessage>")
k.queue("POST", QMSG, [], {}, qbody("x" * 65537), known_diff=True)
k.queue("POST", QMSG, [], {}, qbody("y" * 65536))
k.queue("POST", "/nosuch@R@/messages", [], {}, qbody("x"))
k.queue("GET", "/nosuch@R@/messages", [])
k.queue("GET", QMSG, [("peekonly", "true")])

k = K("queue_visibility_expiry")
k.queue("PUT", QN)
k.queue("POST", QMSG, [("visibilitytimeout", "2")], {}, qbody("later"))
k.queue("GET", QMSG, [])
k.queue("GET", QMSG, [], delay=3)
k.queue("GET", QMSG, [("visibilitytimeout", "1")])
k.queue("GET", QMSG, [], delay=2)
k.queue("POST", QMSG, [("messagettl", "1")], {}, qbody("shortlived"))
k.queue("GET", QMSG, [("peekonly", "true"), ("numofmessages", "5")], delay=2)

k = K("queue_acl_service")
k.queue("PUT", "", [("restype", "service"), ("comp", "properties")], XML, DEFAULT_PROPS)
k.queue("PUT", QN)
k.queue("PUT", QN, [("comp", "acl")], XML, "<SignedIdentifiers><SignedIdentifier><Id>p1</Id><AccessPolicy><Permission>raup</Permission></AccessPolicy></SignedIdentifier></SignedIdentifiers>")
k.queue("GET", QN, [("comp", "acl")])
k.queue("GET", "", [("restype", "service"), ("comp", "properties")])
k.queue("PUT", "", [("restype", "service"), ("comp", "properties")], XML, "<StorageServiceProperties><Cors><CorsRule><AllowedOrigins>*</AllowedOrigins><AllowedMethods>GET</AllowedMethods><AllowedHeaders>*</AllowedHeaders><ExposedHeaders>*</ExposedHeaders><MaxAgeInSeconds>5</MaxAgeInSeconds></CorsRule></Cors></StorageServiceProperties>")
k.queue("GET", "", [("restype", "service"), ("comp", "properties")])
k.queue("OPTIONS", QMSG, [], {"Origin": "http://a.example", "Access-Control-Request-Method": "GET"}, auth="anon")

k = K("queue_auth")
k.queue("PUT", QN)
k.queue("POST", QMSG, [], {}, qbody("k"), auth="lite")
k.queue("GET", QMSG, [("peekonly", "true")], auth="badkey", known_diff=True)
k.queue("GET", QMSG, [("peekonly", "true")], auth="anon")
k.queue("GET", QMSG, [("peekonly", "true")], auth="sas", sas=dict(kind="service", resource="q@R@", sp="r"))
k.queue("GET", QMSG, [("peekonly", "true")], auth="sas", sas=dict(kind="service", resource="q@R@", sp="a"))
k.queue("POST", QMSG, [], {}, qbody("s"), auth="sas", sas=dict(kind="service", resource="q@R@", sp="a"))
k.queue("GET", QMSG, [], auth="sas", sas=dict(kind="service", resource="q@R@", sp="p"))
k.queue("GET", QMSG, [("peekonly", "true")], auth="sas", sas=dict(kind="service", resource="q@R@", sp="r", exp=-100), known_diff=True)
k.queue("GET", QMSG, [("peekonly", "true")], auth="sas", sas=dict(kind="account", ss="q", srt="o", sp="r"))
k.queue("GET", QMSG, [("peekonly", "true")], auth="sas", sas=dict(kind="account", ss="b", srt="o", sp="r"))
k.queue("GET", "", [("comp", "list"), ("prefix", "q@R@")], auth="sas", sas=dict(kind="account", ss="q", srt="s", sp="l"))

# ============================================================================================ TABLE
TN = "t@R@x"
J = {"Content-Type": "application/json"}


def ent(pk, rk, **props):
    return json.dumps({"PartitionKey": pk, "RowKey": rk, **props})


def tpath(pk, rk):
    return f"/{TN}(PartitionKey='{pk}',RowKey='{rk}')"


k = K("table_lifecycle")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/Tables", [], {**J, "Prefer": "return-no-content"}, json.dumps({"TableName": TN + "b"}))
k.table("POST", "/Tables", [], J, json.dumps({"TableName": "1bad"}))
k.table("POST", "/Tables", [], J, json.dumps({"TableName": "ab"}))
k.table("POST", "/Tables", [], J, json.dumps({"NoName": "x"}))
k.table("POST", "/Tables", [], J, "not json")
k.table("GET", "/Tables", [("$filter", "TableName eq '" + TN + "'")])
k.table("GET", "/Tables", [("$filter", "TableName ge 't@R@x' and TableName le 't@R@xz'")])
k.table("GET", "/Tables", [("$filter", "TableName ge 't@R@x' and TableName le 't@R@xz'"), ("$top", "1")])
k.table("GET", "/Tables", [("$filter", "TableName ge 't@R@x' and TableName le 't@R@xz'")], {"Accept": "application/json;odata=nometadata"})
k.table("GET", "/Tables", [("$filter", "TableName ge 't@R@x' and TableName le 't@R@xz'")], {"Accept": "application/json;odata=fullmetadata"})
k.table("GET", "/Tables", [("$filter", "bogus ((")])
k.table("DELETE", f"/Tables('{TN}')")
k.table("DELETE", f"/Tables('{TN}')")
k.table("GET", "/Tables", [("$filter", "TableName eq '" + TN + "'")])
k.table("GET", "/Tables", [("$filter", "TableName ge 't@R@x' and TableName le 't@R@xz'")], {"Accept": "application/atom+xml"}, known_diff=True)

k = K("table_tables_paging")
for n in "abc":
    k.table("POST", "/Tables", [], J, json.dumps({"TableName": f"tp@R@{n}"}))
k.table("GET", "/Tables", [("$filter", "TableName ge 'tp@R@' and TableName le 'tp@R@z'"), ("$top", "2")], cap={"nt": "h:x-ms-continuation-NextTableName"})
k.table("GET", "/Tables", [("$filter", "TableName ge 'tp@R@' and TableName le 'tp@R@z'"), ("$top", "2"), ("NextTableName", "<<nt>>")])

k = K("table_entities_basic")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/" + TN, [], J, ent("p1", "r1", Name="alice", Age=30, Score=1.5, Ok=True))
k.table("POST", "/" + TN, [], J, ent("p1", "r1", Name="dup"))
k.table("POST", "/" + TN, [], {**J, "Prefer": "return-no-content"}, ent("p1", "r2", Name="bob"))
k.table("POST", "/" + TN, [], {**J, "Accept": "application/json;odata=nometadata"}, ent("p1", "r3", Name="carl"))
k.table("POST", "/" + TN, [], {**J, "Accept": "application/json;odata=fullmetadata"}, ent("p1", "r4", Name="dora"))
k.table("POST", "/" + TN, [], J, json.dumps({"PartitionKey": "p1"}))
k.table("POST", "/" + TN, [], J, json.dumps({"RowKey": "x"}))
k.table("POST", "/" + TN, [], J, ent("p/1", "r"))
k.table("POST", "/" + TN, [], J, ent("p1", "r5", Big=2147483648))
k.table("POST", "/nosuch@R@x", [], J, ent("p", "r"))
k.table("GET", tpath("p1", "r1"))
k.table("GET", tpath("p1", "r1"), [], {"Accept": "application/json;odata=nometadata"})
k.table("GET", tpath("p1", "r1"), [], {"Accept": "application/json;odata=fullmetadata"})
k.table("GET", tpath("p1", "r1"), [("$select", "Name,Age")])
k.table("GET", tpath("p1", "zz"))
k.table("GET", tpath("pz", "r1"))
k.table("GET", "/" + TN + "()")
k.table("GET", "/" + TN + "()", [("$top", "2")], cap={"np": "h:x-ms-continuation-NextPartitionKey", "nr": "h:x-ms-continuation-NextRowKey"})
k.table("GET", "/" + TN + "()", [("$top", "2"), ("NextPartitionKey", "<<np>>"), ("NextRowKey", "<<nr>>")])
k.table("GET", "/" + TN + "()", [("$top", "0")])
k.table("GET", "/" + TN + "()", [("$select", "Name")], {"Accept": "application/json;odata=nometadata"})

k = K("table_types")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
body = {"PartitionKey": "p", "RowKey": "types", "S": "str", "I": 5, "L": "9223372036854775807", "L@odata.type": "Edm.Int64",
        "D": 3.25, "D2": 3.0, "D3": "NaN", "D3@odata.type": "Edm.Double", "B": False,
        "T": "2024-05-06T07:08:09.1234567Z", "T@odata.type": "Edm.DateTime", "G": "0f8fad5b-d9cb-469f-a165-70867728950e",
        "G@odata.type": "Edm.Guid", "Bin": "AQIDBA==", "Bin@odata.type": "Edm.Binary", "Nul": None, "Uni": "héllo ☃"}
k.table("POST", "/" + TN, [], J, json.dumps(body))
for lv in ("minimalmetadata", "nometadata", "fullmetadata"):
    k.table("GET", tpath("p", "types"), [], {"Accept": f"application/json;odata={lv}"})
k.table("GET", "/" + TN + "()", [], {"Accept": "application/json;odata=fullmetadata"})

k = K("table_filters")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
for i in range(1, 7):
    k.table("POST", "/" + TN, [], {**J, "Prefer": "return-no-content"},
            ent("pa" if i <= 3 else "pb", f"r{i}", N=i, S="x" * i, F=i / 2, Flag=(i % 2 == 0), Big=str(i * 10 ** 12),
                **{"Big@odata.type": "Edm.Int64"}))
Q = "/" + TN + "()"
for f in ("PartitionKey eq 'pa'", "PartitionKey eq 'pa' and RowKey gt 'r1'", "PartitionKey ne 'pa'", "N gt 2", "N ge 2 and N le 4",
          "N lt 3 or N gt 5", "not (N gt 2)", "(N eq 1 or N eq 2) and Flag eq false", "S eq 'xxx'", "F gt 1.5", "F eq 1.0",
          "Flag eq true", "Flag", "Big gt 3000000000000L", "Big eq 2000000000000L", "N eq 1.0", "Missing eq 1",
          "RowKey ge 'r2' and RowKey lt 'r5'", "PartitionKey eq 'pa' or PartitionKey eq 'pb'", "N gt 'x'", "N eq", "N eq 1 and",
          "((N eq 1)", "S eq 'it''s'", "Timestamp gt datetime'2000-01-01T00:00:00Z'", "PartitionKey lt 'pb'",
          "N eq 3 and S eq 'xxx'", "PartitionKey eq 'pb' and N ge 5"):
    k.table("GET", Q, [("$filter", f)], {"Accept": "application/json;odata=nometadata"})
k.table("GET", Q, [("$filter", "N ge 2"), ("$top", "2"), ("$select", "N,S")], {"Accept": "application/json;odata=nometadata"},
        cap={"np": "h:x-ms-continuation-NextPartitionKey", "nr": "h:x-ms-continuation-NextRowKey"})
k.table("GET", Q, [("$filter", "N ge 2"), ("$top", "2"), ("$select", "N,S"), ("NextPartitionKey", "<<np>>"), ("NextRowKey", "<<nr>>")],
        {"Accept": "application/json;odata=nometadata"})

k = K("table_update_merge")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/" + TN, [], J, ent("p", "r", A=1, B="two"), cap={"e": "h:ETag"})
k.table("PUT", tpath("p", "r"), [], {**J, "If-Match": "*"}, ent("p", "r", C=3))
k.table("GET", tpath("p", "r"), [], {"Accept": "application/json;odata=nometadata"})
k.table("MERGE", tpath("p", "r"), [], {**J, "If-Match": "*"}, ent("p", "r", D=4))
k.table("GET", tpath("p", "r"), [], {"Accept": "application/json;odata=nometadata"}, cap={"e2": "h:ETag"})
k.table("PUT", tpath("p", "r"), [], {**J, "If-Match": "<<e>>"}, ent("p", "r", X=1))
k.table("MERGE", tpath("p", "r"), [], {**J, "If-Match": "<<e>>"}, ent("p", "r", X=1))
k.table("PATCH", tpath("p", "r"), [], {**J, "If-Match": "<<e2>>"}, ent("p", "r", E=5))
k.table("GET", tpath("p", "r"), [], {"Accept": "application/json;odata=nometadata"})
k.table("PUT", tpath("p", "new"), [], {**J, "If-Match": "*"}, ent("p", "new", A=1))
k.table("MERGE", tpath("p", "new"), [], {**J, "If-Match": "*"}, ent("p", "new", A=1))
k.table("PUT", tpath("p", "up1"), [], J, ent("p", "up1", A=1))
k.table("PUT", tpath("p", "up1"), [], J, ent("p", "up1", B=2))
k.table("MERGE", tpath("p", "up1"), [], J, ent("p", "up1", C=3))
k.table("MERGE", tpath("p", "up2"), [], J, ent("p", "up2", C=3))
k.table("GET", "/" + TN + "()", [("$filter", "PartitionKey eq 'p'")], {"Accept": "application/json;odata=nometadata"})
k.table("PUT", tpath("p", "up1"), [], {**J, "If-Match": "*"}, ent("q", "up1", A=1))
k.table("DELETE", tpath("p", "up1"))
k.table("DELETE", tpath("p", "up1"), [], {"If-Match": '*'})
k.table("DELETE", tpath("p", "up1"), [], {"If-Match": '*'})
k.table("DELETE", tpath("p", "r"), [], {"If-Match": "<<e>>"})
k.table("DELETE", tpath("p", "r"), [], {"If-Match": "<<e2>>"})
k.table("GET", tpath("p", "r"))

BATCH = "batch_aaaaaaaa-1111-2222-3333-444444444444"
CS = "changeset_bbbbbbbb-1111-2222-3333-444444444444"


def tbatch(ops, tn=TN):
    out = f"--{BATCH}\r\nContent-Type: multipart/mixed; boundary={CS}\r\n\r\n"
    for i, (m, path, hdr, body) in enumerate(ops):
        out += (f"--{CS}\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\n{m} http://127.0.0.1:10002/devstoreaccount1{path} HTTP/1.1\r\n"
                f"Content-Type: application/json\r\nAccept: application/json;odata=minimalmetadata\r\n" + "".join(f"{a}: {b}\r\n" for a, b in hdr.items())
                + "\r\n" + body + "\r\n")
    return out + f"--{CS}--\r\n--{BATCH}--\r\n"


BH = {"Content-Type": f"multipart/mixed; boundary={BATCH}"}
k = K("table_batch")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/$batch", [], BH, tbatch([("POST", "/" + TN, {}, ent("pb", "1", A=1)), ("POST", "/" + TN, {}, ent("pb", "2", A=2)),
                                          ("POST", "/" + TN, {"Prefer": "return-no-content"}, ent("pb", "3", A=3))]))
k.table("GET", "/" + TN + "()", [("$filter", "PartitionKey eq 'pb'")], {"Accept": "application/json;odata=nometadata"})
k.table("POST", "/$batch", [], BH, tbatch([("POST", "/" + TN, {}, ent("pb", "4", A=4)), ("POST", "/" + TN, {}, ent("pb", "1", A=9))]))
k.table("GET", tpath("pb", "4"))
k.table("POST", "/$batch", [], BH, tbatch([("PUT", tpath("pb", "1"), {"If-Match": "*"}, ent("pb", "1", Z=1)),
                                          ("MERGE", tpath("pb", "2"), {"If-Match": "*"}, ent("pb", "2", Z=2)),
                                          ("DELETE", tpath("pb", "3"), {"If-Match": "*"}, "")]))
k.table("GET", "/" + TN + "()", [("$filter", "PartitionKey eq 'pb'")], {"Accept": "application/json;odata=nometadata"})
k.table("POST", "/$batch", [], BH, tbatch([("POST", "/" + TN, {}, ent("pb", "7", A=1)), ("POST", "/" + TN, {}, ent("pb", "7", A=2))]))
k.table("POST", "/$batch", [], BH, tbatch([("DELETE", tpath("pb", "nope"), {"If-Match": "*"}, "")]))

k = K("table_acl_service")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("PUT", f"/{TN}", [("comp", "acl")], XML, "<SignedIdentifiers><SignedIdentifier><Id>p1</Id><AccessPolicy><Permission>raud</Permission></AccessPolicy></SignedIdentifier></SignedIdentifiers>")
k.table("GET", f"/{TN}", [("comp", "acl")])
k.table("GET", "", [("restype", "service"), ("comp", "properties")])

k = K("table_auth")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/" + TN, [], J, ent("p", "1", A=1), auth="lite")
k.table("GET", tpath("p", "1"), [], auth="lite")
k.table("GET", tpath("p", "1"), [], auth="badkey", known_diff=True)
k.table("GET", tpath("p", "1"), [], auth="anon")
k.table("GET", tpath("p", "1"), [], auth="sas", sas=dict(kind="service", resource=TN, sp="r"))
k.table("GET", tpath("p", "1"), [], auth="sas", sas=dict(kind="service", resource=TN, sp="a"))
k.table("POST", "/" + TN, [], J, ent("p", "2", A=1), auth="sas", sas=dict(kind="service", resource=TN, sp="a"))
k.table("GET", tpath("p", "1"), [], auth="sas", sas=dict(kind="service", resource=TN, sp="r", range=("p", "", "p", "")))
k.table("GET", tpath("p", "1"), [], auth="sas", sas=dict(kind="service", resource=TN, sp="r", exp=-100), known_diff=True)
k.table("GET", tpath("p", "1"), [], auth="sas", sas=dict(kind="account", ss="t", srt="o", sp="r"))
k.table("GET", tpath("p", "1"), [], auth="sas", sas=dict(kind="account", ss="b", srt="o", sp="r"))
k.table("GET", "/Tables", [], auth="sas", sas=dict(kind="account", ss="t", srt="c", sp="r"))

for _svc, _nm in (("blob", "blob_service"), ("queue", "queue_acl_service")):
    CASES[_nm].append(dict(svc=_svc, method="PUT", path="", query=[("restype", "service"), ("comp", "properties")], headers=dict(XML),
                           body=DEFAULT_PROPS))

k = K("table_batch_xpartition")
k.table("POST", "/Tables", [], J, json.dumps({"TableName": TN}))
k.table("POST", "/$batch", [], BH, tbatch([("POST", "/" + TN, {}, ent("pb", "5", A=5)), ("POST", "/" + TN, {}, ent("pc", "1", A=1))]), known_diff=True)
k.table("GET", tpath("pb", "5"), known_diff=True)

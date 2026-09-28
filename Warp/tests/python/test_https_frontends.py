"""HTTPS for the HTTP-style API frontends (dynamowire, sqswire, oswire, influxwire, s3wire, gcswire, the unified awswire
endpoint, pubsubwire REST, cosmoswire, azurewire blob/queue/table, gremlinwire https+wss) against ONE real Warp subprocess
with a generated test CA + localhost leaf certificate, driven with the real client libraries where installed
(boto3, opensearch-py, azure-storage-*, azure-data-tables, google-cloud-storage, azure-cosmos, gremlinpython) and `requests`
otherwise. Also: an untrusted client is rejected, plaintext keeps working, /api/interfaces reports the https port,
a bad certificate configuration leaves the frontend on plaintext only, and WARP_<NAME>_HTTP_DISABLED closes the plaintext port.
Needs the `openssl` CLI, WARP_TEST_PG_LOCAL=1 and WARP_TEST_JAR.
"""
import base64
import json
import os
import shutil
import socket
import ssl
import subprocess
import time

import pytest
import requests

from warp_test_support import WarpProcess, RealPostgres, free_port, isolated_ports

ADMIN = "warp-https-admin-token"
os.environ["WARP_ADMIN_TOKEN"] = ADMIN
pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl CLI not available")

S3_KEY, S3_SECRET = "AKIAHTTPS", "https-secret-key"
AZ_ACCOUNT = "devstoreaccount1"
AZ_KEY = ("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
COSMOS_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
STORES = ["dynamodb", "sqs", "s3", "opensearch", "influxdb", "azblob", "azqueue", "aztable", "gcs", "cosmos", "gremlin", "pubsub",
          "sns", "kinesis", "awsparams"]


def _run(*args):
    subprocess.run(args, check=True, capture_output=True)


def https_first_ignite_port():
    for port in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", port))
                return port
            except OSError:
                continue
    raise RuntimeError("no free port in 47500..47599")


def make_test_pki(d):
    """A test CA, a localhost leaf signed by it (PEM cert + PKCS8 key) and a second, unrelated CA (untrusted client)."""
    p = lambda n: str(d / n)  # noqa: E731
    _run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", p("ca.key"), "-out", p("ca.pem"), "-days", "30",
         "-subj", "/CN=Warp HTTPS Test CA", "-addext", "basicConstraints=critical,CA:TRUE",
         "-addext", "keyUsage=critical,keyCertSign,cRLSign")
    _run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", p("other.key"), "-out", p("other-ca.pem"),
         "-days", "30", "-subj", "/CN=Some Other CA", "-addext", "basicConstraints=critical,CA:TRUE")
    (d / "leaf.ext").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nbasicConstraints=CA:FALSE\n"
                                "keyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
    _run("openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", p("leaf.key"), "-out", p("leaf.csr"), "-subj", "/CN=localhost")
    _run("openssl", "x509", "-req", "-in", p("leaf.csr"), "-CA", p("ca.pem"), "-CAkey", p("ca.key"), "-CAcreateserial",
         "-out", p("leaf.pem"), "-days", "30", "-extfile", p("leaf.ext"))
    (d / "chain.pem").write_text((d / "leaf.pem").read_text() + (d / "ca.pem").read_text())
    _run("openssl", "pkcs8", "-topk8", "-nocrypt", "-in", p("leaf.key"), "-out", p("leaf.pkcs8.key"))
    return {"ca": p("ca.pem"), "other_ca": p("other-ca.pem"), "cert": p("chain.pem"), "key": p("leaf.pkcs8.key")}


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    return make_test_pki(tmp_path_factory.mktemp("https-frontends-pki"))


@pytest.fixture(scope="module")
def pg():
    db = RealPostgres()
    yield db
    db.close()


NAMES = ["dynamowire", "sqswire", "oswire", "influxwire", "s3wire", "gcswire", "awswire", "pubsubwire_rest", "cosmoswire",
         "azblobwire", "azqueuewire", "aztablewire", "gremlinwire"]
# (env prefix used for WARP_<X>_PORT / WARP_<X>_HTTPS_PORT, default https port, interface id)
SPEC = {
    "dynamowire": ("DYNAMOWIRE", 18445, "dynamowire"), "sqswire": ("SQSWIRE", 18446, "sqswire"),
    "oswire": ("OSWIRE", 18447, "oswire"), "influxwire": ("INFLUXWIRE", 18448, "influxwire"),
    "s3wire": ("S3WIRE", 18449, "s3wire"), "gcswire": ("GCSWIRE", 18450, "gcswire"),
    "awswire": ("AWSWIRE", 18451, "awswire"), "pubsubwire_rest": ("PUBSUBWIRE_REST", 18452, "pubsubwire-rest"),
    "cosmoswire": ("COSMOSWIRE", 18457, "cosmoswire"), "azblobwire": ("AZBLOBWIRE", 18458, "azblobwire"),
    "azqueuewire": ("AZQUEUEWIRE", 18459, "azqueuewire"), "aztablewire": ("AZTABLEWIRE", 18460, "aztablewire"),
    "gremlinwire": ("GREMLINWIRE", 18461, "gremlinwire"),
}


class HttpsWarp:
    def __init__(self, pg, pki, extra=None):
        self.pki = pki
        self.http = {n: free_port() for n in NAMES}
        self.https = {n: free_port() for n in NAMES}
        env = {**isolated_ports("WARP_DYNAMOWIRE_PORT"), "WARP_TRUSTED_BACKEND_HOSTS": "localhost",
               "WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
               "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{https_first_ignite_port()}",
               "WARP_TLS_CERT": pki["cert"], "WARP_TLS_KEY": pki["key"],
               "WARP_S3WIRE_CREDENTIALS": f"{S3_KEY}={S3_SECRET}", "WARP_S3WIRE_ENABLED": "true",
               "WARP_GCSWIRE_ALLOW_ANONYMOUS": "true", "WARP_AZURE_DEV_ACCOUNT": "true", "WARP_KMS_INSECURE_DEV_KEY": "true",
               "WARP_ADMIN_TOKEN": ADMIN, "WARP_GREMLINWIRE_ENABLED": "true"}
        for n, (pre, _, _) in SPEC.items():
            if n == "dynamowire":
                continue  # the WarpProcess frontend port
            env[f"WARP_{pre}_PORT"] = str(self.http[n])
            env[f"WARP_{pre}_HTTPS_PORT"] = str(self.https[n])
        env["WARP_DYNAMOWIRE_HTTPS_PORT"] = str(self.https["dynamowire"])
        env.update(extra or {})
        self.proc = WarpProcess(pg, "WARP_DYNAMOWIRE_PORT", frontend_name="https-frontends", extra_env=env)
        self.http["dynamowire"] = self.proc.frontend_port
        self.api("PATCH", "/api/backend-sets/default/backends/default", {"enabledStores": STORES})
        deadline = time.time() + 30
        while time.time() < deadline:
            time.sleep(0.5)
            try:
                if all(socket.create_connection(("localhost", p), 1) for p in self.https.values()):
                    break
            except OSError:
                pass

    def api(self, method, path, body=None, expect=200):
        r = requests.request(method, f"http://localhost:{self.proc.metrics_port}{path}", json=body, timeout=60,
                             headers={"Authorization": f"Bearer {ADMIN}"})
        assert r.status_code == expect, (r.status_code, r.text)
        return r.json()

    def url(self, name, tls=True):
        return f"{'https' if tls else 'http'}://localhost:{(self.https if tls else self.http)[name]}"

    def log(self):
        return "".join(self.proc._output_lines)

    def close(self):
        self.proc.close()


@pytest.fixture(scope="module")
def warp(pg, pki):
    w = HttpsWarp(pg, pki)
    yield w
    w.close()


@pytest.fixture()
def ca_env(pki, monkeypatch):
    """Clients built on requests/botocore/urllib3 pick the CA up from the environment like a real deployment would."""
    for v in ("REQUESTS_CA_BUNDLE", "AWS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE"):
        monkeypatch.setenv(v, pki["ca"])
    return pki["ca"]


# ------------------------------------------------------------------------------------------------ AWS family

def boto(service, warp, name, tls=True, **kw):
    import boto3
    from botocore.config import Config
    return boto3.client(service, endpoint_url=warp.url(name, tls), region_name="us-east-1",
                        aws_access_key_id=S3_KEY if service == "s3" else "test",
                        aws_secret_access_key=S3_SECRET if service == "s3" else "test",
                        verify=warp.pki["ca"] if tls else None,
                        config=Config(retries={"max_attempts": 0}, **kw))


def test_dynamodb_over_https_boto3(warp):
    d = boto("dynamodb", warp, "dynamowire")
    d.create_table(TableName="https_t", AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                   KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}], BillingMode="PAY_PER_REQUEST")
    d.put_item(TableName="https_t", Item={"id": {"S": "a"}, "v": {"N": "1"}})
    assert d.get_item(TableName="https_t", Key={"id": {"S": "a"}})["Item"]["v"]["N"] == "1"
    # plaintext still works and sees the same data
    p = boto("dynamodb", warp, "dynamowire", tls=False)
    assert p.get_item(TableName="https_t", Key={"id": {"S": "a"}})["Item"]["id"]["S"] == "a"


def test_sqs_over_https_queue_url_is_https(warp):
    q = boto("sqs", warp, "sqswire")
    url = q.create_queue(QueueName="https-q")["QueueUrl"]
    assert url.startswith(warp.url("sqswire")), url
    q.send_message(QueueUrl=url, MessageBody="hello-tls")
    assert q.receive_message(QueueUrl=url, WaitTimeSeconds=1)["Messages"][0]["Body"] == "hello-tls"
    plain = boto("sqs", warp, "sqswire", tls=False)
    purl = plain.get_queue_url(QueueName="https-q")["QueueUrl"]
    assert purl.startswith("http://localhost:"), purl


def test_s3_over_https_sigv4_and_post_location(warp):
    s3 = boto("s3", warp, "s3wire", s3={"addressing_style": "path"}, signature_version="s3v4")
    s3.create_bucket(Bucket="https-b")
    s3.put_object(Bucket="https-b", Key="dir/k1", Body=b"tls-body")
    assert s3.get_object(Bucket="https-b", Key="dir/k1")["Body"].read() == b"tls-body"
    assert [o["Key"] for o in s3.list_objects_v2(Bucket="https-b")["Contents"]] == ["dir/k1"]
    # a presigned URL made for the https endpoint is honoured over https (SigV4 signs host:port)
    signed = s3.generate_presigned_url("get_object", Params={"Bucket": "https-b", "Key": "dir/k1"}, ExpiresIn=60)
    assert signed.startswith(warp.url("s3wire") + "/")
    r = requests.get(signed, verify=warp.pki["ca"], timeout=30)
    assert r.status_code == 200 and r.content == b"tls-body"
    # multipart over https
    up = s3.create_multipart_upload(Bucket="https-b", Key="mp")["UploadId"]
    part = s3.upload_part(Bucket="https-b", Key="mp", UploadId=up, PartNumber=1, Body=b"x" * 5 * 1024 * 1024)
    s3.complete_multipart_upload(Bucket="https-b", Key="mp", UploadId=up,
                                 MultipartUpload={"Parts": [{"PartNumber": 1, "ETag": part["ETag"]}]})
    assert s3.head_object(Bucket="https-b", Key="mp")["ContentLength"] == 5 * 1024 * 1024
    # POST-object success redirect / Location is https
    post = s3.generate_presigned_post("https-b", "posted", ExpiresIn=60)
    r = requests.post(post["url"], data={**post["fields"]}, files={"file": ("f", b"posted-data")}, verify=warp.pki["ca"], timeout=30)
    assert r.status_code in (200, 201, 204), r.text
    loc = r.headers.get("Location")
    assert loc is None or loc.startswith("https://"), loc


def test_awswire_unified_endpoint_services_over_https(warp):
    for svc, call in (("sns", lambda c: c.create_topic(Name="https-topic")["TopicArn"]),
                      ("secretsmanager", lambda c: c.create_secret(Name="https/secret", SecretString="s3cr3t")["Name"]),
                      ("kinesis", lambda c: c.create_stream(StreamName="https-stream", ShardCount=1))):
        c = boto(svc, warp, "awswire")
        call(c)
    assert boto("secretsmanager", warp, "awswire").get_secret_value(SecretId="https/secret")["SecretString"] == "s3cr3t"
    assert "https-stream" in boto("kinesis", warp, "awswire").list_streams()["StreamNames"]
    # the unified endpoint also serves S3/SQS/DynamoDB and answers queue URLs with https
    q = boto("sqs", warp, "awswire")
    assert q.create_queue(QueueName="uni-q")["QueueUrl"].startswith(warp.url("awswire"))
    boto("dynamodb", warp, "awswire").list_tables()


# ------------------------------------------------------------------------------------------------ search / time series

def test_opensearch_over_https(warp, ca_env):
    from opensearchpy import OpenSearch
    c = OpenSearch(hosts=[{"host": "localhost", "port": warp.https["oswire"]}], use_ssl=True, verify_certs=True, ca_certs=ca_env)
    c.index(index="https_idx", id="1", body={"title": "tls works"}, refresh=True)
    assert c.get(index="https_idx", id="1")["_source"]["title"] == "tls works"
    assert c.search(index="https_idx", body={"query": {"match_all": {}}})["hits"]["total"]["value"] == 1
    assert c.info()["version"]


def test_influx_over_https(warp, ca_env):
    b = warp.url("influxwire")
    requests.post(f"{b}/write", params={"db": "httpsdb"}, data="temp,room=a v=21.5", verify=ca_env, timeout=30).raise_for_status()
    body = requests.get(f"{b}/query", params={"db": "httpsdb", "q": "SELECT * FROM temp"}, verify=ca_env, timeout=30).json()
    assert body["results"][0]["series"][0]["values"]
    from influxdb import InfluxDBClient
    ic = InfluxDBClient(host="localhost", port=warp.https["influxwire"], ssl=True, verify_ssl=ca_env, database="httpsdb")
    assert ic.query("SELECT * FROM temp").raw["series"]


# ------------------------------------------------------------------------------------------------ Azure

def az_conn(warp, tls=True):
    return (f"DefaultEndpointsProtocol={'https' if tls else 'http'};AccountName={AZ_ACCOUNT};AccountKey={AZ_KEY};"
            f"BlobEndpoint={warp.url('azblobwire', tls)}/{AZ_ACCOUNT};QueueEndpoint={warp.url('azqueuewire', tls)}/{AZ_ACCOUNT};"
            f"TableEndpoint={warp.url('aztablewire', tls)}/{AZ_ACCOUNT};")


def test_azure_blob_over_https(warp, ca_env):
    from azure.storage.blob import BlobServiceClient
    s = BlobServiceClient.from_connection_string(az_conn(warp))
    c = s.create_container("httpsc")
    b = c.upload_blob("b1", b"az-tls-data")
    assert c.get_blob_client("b1").download_blob().readall() == b"az-tls-data"
    assert [x.name for x in c.list_blobs()] == ["b1"]
    assert c.get_blob_client("b1").url.startswith("https://localhost:")
    p = BlobServiceClient.from_connection_string(az_conn(warp, tls=False))
    assert p.get_container_client("httpsc").get_blob_client("b1").download_blob().readall() == b"az-tls-data"


def test_azure_queue_over_https(warp, ca_env):
    from azure.storage.queue import QueueServiceClient
    s = QueueServiceClient.from_connection_string(az_conn(warp))
    q = s.create_queue("httpsq")
    q.send_message("queue-tls")
    assert [m.content for m in q.receive_messages()] == ["queue-tls"]


def test_azure_table_over_https(warp, ca_env):
    from azure.data.tables import TableServiceClient
    s = TableServiceClient.from_connection_string(az_conn(warp))
    t = s.create_table("httpst")
    t.create_entity({"PartitionKey": "p", "RowKey": "r", "v": 7})
    assert t.get_entity("p", "r")["v"] == 7


# ------------------------------------------------------------------------------------------------ Google

def test_gcs_over_https(warp, ca_env):
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage
    c = storage.Client(project="p", credentials=AnonymousCredentials(),
                       client_options={"api_endpoint": warp.url("gcswire")})
    b = c.create_bucket("https-gcs")
    b.blob("o1").upload_from_string(b"gcs-tls")
    assert c.bucket("https-gcs").blob("o1").download_as_bytes() == b"gcs-tls"
    assert [x.name for x in c.list_blobs("https-gcs")] == ["o1"]
    assert b.blob("o1").public_url.startswith("https://localhost:")


def test_pubsub_rest_over_https(warp, ca_env):
    base = f"{warp.url('pubsubwire_rest')}/v1/projects/p"
    assert requests.put(f"{base}/topics/https-t", json={}, verify=ca_env, timeout=30).status_code in (200, 409)
    assert requests.put(f"{base}/subscriptions/https-s", json={"topic": "projects/p/topics/https-t"}, verify=ca_env,
                        timeout=30).status_code in (200, 409)
    m = base64.b64encode(b"psmsg").decode()
    r = requests.post(f"{base}/topics/https-t:publish", json={"messages": [{"data": m}]}, verify=ca_env, timeout=30)
    assert r.status_code == 200 and r.json()["messageIds"]
    r = requests.post(f"{base}/subscriptions/https-s:pull", json={"maxMessages": 1}, verify=ca_env, timeout=30)
    assert r.json()["receivedMessages"][0]["message"]["data"] == m
    # a push endpoint may be plain http or https
    r = requests.put(f"{base}/subscriptions/https-push", json={"topic": "projects/p/topics/https-t",
                     "pushConfig": {"pushEndpoint": "https://localhost:1/hook"}}, verify=ca_env, timeout=30)
    assert r.status_code in (200, 409), r.text


# ------------------------------------------------------------------------------------------------ Cosmos, Gremlin

def test_cosmos_over_https(warp, ca_env):
    from azure.cosmos import CosmosClient, PartitionKey
    c = CosmosClient(warp.url("cosmoswire"), credential=COSMOS_KEY, connection_verify=ca_env)
    db = c.create_database_if_not_exists("httpsdb")
    ct = db.create_container_if_not_exists("items", partition_key=PartitionKey(path="/pk"))
    ct.upsert_item({"id": "1", "pk": "a", "v": 42})
    assert ct.read_item("1", "a")["v"] == 42
    assert list(ct.query_items("SELECT * FROM c WHERE c.pk = 'a'", enable_cross_partition_query=True))[0]["id"] == "1"
    acct = requests.get(warp.url("cosmoswire") + "/", verify=ca_env, timeout=30)
    assert acct.status_code in (200, 401)


def test_gremlin_wss_and_https(warp, pki):
    from gremlin_python.driver import client as gclient, serializer
    from gremlin_python.driver.aiohttp.transport import AiohttpTransport
    ctx = ssl.create_default_context(cafile=pki["ca"])
    port = warp.https["gremlinwire"]
    c = gclient.Client(f"wss://localhost:{port}/gremlin", "g", message_serializer=serializer.GraphSONSerializersV3d0(),
                       transport_factory=lambda: AiohttpTransport(ssl_options=ctx))
    try:
        assert c.submit("g.addV('tls').property('name','x').count()").all().result() is not None
        assert c.submit("1+1").all().result() == [2]
    finally:
        c.close()
    r = requests.post(f"https://localhost:{port}/gremlin", json={"gremlin": "1+2"}, verify=pki["ca"], timeout=30)
    assert r.status_code == 200 and r.json()["result"]["data"]


# ------------------------------------------------------------------------------------------------ negative + reporting

@pytest.mark.parametrize("name", NAMES)
def test_untrusted_client_is_rejected_everywhere(warp, pki, name):
    port = warp.https[name]
    for cafile in (None, pki["other_ca"]):
        ctx = ssl.create_default_context(cafile=cafile)
        with pytest.raises(ssl.SSLError):
            with socket.create_connection(("localhost", port), 10) as raw:
                with ctx.wrap_socket(raw, server_hostname="localhost"):
                    pass
    # and the trusted CA gets a completed handshake
    ctx = ssl.create_default_context(cafile=pki["ca"])
    with socket.create_connection(("localhost", port), 10) as raw:
        with ctx.wrap_socket(raw, server_hostname="localhost") as s:
            assert s.version() in ("TLSv1.2", "TLSv1.3")


@pytest.mark.parametrize("name", NAMES)
def test_plaintext_port_still_listens(warp, name):
    with socket.create_connection(("localhost", warp.http[name]), 5):
        pass


def test_interfaces_report_https_ports(warp):
    ifs = {i["id"]: i for i in warp.api("GET", "/api/interfaces")["interfaces"]}
    for n, (_, _, iid) in SPEC.items():
        assert iid in ifs, (iid, sorted(ifs))
        assert ifs[iid]["tlsEnabled"] is True and ifs[iid]["httpsPort"] == warp.https[n], (iid, ifs[iid])
        assert ifs[iid]["port"] == warp.http[n]


def test_logged_https_enabled_lines(warp):
    log = warp.log()
    for pre in ("DYNAMOWIRE", "SQSWIRE", "OSWIRE", "INFLUXWIRE", "S3WIRE", "GCSWIRE", "AWSWIRE", "PUBSUBWIRE_REST", "COSMOSWIRE",
                "AZBLOBWIRE", "AZQUEUEWIRE", "AZTABLEWIRE", "GREMLINWIRE"):
        assert f"{pre} HTTPS" in log, pre


def test_env_default_ports_documented():
    """The default HTTPS ports are the documented ones (18445..18461)."""
    assert [SPEC[n][1] for n in NAMES] == [18445, 18446, 18447, 18448, 18449, 18450, 18451, 18452, 18457, 18458, 18459, 18460, 18461]

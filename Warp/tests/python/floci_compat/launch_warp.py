#!/usr/bin/env python3
"""Start a throwaway Warp (dynamowire + sqswire [+ s3wire]) on free ports with a local Postgres, print the
endpoints as JSON, and stay up until SIGTERM/SIGINT. Reuses tests/python/warp_test_support.py.

  WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=<jar> python3 launch_warp.py --state state.json [--minio]

s3wire in Postgres mode (--s3-postgres) needs nothing else: it enables the s3 store on the default backend. Proxy mode needs an
S3-compatible backend: --minio starts a MinIO container, or pass --s3-backend-endpoint/
--s3-backend-bucket/--s3-backend-key/--s3-backend-secret for an existing one. Credentials for every
frontend are test/test (what Floci's tests use).
"""
import argparse, json, os, signal, subprocess, sys, time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from warp_test_support import WarpProcess, RealPostgres, RealMinio, free_port, isolated_ports  # noqa: E402


def first_free_discovery_port():
    # Ignite binds the first port it CAN bind (0.0.0.0) and must find that same port in its seed list: test bindability,
    # not connectability (a stray JVM on another interface makes lsof/connect_ex look free).
    import socket
    for p in range(47500, 47600):
        with socket.socket() as s:
            try:
                s.bind(("0.0.0.0", p))
                return p
            except OSError:
                continue
    raise RuntimeError("no free Ignite discovery port")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state", required=True)
    ap.add_argument("--minio", action="store_true")
    ap.add_argument("--s3-backend-endpoint")
    ap.add_argument("--s3-backend-bucket", default="warp-backend")
    ap.add_argument("--s3-backend-key", default="warptestkey")
    ap.add_argument("--s3-backend-secret", default="warptestsecret")
    ap.add_argument("--s3-postgres", action="store_true",
                    help="s3wire in POSTGRES mode: enable the s3 store on the default backend (admin API), "
                         "credentials test=test, no MinIO")
    ap.add_argument("--vhost-domain", help="WARP_S3WIRE_VHOST_DOMAIN for virtual-hosted-style S3 (e.g. localhost)")
    ap.add_argument("--aws-unified", action="store_true",
                    help="also start the unified AWS endpoint (WARP_AWSWIRE_PORT, free port) and enable the sns, kinesis and awsparams "
                         "stores (Secrets/SSM/KMS/STS) on the default backend; KMS runs with WARP_KMS_INSECURE_DEV_KEY=true; "
                         "also enables the s3 store when --s3-postgres is given")
    ap.add_argument("--extra-env", action="append", default=[], help="KEY=VALUE for the Warp process")
    a = ap.parse_args()

    os.environ.setdefault("WARP_ADMIN_TOKEN", "warp-test-admin-token")
    pg, minio, warp = RealPostgres(), None, None
    def cleanup(*_):
        if warp is not None and os.environ.get("LAUNCH_DUMP_LOG"):
            with open(os.environ["LAUNCH_DUMP_LOG"], "w") as f:  # the Warp process's stdout/stderr, for debugging a run
                f.write("".join(warp._output_lines))
        for o in (warp, minio, pg):
            try:
                o and o.close()
            except Exception:  # noqa: BLE001
                pass
        sys.exit(0)
    signal.signal(signal.SIGTERM, cleanup); signal.signal(signal.SIGINT, cleanup)
    try:
        env = isolated_ports(exclude="WARP_DYNAMOWIRE_PORT")
        env["WARP_SQSWIRE_PORT"] = str(free_port())
        env.update({"WARP_CLUSTER_ENABLED": "true", "WARP_CLUSTER_DISCOVERY": "static",
                    "WARP_CLUSTER_SEED_NODES": f"127.0.0.1:{first_free_discovery_port()}"})
        s3_port = None
        endpoint = a.s3_backend_endpoint
        if a.minio:
            import boto3
            from botocore.client import Config
            minio = RealMinio()
            endpoint, a.s3_backend_key, a.s3_backend_secret = minio.endpoint, minio.ACCESS_KEY, minio.SECRET_KEY
            boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1", aws_access_key_id=a.s3_backend_key,
                         aws_secret_access_key=a.s3_backend_secret,
                         config=Config(s3={"addressing_style": "path"})).create_bucket(Bucket=a.s3_backend_bucket)
        if endpoint:
            s3_port = int(env["WARP_S3WIRE_PORT"])
            env.update({"WARP_S3WIRE_BACKEND_ENDPOINT": endpoint, "WARP_S3WIRE_BACKEND_BUCKET": a.s3_backend_bucket,
                        "WARP_S3WIRE_BACKEND_ACCESS_KEY": a.s3_backend_key,
                        "WARP_S3WIRE_BACKEND_SECRET_KEY": a.s3_backend_secret,
                        "WARP_S3WIRE_CREDENTIALS": "test=test"})
        if a.s3_postgres:
            s3_port = int(env.setdefault("WARP_S3WIRE_PORT", str(free_port())))
            env.update({"WARP_S3WIRE_ENABLED": "true", "WARP_S3WIRE_CREDENTIALS": "test=test"})
        if a.vhost_domain:
            env["WARP_S3WIRE_VHOST_DOMAIN"] = a.vhost_domain
        aws_port = None
        if a.aws_unified:
            aws_port = free_port()
            env.update({"WARP_AWSWIRE_PORT": str(aws_port), "WARP_KMS_INSECURE_DEV_KEY": "true"})
        for kv in a.extra_env:
            k, v = kv.split("=", 1); env[k] = v
        warp = WarpProcess(pg, "WARP_DYNAMOWIRE_PORT", frontend_name="dynamowire", extra_env=env)
        if a.s3_postgres or a.aws_unified:
            import requests
            stores = (["s3"] if a.s3_postgres else []) + (["sns", "kinesis", "awsparams"] if a.aws_unified else [])
            r = requests.patch(f"http://localhost:{warp.metrics_port}/api/backend-sets/default/backends/default",
                               json={"enabledStores": stores}, timeout=60,
                               headers={"Authorization": f"Bearer {os.environ['WARP_ADMIN_TOKEN']}"})
            assert r.status_code == 200, (r.status_code, r.text)
        if aws_port:
            import requests
            for _ in range(120):
                try:
                    if requests.get(f"http://localhost:{aws_port}/_warp/health", timeout=2).status_code == 200:
                        break
                except Exception:  # noqa: BLE001
                    time.sleep(0.5)
        state = {"dynamodb": f"http://localhost:{warp.frontend_port}",
                 "sqs": f"http://localhost:{env['WARP_SQSWIRE_PORT']}",
                 "s3": f"http://localhost:{s3_port}" if s3_port else None,
                 "aws": f"http://localhost:{aws_port}" if aws_port else None,
                 "pid": os.getpid(), "warp_pid": warp.process.pid}
        json.dump(state, open(a.state, "w"))
        print(json.dumps(state), flush=True)
        while True:
            if warp.process.poll() is not None:
                print("warp exited", warp.process.returncode, "".join(warp._output_lines[-30:]), file=sys.stderr)
                cleanup()
            time.sleep(1)
    except BaseException as e:  # noqa: BLE001
        print("launch failed:", e, file=sys.stderr)
        cleanup()


if __name__ == "__main__":
    main()

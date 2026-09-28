#!/usr/bin/env python3
"""Client-side latency (ms) of the awswire services against a real Warp on native Postgres: boto3, one client per service (keep-alive
connection), sequential, the unified endpoint, 300 measured requests per operation after a warm-up. There is no comparison product:
this reports Warp's p50 / p95 only.

  WARP_TEST_PG_LOCAL=1 WARP_TEST_JAR=<jar> python3 awsextras_rtt_bench.py [--backends 1|2]
"""
import argparse
import json
import statistics
import time

import boto3
from botocore.config import Config

from awsextras_warp_support import start_aws_warp
from warp_test_support import RealPostgres


def timeit(fn, n=300, warm=30):
    for _ in range(warm):
        fn()
    xs = []
    for _ in range(n):
        t = time.perf_counter()
        fn()
        xs.append((time.perf_counter() - t) * 1000)
    xs.sort()
    return statistics.median(xs), xs[int(len(xs) * 0.95) - 1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backends", type=int, default=1)
    a = ap.parse_args()
    pgs = [RealPostgres() for _ in range(a.backends)]
    warp = start_aws_warp(pgs[0], pgs[1] if a.backends == 2 else None)
    try:
        kw = dict(endpoint_url=warp.endpoint, region_name="us-east-1", aws_access_key_id="t", aws_secret_access_key="t",
                  config=Config(retries={"max_attempts": 0}))
        sns, sqs, k, sm, ssm, kms, sts = (boto3.client(s, **kw) for s in ("sns", "sqs", "kinesis", "secretsmanager", "ssm", "kms", "sts"))
        rows = []

        def bench(label, fn, **kws):
            p50, p95 = timeit(fn, **kws)
            rows.append((label, p50, p95))
            print(f"{label:52s} {p50:7.2f} {p95:7.2f}", flush=True)

        topic0 = sns.create_topic(Name="bench-nosub")["TopicArn"]
        topic1 = sns.create_topic(Name="bench-sqs")["TopicArn"]
        q = sqs.create_queue(QueueName="bench-q")["QueueUrl"]
        qarn = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sns.subscribe(TopicArn=topic1, Protocol="sqs", Endpoint=qarn)
        bench("sqs SendMessage (reference, unified endpoint)", lambda: sqs.send_message(QueueUrl=q, MessageBody="x" * 100))
        bench("sns Publish, no subscribers", lambda: sns.publish(TopicArn=topic0, Message="x" * 100))
        bench("sns Publish, 1 SQS subscription (envelope)", lambda: sns.publish(TopicArn=topic1, Message="x" * 100))
        bench("sns GetTopicAttributes", lambda: sns.get_topic_attributes(TopicArn=topic0))
        k.create_stream(StreamName="bench-s", ShardCount=1)
        shard = k.describe_stream(StreamName="bench-s")["StreamDescription"]["Shards"][0]["ShardId"]
        bench("kinesis PutRecord 100 B", lambda: k.put_record(StreamName="bench-s", Data=b"x" * 100, PartitionKey="pk"))
        bench("kinesis PutRecords 10 x 100 B", lambda: k.put_records(StreamName="bench-s", Records=[{"Data": b"x" * 100, "PartitionKey": f"p{i}"} for i in range(10)]))
        it = k.get_shard_iterator(StreamName="bench-s", ShardId=shard, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        bench("kinesis GetRecords (Limit 10)", lambda: k.get_records(ShardIterator=it, Limit=10))
        sm.create_secret(Name="bench-secret", SecretString="s3cret")
        bench("secretsmanager GetSecretValue (KMS decrypt)", lambda: sm.get_secret_value(SecretId="bench-secret"))
        n = [0]

        def put_secret():
            n[0] += 1
            sm.put_secret_value(SecretId="bench-secret", SecretString=f"v{n[0]}")
        bench("secretsmanager PutSecretValue (KMS encrypt)", put_secret, n=100, warm=10)
        ssm.put_parameter(Name="/bench/plain", Value="v", Type="String")
        ssm.put_parameter(Name="/bench/secure", Value="v", Type="SecureString")
        bench("ssm GetParameter String", lambda: ssm.get_parameter(Name="/bench/plain"))
        bench("ssm GetParameter SecureString WithDecryption", lambda: ssm.get_parameter(Name="/bench/secure", WithDecryption=True))
        bench("ssm PutParameter Overwrite", lambda: ssm.put_parameter(Name="/bench/plain", Value="w", Type="String", Overwrite=True))
        kid = kms.create_key()["KeyMetadata"]["KeyId"]
        ct = kms.encrypt(KeyId=kid, Plaintext=b"x" * 64)["CiphertextBlob"]
        bench("kms Encrypt 64 B (symmetric)", lambda: kms.encrypt(KeyId=kid, Plaintext=b"x" * 64))
        bench("kms Decrypt", lambda: kms.decrypt(CiphertextBlob=ct))
        bench("kms GenerateDataKey AES_256", lambda: kms.generate_data_key(KeyId=kid, KeySpec="AES_256"))
        ek = kms.create_key(KeySpec="ECC_NIST_P256", KeyUsage="SIGN_VERIFY")["KeyMetadata"]["KeyId"]
        bench("kms Sign ECDSA_SHA_256", lambda: kms.sign(KeyId=ek, Message=b"m", SigningAlgorithm="ECDSA_SHA_256"))
        bench("sts GetCallerIdentity", lambda: sts.get_caller_identity())
        bench("sts AssumeRole (stores a session)", lambda: sts.assume_role(RoleArn="arn:aws:iam::000000000000:role/r", RoleSessionName="sess"))
        print(json.dumps({"backends": a.backends, "rows": rows}))
    finally:
        warp.close()
        for p in pgs:
            p.close()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Run Floci's AWS-SDK compatibility tests (compatibility-tests/ in floci-io/floci, MIT) for one service
against ANY AWS-style endpoint (here: Warp's dynamowire / sqswire / s3wire) and emit JSON + markdown.

  run_floci_compat.py --service dynamodb --endpoint http://localhost:18000 --suite python \
       --floci-dir /path/to/floci [--out results] [--timeout 60] [--tag warp]

Suites: python (pytest+boto3), node (vitest, needs `npm install` in sdk-test-node), java (mvn test -Dtest=..).
The awscli suite (bats) is not wired: it needs aws-cli v2 + bats-core (see README).
Floci's code is never copied into this repo: --floci-dir points at a checkout elsewhere.
"""
import argparse, glob, json, os, re, subprocess, sys, time
import xml.etree.ElementTree as ET
from collections import OrderedDict

# service -> suite -> test targets (files / classes) in Floci's compatibility-tests/
TARGETS = {
    "python": {
        "dynamodb": ["test_dynamodb.py"],
        "sqs": ["test_sqs.py"],
        "s3": ["test_s3.py", "test_s3_cors.py"],
    },
    "node": {
        "dynamodb": ["dynamodb.test.ts", "dynamodb-conformance.test.ts"],
        "sqs": ["sqs.test.ts"],
        "s3": ["s3.test.ts", "s3-cors.test.ts", "s3-multipart-checksum.test.ts"],
    },
    "java": {
        "dynamodb": ["DynamoDbTest", "DynamoDbExpressionTests", "DynamoDbConformanceChangesTest",
                     "DynamoDbAccessPathValidationTest", "DynamoDbPartiQLTest", "DynamoDbEnhancedClientTest",
                     "DynamoDbScanConditionTests", "DynamoDbConcurrencyTest"],
        "sqs": ["SqsTest", "SqsMd5Test"],
        "s3": ["S3Test", "S3FeaturesTest", "S3VirtualHostStyleTest", "S3MultipartChecksumTest", "S3AnnotationsTest"],
    },
}


def first_line(text):
    for ln in (text or "").splitlines():
        ln = ln.strip()
        if ln:
            return ln[:300]
    return ""


A_NAME = ("gsi", "lsi", "index", "tag", "ttl", "updatetable", "continuousbackup", "backup", "legacy", "attributeupdates",
          "queryfilter", "scanfilter", "keyconditions", "attribute_type", "attributetype", "parallelscan",
          "parallel scan", "batch", "transaction", "itemcollectionmetrics", "consumedcapacity", "stream",
          "deadletter", "dlq", "movetask", "longpoll", "long_poll", "versioning", "multipart", "listparts")
C_NAME = ("vector", "replica", "export", "import", "kinesis", "arn", "accessdenied")


def classify(name, msg):
    """Heuristic class; a human pass refines it (see results/*.md). a=not implemented, b=wrong behaviour/shape,
    c=Floci-specific assumption, d=environment/harness."""
    m, n = (msg or "").lower(), (name or "").lower()
    if any(k in m for k in ("connection refused", "econnrefused", "timed out", "read timeout", "connect timeout",
                            "no such file", "importerror", "modulenotfound", "fixture ")):
        return "d"
    if any(k in n for k in C_NAME) or any(k in m for k in ("000000000000", ":4566", "floci")):
        return "c"
    if any(k in m for k in ("unknownoperation", "unsupportedoperation", "not implemented", "notimplemented",
                            "does not implement", "unsupported function", "could not parse", "not supported",
                            "non-key attribute")):
        return "a"
    if any(k in n for k in A_NAME):
        return "a"
    return "b"


def parse_junit(path, service, suite):
    rows = []
    if not os.path.exists(path):
        return rows
    for tc in ET.parse(path).getroot().iter("testcase"):
        cls = tc.get("classname", "")
        name = tc.get("name", "")
        st, msg = "pass", ""
        for tag, s in (("failure", "fail"), ("error", "error"), ("skipped", "skip")):
            el = tc.find(tag)
            if el is not None:
                st = s
                msg = first_line(el.get("message") or el.text or "")
                if not msg:
                    msg = first_line(el.text or "")
                break
        rows.append({"file": cls.split(".")[-2] + ".py" if suite == "python" and "." in cls else cls,
                     "test": name, "status": st, "message": msg, "time": float(tc.get("time") or 0)})
    return rows


def run_python(floci, service, env, timeout, out_xml):
    d = os.path.join(floci, "compatibility-tests", "sdk-test-python")
    files = [f"tests/{t}" for t in TARGETS["python"][service]]
    cmd = [sys.executable, "-m", "pytest", *files, "-o", "addopts=", "-p", "no:cacheprovider", "-q", "--tb=line",
           f"--junit-xml={out_xml}", f"--timeout={timeout}"]
    return cmd, d


def run_node(floci, service, env, timeout, out_xml):
    d = os.path.join(floci, "compatibility-tests", "sdk-test-node")
    files = [f"tests/{t}" for t in TARGETS["node"][service]]
    env["TEST_RESULTS_DIR"] = os.path.dirname(out_xml)
    cmd = ["npx", "vitest", "run", *files, "--testTimeout", str(timeout * 1000),
           "--reporter=junit", f"--outputFile.junit={out_xml}"]
    return cmd, d


def run_java(floci, service, env, timeout, out_xml):
    d = os.path.join(floci, "compatibility-tests", "sdk-test-java")
    cmd = ["mvn", "-q", "-B", "test", "-Dtest=" + ",".join(TARGETS["java"][service]),
           "-Dsurefire.failIfNoSpecifiedTests=false", "-DfailIfNoTests=false"]
    out_xml_glob = os.path.join(d, "target", "surefire-reports", "TEST-*.xml")
    return cmd, d, out_xml_glob


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--service", required=True, choices=["s3", "dynamodb", "sqs"])
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--suite", default="python", choices=["python", "node", "java"])
    ap.add_argument("--floci-dir", required=True)
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    ap.add_argument("--timeout", type=int, default=60, help="per-test timeout seconds")
    ap.add_argument("--total-timeout", type=int, default=1800)
    ap.add_argument("--tag", default="warp")
    ap.add_argument("--access-key", default="test")
    ap.add_argument("--secret-key", default="test")
    ap.add_argument("--region", default="us-east-1")
    a = ap.parse_args()

    os.makedirs(a.out, exist_ok=True)
    env = dict(os.environ, FLOCI_ENDPOINT=a.endpoint, AWS_ACCESS_KEY_ID=a.access_key,
               AWS_SECRET_ACCESS_KEY=a.secret_key, AWS_DEFAULT_REGION=a.region, AWS_REGION=a.region,
               AWS_ENDPOINT_URL=a.endpoint)
    base = f"{a.tag}-{a.service}-{a.suite}"
    xml = os.path.abspath(os.path.join(a.out, base + ".junit.xml"))
    if a.suite == "java":
        cmd, cwd, xml_glob = run_java(a.floci_dir, a.service, env, a.timeout, xml)
    else:
        cmd, cwd = {"python": run_python, "node": run_node}[a.suite](a.floci_dir, a.service, env, a.timeout, xml)
    if a.suite == "java":
        for old in glob.glob(xml_glob):
            os.remove(old)  # stale reports from an earlier service run
    t0 = time.time()
    log = os.path.join(a.out, base + ".log")
    with open(log, "w") as lf:
        try:
            r = subprocess.run(cmd, cwd=cwd, env=env, stdout=lf, stderr=subprocess.STDOUT, timeout=a.total_timeout)
            rc = r.returncode
        except subprocess.TimeoutExpired:
            rc = -1
            lf.write("\n[runner] total timeout exceeded\n")
    if a.suite == "java":
        rows = []
        for f in glob.glob(xml_glob):
            rows += parse_junit(f, a.service, "java")
    else:
        rows = parse_junit(xml, a.service, a.suite)
    for r_ in rows:
        r_["class"] = classify(r_["test"], r_["message"]) if r_["status"] in ("fail", "error") else ""
    counts = OrderedDict((k, sum(1 for r_ in rows if r_["status"] == k)) for k in ("pass", "fail", "error", "skip"))
    per_file = OrderedDict()
    for r_ in rows:
        pf = per_file.setdefault(r_["file"], OrderedDict((k, 0) for k in ("pass", "fail", "error", "skip")))
        pf[r_["status"]] += 1
    summary = {"service": a.service, "suite": a.suite, "endpoint": a.endpoint, "tag": a.tag,
               "date": time.strftime("%Y-%m-%d %H:%M"), "runner_rc": rc, "seconds": round(time.time() - t0, 1),
               "counts": counts, "per_file": per_file, "cmd": " ".join(cmd),
               "failures": [r_ for r_ in rows if r_["status"] in ("fail", "error")]}
    json.dump(summary, open(os.path.join(a.out, base + ".json"), "w"), indent=1)
    md = [f"# Floci {a.suite} / {a.service} vs {a.tag} ({summary['date']})", "",
          f"endpoint `{a.endpoint}`, runner rc={rc}, {summary['seconds']}s", "",
          "| status | count |", "|---|---|", *[f"| {k} | {v} |" for k, v in counts.items()], "",
          "| file | pass | fail | error | skip |", "|---|---|---|---|---|",
          *[f"| {f} | {c['pass']} | {c['fail']} | {c['error']} | {c['skip']} |" for f, c in per_file.items()], "",
          "## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)", ""]
    for r_ in summary["failures"]:
        md.append(f"- [{r_['class']}] `{r_['file']}::{r_['test']}` -- {r_['message']}")
    open(os.path.join(a.out, base + ".md"), "w").write("\n".join(md) + "\n")
    print(json.dumps({"counts": counts, "rc": rc, "seconds": summary["seconds"]}))
    if not rows:
        print(f"no results parsed; see {log}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()

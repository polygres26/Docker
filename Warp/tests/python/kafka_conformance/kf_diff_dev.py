"""Developer helper: replay the corpus against a running broker (Warp or Kafka) and print the differences with the golden recording.
   python3 kf_diff_dev.py HOST PORT [case-substring]"""
import json
import sys

import kf_corpus as C
import kf_harness as H

host, port = sys.argv[1], int(sys.argv[2])
only = sys.argv[3] if len(sys.argv) > 3 else ""
H.warmup(host, port)
golden = H.load_golden()["cases"]
tot = [0, 0, 0, 0]
for fn in C.cases():
    if only not in fn.__name__ or fn.__name__ not in golden:
        continue
    got = H.run_case(fn, host, port, tag="Zq0")
    ident, known, unstable, bad, _ = H.compare(fn.__name__, golden[fn.__name__], got)
    tot[0] += ident
    tot[1] += known
    tot[2] += unstable
    tot[3] += len(bad)
    print("%-40s identical=%d known=%d unstable=%d UNEXPLAINED=%d" % (fn.__name__, ident, known, unstable, len(bad)), flush=True)
    for lab, path, a, b in bad[:12]:
        print("    %s %s\n        golden: %s\n        warp  : %s" % (lab, path, json.dumps(a)[:400], json.dumps(b)[:400]))
print("TOTAL identical=%d known=%d unstable=%d unexplained=%d" % tuple(tot))

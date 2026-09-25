#!/usr/bin/env python3
"""Classifies the failures of a run_spec.py run and writes results/spec_summary.json + results/spec_failures.tsv.

  classify.py warp.json [oracle.json]

Classes: a = feature not implemented (clear OpenSearch-style error), b = wrong behaviour / not yet matching,
c = OpenSearch/Lucene-specific and infeasible (or deliberately different) on Postgres, d = harness limitation.
"""
import collections
import json
import os
import re
import sys

RULES = [  # (class, label, predicate over (test key, message))
    ("a", "intervals query", lambda k, m: "230_interval" in k),
    ("a", "profile API", lambda k, m: "profil" in k.lower() or "[profile]" in m),
    ("a", "significant_terms / significant_text", lambda k, m: "sig_terms" in k or "sig_text" in k or "significant_" in m),
    ("a", "unsigned_long", lambda k, m: "unsigned" in k),
    ("a", "more_like_this", lambda k, m: k.startswith("mlt/")),
    ("a", "geo shape/sort/aggregations", lambda k, m: "geo" in k and "80_geo_point" not in k or "geo_shape" in m or "_geo_distance" in m),
    ("a", "auto_date_histogram / ip_range / range field types / date_nanos", lambda k, m: any(x in k for x in ("auto_date", "ip_range", "date_nanos", "range_field")) or "integer_range" in m or "ip_range" in m),
    ("a", "flat_object / token_count / stored & doc-values field variants", lambda k, m: "flat_object" in k or "330_fetch_fields" in k or "20_stored_fields" in k or "340_doc_values" in k),
    ("a", "scripted aggregations (moving_fn, bucket_script), rescore, script_fields, span queries, terms lookup, suggest, sliced scroll",
     lambda k, m: "not supported by Warp" in m or "terms lookup" in m or "scripted aggregations" in m or "moving_fn" in k or "bitmap" in k or "slices" in k),
    ("c", "hdr histogram percentile precision", lambda k, m: "hdr_metric" in k),
    ("c", "routing (a document lives on the host its _id hashes to; custom routing is ignored)", lambda k, m: "routing" in k.lower() or "_routing" in m),
    ("c", "refresh / realtime visibility (writes are visible at once unless refresh_interval is -1)", lambda k, m: "refresh" in k.lower() or "realtime" in k.lower()),
    ("c", "Lucene / shard internals (request cache, batched reduce phases, pre-filter shard skipping, fielddata stats, _ignored, partitions)",
     lambda k, m: any(x in m for x in ("request_cache", "num_reduce_phases", "_shards.skipped", "fielddata", "_ignored", "partition")) or "batch_reduce" in k or "pre_filter" in k),
    ("c", "single-node/cluster details (node roles, transport, settings, discovery, plugin-provided aggregation catalog)",
     lambda k, m: k.startswith(("nodes.info", "cluster.stats", "cluster.health", "cat.indices", "indices.flush", "indices.get_settings"))),
    ("b", "other differences", lambda k, m: True),
]


def main():
    warp = json.load(open(sys.argv[1]))
    here = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(here, exist_ok=True)
    per_dir = collections.defaultdict(lambda: [0, 0])
    rows, by_class = [], collections.Counter()
    for k, (status, msg) in sorted(warp.items()):
        d = k.split("/")[0]
        per_dir[d][1] += 1
        if status == "pass":
            per_dir[d][0] += 1
            continue
        for cls, label, pred in RULES:
            if pred(k, msg):
                rows.append((k.replace("::", " :: "), cls, label, msg.replace("\n", " ")[:200]))
                by_class[cls] += 1
                break
    total = [sum(v[0] for v in per_dir.values()), sum(v[1] for v in per_dir.values())]
    json.dump({"passed": total[0], "valid_on_real_opensearch": total[1], "failures_by_class": dict(by_class),
               "per_directory": {d: {"passed": v[0], "total": v[1]} for d, v in sorted(per_dir.items())}},
              open(os.path.join(here, "spec_summary.json"), "w"), indent=1)
    with open(os.path.join(here, "spec_failures.tsv"), "w") as f:
        f.write("test\tclass\tcategory\tmessage\n")
        for r in rows:
            f.write("\t".join(r) + "\n")
    print(total, dict(by_class))


if __name__ == "__main__":
    main()

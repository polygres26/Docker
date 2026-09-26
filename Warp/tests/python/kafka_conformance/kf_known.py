"""Documented differences between Warp's kafkawire and a real Apache Kafka 4.3 (KRaft, single node), as seen by the conformance corpus.

Every entry: (case regex, step-label regex, JSON-path regex, reason). A step that differs from the golden recording is either identical,
matched by an entry here (a known, explained divergence), or UNEXPLAINED -- and any unexplained difference fails test_kafka_conformance.py.
Normalization (kf_harness.Ctx.norm) already removes what differs by construction: broker/node ids, hosts and ports, member ids, producer
ids, topic ids, session ids, throttle times, cluster id and the free-text of error messages.
"""
import re

KNOWN = [
    # -------------------------------------------------------------------------------------------------------- transactions
    (r"case_idempotent", r"init_transactional", r".*",
     "Transactions are not implemented: InitProducerId with a transactional.id answers UNSUPPORTED_VERSION (35) instead of granting a producer "
     "id; the API keys AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/TxnOffsetCommit are not advertised. Idempotent (non-transactional) producers work."),
    # -------------------------------------------------------------------------------------------------------- configs
    (r"case_describe_configs", r"describe_topic_v.*", r"\.configs\.min\.insync\.replicas\[1\]",
     "min.insync.replicas is a dynamic-default BROKER config (source 3) on the recorded broker because the container sets it; Warp reports it as a "
     "plain default (source 5). Value (1) is identical."),
]


def match(case, label, path):
    for c, l, p, reason in KNOWN:
        if re.fullmatch(c, case) and re.fullmatch(l, label) and re.fullmatch(p, path):
            return reason
    return None

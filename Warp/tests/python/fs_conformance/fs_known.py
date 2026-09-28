"""Documented differences between Warp's firestorewire and the official Firestore emulator (the oracle) that the differential
harness must not fail on. Keys are "case#step" or "case#first-last". Each entry says WHY: where the emulator deviates from real
Firestore, Warp implements real Firestore and the reason is given here (see docs/WARP_GUIDE.md, "The Firestore store").
"""

# steps where the answers differ in substance
KNOWN = {
    # the emulator does not implement PartitionQuery (UNIMPLEMENTED); Warp answers with a single partition (no split points)
    "batch#13-15": "PartitionQuery: emulator UNIMPLEMENTED",
    # several writes to one document in one commit: the emulator collapses them and reports the FINAL update_time for each; Warp
    # reports each write's own result (a delete has no update_time)
    "commit_masks#18": "duplicate writes to one doc: write_results",
    # the emulator accepts a document name of another project and writes it under the request's project; Warp rejects it
    "commit_masks#42": "project mismatch accepted by the emulator",
    # the emulator (built on Cloud Datastore) cannot scan keys in descending order; real Firestore can, and so does Warp
    "groups#6": "emulator: descending key scans unsupported",
    "order_cursors#4": "emulator: descending key scans unsupported",
    "order_cursors#21": "emulator: descending key scans unsupported",
    # skipped_results: the emulator attaches it to the first document message in some cases and sends it standalone in others;
    # Warp always sends one standalone message before the first document (only when an offset and a limit are set)
    "order_cursors#7": "skipped_results placement",
    "query_edges#9": "skipped_results placement",
    "query_edges#12": "skipped_results placement",
    # the emulator accepts find_nearest and returns nothing
    "query_edges#28": "find_nearest: emulator returns no rows",
    # the emulator's REST layer adds a done flag to RunAggregationQuery and answers an old readTime with its own text/status
    "rest#13": "REST: done flag on aggregation",
    "rest#22": "REST: old readTime",
    # Warp uses optimistic transactions (validated at commit) where the emulator locks pessimistically; see the contention tests
    # in test_firestore_conformance.py
    # emulator artifact: 'Cannot upsert then insert an entity in the same request' (Datastore); Warp answers ALREADY_EXISTS
    "transforms#32": "upsert then create in one commit",
}

# steps where only the error MESSAGE text differs (same gRPC status code, same result otherwise): the emulator's texts leak its
# Cloud Datastore implementation ("no entity to update: app: ...", "entity already exists: EntityRef[...]"); Warp uses the texts
# real Firestore returns
MSG_ONLY = {
    "crud#2": "ALREADY_EXISTS text",
    "crud#4": "NOT_FOUND text",
    "crud#10": "NOT_FOUND text",
    "crud#11": "ALREADY_EXISTS text",
    "crud#18": "NOT_FOUND text",
    "crud#43": "reserved id text (the emulator applies a Datastore rule)",
    "batch#6": "status texts inside BatchWrite",
    "batch#8": "NOT_FOUND text",
    "batch#9": "NOT_FOUND text",
    "commit_masks#16": "NOT_FOUND text",
    "commit_masks#17": "NOT_FOUND text",
    "query_edges#29": "text",
    "rest#2": "NOT_FOUND text",
    "rest#4": "ALREADY_EXISTS text",
    "rest#8": "NOT_FOUND text",
    "rest#18": "NOT_FOUND text",
    "rest#20": "invalid JSON payload text",
    "rest#21": "invalid JSON payload text",
    "write_stream#4": "NOT_FOUND text",
    "write_stream#5": "NOT_FOUND text",
    "write_stream#9": "NOT_FOUND text",
    "types_special#16": "nested array inside a map: emulator says 'invalid nested entity'",
}

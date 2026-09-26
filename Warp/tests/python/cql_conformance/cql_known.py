"""Every divergence between Warp's cqlwire and the recorded Apache Cassandra 5.0.9 answers, each with its reason.

Policy: Warp follows real Cassandra. Error CODES and result rows are compared exactly; an error MESSAGE that differs only cosmetically is
listed here (never hidden): the test fails both for a mismatch that is not listed and for a listed entry that no longer occurs.

MESSAGE_PATTERNS: (regex on the recorded Cassandra message, reason). A step whose error class matches but whose message differs passes only
if the recorded message matches one of these.
RESULT_DIVERGENCES: {(case, step index): reason} for steps whose recorded result differs semantically.
"""
import re

MESSAGE_PATTERNS = [
    (re.compile(r"^<Error from server: code=2000 \[Syntax error in CQL query\] message=\"(line \d+:-?\d+ |Failed parsing|Unknown property)"),
     "Syntax error text: Cassandra's ANTLR parser prints token names (K_FROM), the surrounding input '(...[offending]...)' and 'line 0:-1' for "
     "end of input; Warp's hand written parser reports its own position and token wording. Same error class (SyntaxException, code 0x2000), "
     "same rejected statements. Cosmetic."),
    (re.compile(r"^Could not decode JSON string as a map: com\.fasterxml"),
     "JSON syntax errors quote Jackson's parser message; Warp's JSON parser is Gson and quotes its own. Same error class."),
    (re.compile(r"^Invalid call to function system\.cast_as_\w+, none of its type signatures match"),
     "Cassandra lists every overload of the generated cast function; Warp says 'Cannot cast text to int'. Same error class."),
    (re.compile(r"^org\.apache\.cassandra\.auth\.CassandraRoleManager doesn't support PASSWORD"),
     "Warp has no role manager: CREATE ROLE and friends are refused with their own text (InvalidRequest, same class)."),
]

# (case, step index) -> reason
RESULT_DIVERGENCES = {
    ("counters", 15): "Cassandra resurrects the pre-delete counter value after DELETE + UPDATE (-100 + 1 = -99: a counter tombstone does not "
                      "reset its shards); Warp deletes the counter cell, so the next increment starts from 0 (1). Cassandra documents that "
                      "deleted counters must not be reused.",
}

# Cassandra behaviours Warp deliberately does not reproduce (no recorded step depends on them):
NOT_IMPLEMENTED = [
    "Native protocol v5 (framing with CRC): the server answers a v5 STARTUP with the 'Invalid or unsupported protocol version' error so every "
    "driver negotiates v4.",
    "Frame compression (LZ4/Snappy): SUPPORTED lists no compression algorithm.",
    "Tracing, custom payloads (skipped on read), speculative execution hints, read/write timeouts and consistency levels (accepted and ignored).",
    "Materialized views, user defined functions and aggregates, triggers, roles and permissions (the statements are refused with Cassandra's own "
    "'disabled' wording where it has one).",
    "Non-frozen UDT columns: stored and replaced as a whole value, assignment to one field (col.f = x) is refused.",
    "Tombstones are cells with a gc_grace_seconds (10 days) lifetime and no compaction: cell, row, collection (range) and clustering-range tombstones "
    "shadow older writes like Cassandra's (verified by 34 seeded random-operation cases), but they are stored, scanned and swept like data.",
    "Cross partition batches are applied per backend (one transaction per Postgres host), not atomically across hosts; logged batches have no "
    "batchlog.",
    "Secondary indexes are catalog entries: indexed queries scan and filter (same results, no separate index structure); SASI/SAI options are stored, not used.",
    "Static counters and counters in collections are not supported; SELECT ... GROUP BY on non-key expressions is refused as in Cassandra.",
]

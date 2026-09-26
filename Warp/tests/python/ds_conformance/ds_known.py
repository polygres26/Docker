"""Documented differences between Warp's datastorewire and the official Datastore emulator (the oracle). Keys are "case#step" or
"case#first-last"; each entry says why (see docs/WARP_GUIDE.md, "The Datastore store").

The emulator is the LEGACY Cloud Datastore: it lacks RunAggregationQuery, IN / NOT_IN / NOT_EQUAL / OR, only allows one inequality
property with a matching first sort order, only ancestor queries inside transactions, and always answers moreResults =
MORE_RESULTS_AFTER_LIMIT. Warp implements the current Datastore API (Firestore in Datastore mode lifts those restrictions), so the
steps that exercise them are listed here and covered by Warp-side tests in test_datastore_conformance.py instead.
"""

KNOWN = {
    "aggregations#1-17": "emulator: RunAggregationQuery unimplemented",
    "crud#39": "emulator: an invalid transaction id is UNKNOWN with no message (an NPE); Warp: INVALID_ARGUMENT",
    "filters#5-7": "emulator: NOT_EQUAL / IN / NOT_IN unsupported",
    "filters#11-12": "emulator: OR unsupported",
    "filters#14": "emulator: legacy 'one inequality property' rule",
    "filters#18": "emulator: IN unsupported",
    "filters#19": "emulator: legacy 'first sort property must be the inequality property' rule",
    "filters#38-40": "emulator: legacy 'first sort property must be the inequality property' rule",
    "projection#6": "emulator ignores distinct_on properties that are not projected; Warp dedupes on them",
    "gql#24": "emulator: no IN in GQL",
    "gql#25": "emulator: != unsupported",
    "gql#28": "emulator: RunAggregationQuery unimplemented",
    "rest#8": "emulator: RunAggregationQuery unimplemented",
    "transactions#16": "emulator: only ancestor queries inside transactions (legacy); Warp allows any query",
    "values_order#29-31": "emulator: NOT_EQUAL / IN / NOT_IN unsupported",
    "values_order#33-34": "emulator projects timestamps as their integer micros and blobs as empty strings (index representation); Warp returns the typed value",
}

MSG_ONLY = {
    "gql#15": "GQL syntax error text (JavaCC parser messages)",
    "gql#16": "GQL syntax error text (JavaCC parser messages)",
    "gql#18": "GQL syntax error text (JavaCC parser messages)",
    "rest#11": "invalid JSON payload text",
    "rest#12": "invalid JSON payload text",
}

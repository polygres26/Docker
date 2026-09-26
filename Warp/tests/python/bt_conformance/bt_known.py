"""Known differences between Google's Bigtable emulator (the oracle) and Warp. Warp implements the real Cloud Bigtable behaviour where
the emulator is lenient or incomplete; everywhere else it reproduces the emulator, including its status codes and message texts
(the emulator answers UNKNOWN for many validation errors that real Bigtable reports as INVALID_ARGUMENT or NOT_FOUND: those are
kept as the emulator has them because there is no real service here to check against). Key "case#step" or "case#a-b" (inclusive);
value (kind, reason): kind "msg" (status text differs), "body" (response body differs, also covers msg), "code" (status code
differs, also covers the others), "any".

MSG_FAMILIES: status texts that are the same error in different words (regular expression syntax errors come from Go's regexp in the
emulator and from java.util.regex here): two texts of one family are treated as equal and counted as "known".
WARP_STRIP: (field, value, rpcs) fields Warp returns as real Bigtable does and the emulator omits; removed from Warp's side before
comparing.
"""
import re

MSG_FAMILIES = [
    (re.compile(r"^Error in field '(row_key|family_name|column_qualifier|value)_regex_filter' : error parsing regexp: .*$"), "REGEXP"),
]

# real Bigtable returns the table's granularity in the SCHEMA and FULL views; the emulator omits it on GetTable / ListTables
WARP_STRIP = [("granularity", "MILLIS", ("GetTable", "ListTables"))]

_ID = "emulator accepts what real Bigtable rejects: table ids and column family names must match [_a-zA-Z0-9][-_.a-zA-Z0-9]*, parents projects/*/instances/*"
KNOWN = {
    "table_crud#5": ("body", "the emulator ignores GetTable's NAME_ONLY view and returns the families; real Bigtable returns the name only"),
    "table_crud#10": ("body", "the emulator ignores ListTables' FULL view and returns names only; real Bigtable returns the schema"),
    "table_names#0-3": ("code", _ID),
    "table_names#5-7": ("code", _ID),
    "modify_families#13-14": ("code", _ID),
    "modify_families#15": ("body", "cascade of modify_families#13-14: the emulator created the invalid families"),
    "modify_families#17": ("body", "the emulator applies a ModifyColumnFamilies request modification by modification (create n2 stays although the drop of "
                                   "an unknown family fails); real Bigtable applies all modifications or none, as Warp does; plus the cascade of #13-14"),
    "update_table#1": ("body", "the operation's response is the full Table on real Bigtable; the emulator answers name and deletion_protection only"),
    "update_table#4": ("body", "same: the emulator's Operation carries a reduced Table"),
    "mutate_basic#5": ("body", "the emulator applies the mutations of a MutateRow before the failing one; real Bigtable (and Warp) applies a row's "
                              "mutations atomically, so an invalid timestamp rejects the whole request"),
    "mutate_basic#14": ("body", "same: the emulator kept the valid set_cell before the unknown family"),
    "filters_transformers#7": ("body", "the emulator's strip_value_transformer drops the labels an earlier filter set; the filter only replaces the value "
                                       "(Warp keeps the labels)"),
    "read_rowset#38": ("code", "the emulator accepts a negative rows_limit and returns nothing; real Bigtable rejects it"),
    "read_rowset#43-45": ("code", "the emulator does not support reversed scans (UNIMPLEMENTED); real Bigtable and Warp do (see test_bigtable_conformance.py)"),
    "filters_errors#13": ("code", "the emulator validates a filter only when a row reaches it, so an invalid chain over no rows succeeds; real Bigtable (and "
                                  "Warp) validates the request first"),
    "check_and_mutate#17": ("code", "the emulator only validates the mutations of the branch that runs; real Bigtable (and Warp) validates both"),
    "check_and_mutate#19": ("code", "same as check_and_mutate#17: the emulator skips validating the mutations of the branch that does not run"),
    "rmw_increment#20": ("code", "the emulator accepts an empty row key in ReadModifyWriteRow (and stores a row with an empty key); real Bigtable rejects it"),
    "drop_row_range#16": ("code", "the emulator treats an empty row_key_prefix as 'every row'; real Bigtable rejects an empty prefix (Warp too: delete_all_data_from_table exists for that)"),
}

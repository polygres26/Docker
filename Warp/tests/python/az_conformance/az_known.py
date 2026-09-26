"""Documented, accepted differences between Warp and Azurite that are NOT already covered by a step's known_diff flag.

Two verdict classes beyond `same`: `msg` (same failing HTTP status, a different error code / message text / error body --
Azurite is lax about many things real Azure rejects, and sometimes uses its own codes; Warp follows the documented Azure
codes) and `known` (a step flagged known_diff or listed here: only the differing behaviour is accepted). Reasons:

  A  Azurite accepts what real Azure rejects (lax validation): malformed If-* dates, maxresults=0, public-access values,
     $top=0, Int32 overflow, lease ops on leased containers, SAS/anonymous edge cases.
  B  Azurite-specific quirks or bugs (Range 'bytes=-N' echo, empty error bodies, no Content-MD5 on Put Block, blob list
     BlobPrefix ordering and paging with a delimiter, tag header parsing keeping only the first tag, batch needing real
     sub-request signatures, service stats rejected, GetBlockList/append-blob MD5 values).
  C  Warp deliberately follows real Azure: MessageTooLarge (400) instead of Azurite's 413, x-ms-version echoed, the
     AuthenticationFailed body carries the expected string-to-sign, Content-MD5 returned on writes.
"""
KNOWN = {}

# "case#step": class letter (see above). Applied as known_diff (only the status class must agree, or nothing when statuses differ
# because the oracle is lax).
KNOWN.update({
    "blob_container_lifecycle#14": "A", "blob_container_acl_public#11": "A", "blob_container_lease#6": "A",
    "blob_container_lease#10": "A", "blob_container_lease#15": "B", "blob_container_lease#16": "B",
    "blob_get_ranges#4": "B", "blob_get_ranges#6": "B", "blob_get_ranges#9": "B", "blob_conditions#4": "B",
    "blob_conditions#6": "B", "blob_conditions#15": "A", "blob_properties_metadata#6": "B", "blob_blocks#11": "B",
    "blob_blocks#16": "B", "blob_append#6": "B", "blob_page#7": "B", "blob_page#12": "B", "blob_leases#16": "B",
    "blob_leases#18": "A", "blob_batch#4": "B", "blob_auth#2": "B", "blob_auth#6": "B", "blob_auth#7": "B",
    "table_entities_basic#20": "A", "table_update_merge#17": "A", "table_acl_service#1": "B", "table_acl_service#2": "B",
    "table_auth#12": "B",
})

"""Documented, accepted differences between Warp (real GCS behaviour) and fake-gcs-server (an emulator, not GCS) that are NOT already
covered by a step's known_diff flag.

Verdict classes beyond `same`: `msg` (same failing HTTP status, different error text: fake-gcs-server answers plain "Not Found" /
"Precondition failed" style messages where GCS has specific ones) and `known` (a step flagged known_diff or listed here: only the
differing behaviour is accepted). Reasons (letters):

  A  fake-gcs-server ignores or does not implement it (bucket PATCH body, object metageneration, preconditions on PATCH/GET/
     DELETE/copy, ifGenerationMatch on resumable initiation, HMAC keys, IAM, service account, batch, XML PUT/list details).
  B  fake-gcs-server bug or lax validation (accepts invalid bucket/object names and query values, resumable session usable
     after cancel, Range header after the final chunk, persists an unaligned non-final chunk, 308 answers carry a JSON body,
     the resumable start answers with an object body and generation "0").
  R  fake-gcs-server's resumable protocol is not GCS's: it finalises the object on the first `Content-Range: bytes */*` request,
     answers status queries and 308s with an object body and a Range that includes an extra byte, persists an unaligned
     non-final chunk, lets a cancelled session be used again and answers the start request with an object body. Warp follows
     the documented protocol (gcs_conformance resumable tests in test_gcs_conformance.py assert the real behaviour).
  C  Warp follows real GCS documentation (409 for a non-empty bucket delete where fake says 412, composed objects have no
     md5Hash, ETag is opaque, error messages, 401 for anonymous callers when auth is on, resumable start returns an empty body).
"""
KNOWN = {
    "acl_bucket_object#6-7": "C",
    "acl_bucket_object#11": "C",
    "bucket_create_fields#0-1": "A",
    "bucket_delete_nonempty#2": "C",
    "bucket_lifecycle#4-8": "A",
    "bucket_metageneration_preconditions#2-7": "A",
    "bucket_missing_project#0-1": "B",
    "bucket_names#3": "B",
    "bucket_settings_stored#1-2": "A",
    "bucket_versioning#1-4": "A",
    "errors_misc#2": "B",
    "errors_misc#4": "B",
    "errors_misc#6": "B",
    "list_options#12-16": "A",
    "list_paging#12": "B",
    "list_paging#17": "B",
    "obj_compose#4-12": "C",
    "obj_compose#14-16": "C",
    "obj_compose_32#2-4": "C",
    "obj_copy#3": "A",
    "obj_copy#9-10": "A",
    "obj_copy#13-15": "A",
    "obj_get_preconditions#3": "A",
    "obj_get_preconditions#5-7": "A",
    "obj_hash_validation#5-6": "B",
    "obj_media_empty_and_binary#7-8": "C",
    "obj_multipart_name_in_query#1-3": "B",
    "obj_name_validation#3-5": "B",
    "obj_patch_update#3-8": "A",
    "obj_preconditions#6-10": "A",
    "obj_projection_fields#2-6": "A",
    "obj_rewrite#7": "A",
    "obj_versioning#4": "B",
    "obj_versioning#16": "B",
    "resumable_cancel#1-2": "R",
    "resumable_cancel#5": "R",
    "resumable_chunked#1-9": "R",
    "resumable_errors#1-2": "R",
    "resumable_errors#4": "R",
    "resumable_preconditions#2-3": "R",
    "resumable_query_and_resume#1-5": "R",
    "resumable_single_request#1": "R",
}

# Floci python / s3 vs warp-baseline (2026-09-25 14:37)

endpoint `http://localhost:53213`, runner rc=1, 1.6s

| status | count |
|---|---|
| pass | 17 |
| fail | 25 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| test_s3.py | 16 | 13 | 0 | 0 |
| test_s3_cors.py | 1 | 12 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [b] `test_s3.py::test_create_bucket_with_location_constraint` -- AssertionError: assert None == 'eu-central-1'
- [a] `test_s3.py::test_put_object_tagging` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutObjectTagging operation: s3wire does not implement this operation (PUT [tagging])
- [a] `test_s3.py::test_get_object_tagging` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutObjectTagging operation: s3wire does not implement this operation (PUT [tagging])
- [a] `test_s3.py::test_delete_object_tagging` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutObjectTagging operation: s3wire does not implement this operation (PUT [tagging])
- [a] `test_s3.py::test_put_bucket_tagging` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketTagging operation: s3wire does not implement this operation (PUT [tagging])
- [a] `test_s3.py::test_get_bucket_tagging` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketTagging operation: s3wire does not implement this operation (PUT [tagging])
- [a] `test_s3.py::test_delete_bucket_tagging` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketTagging operation: s3wire does not implement this operation (PUT [tagging])
- [a] `test_s3.py::test_multipart_upload_sha256_reports_composite_checksum` -- KeyError: 'ChecksumSHA256'
- [a] `test_s3.py::test_multipart_upload_default_checksum_is_composite_crc32` -- KeyError: 'ChecksumType'
- [b] `test_s3.py::test_single_part_upload_keeps_full_object_checksum` -- KeyError: 'ChecksumSHA256'
- [a] `test_s3.py::test_copy_of_multipart_object_is_a_single_full_object` -- KeyError: 'ChecksumType'
- [a] `test_s3.py::test_get_object_attributes_etag_has_no_quotes` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the GetObjectAttributes operation: s3wire does not implement this operation (GET [attributes])
- [b] `test_s3.py::test_download_with_checksum_mode_works_for_composite_and_full_object` -- KeyError: 'ChecksumSHA256'
- [a] `test_s3_cors.py::test_put_bucket_cors_wildcard` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_wildcard_preflight_returns_200` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_wildcard_actual_get_returns_cors_headers` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_get_without_origin_has_no_cors_headers` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_options_without_origin_has_no_cors_headers` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_specific_origin_echoes_origin` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_non_matching_origin_returns_403` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_non_matching_method_returns_403` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_delete_bucket_cors` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_subdomain_wildcard_matches` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_subdomain_wildcard_rejects_wrong_scheme` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])
- [a] `test_s3_cors.py::test_subdomain_wildcard_rejects_different_domain` -- botocore.exceptions.ClientError: An error occurred (NotImplemented) when calling the PutBucketCors operation: s3wire does not implement this operation (PUT [cors])

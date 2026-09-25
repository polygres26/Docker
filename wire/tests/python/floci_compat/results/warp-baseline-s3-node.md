# Floci node / s3 vs warp-baseline (2026-09-25 14:38)

endpoint `http://localhost:55450`, runner rc=1, 1.2s

| status | count |
|---|---|
| pass | 20 |
| fail | 15 |
| error | 0 |
| skip | 0 |

| file | pass | fail | error | skip |
|---|---|---|---|---|
| tests/s3-cors.test.ts | 7 | 8 | 0 | 0 |
| tests/s3-multipart-checksum.test.ts | 0 | 3 | 0 | 0 |
| tests/s3.test.ts | 13 | 4 | 0 | 0 |

## Failures (class: a=not implemented, b=wrong behaviour, c=Floci-specific, d=environment; heuristic)

- [a] `tests/s3-cors.test.ts::S3 CORS Enforcement > should set wildcard CORS config` -- s3wire does not implement this operation (PUT [cors])
- [b] `tests/s3-cors.test.ts::S3 CORS Enforcement > should allow wildcard preflight` -- expected 403 to be 200 // Object.is equality
- [b] `tests/s3-cors.test.ts::S3 CORS Enforcement > should return CORS headers on actual GET with Origin` -- expected undefined to be '*' // Object.is equality
- [a] `tests/s3-cors.test.ts::S3 CORS Enforcement > should set specific origin CORS config` -- s3wire does not implement this operation (PUT [cors])
- [b] `tests/s3-cors.test.ts::S3 CORS Enforcement > should allow matching origin preflight` -- expected 403 to be 200 // Object.is equality
- [b] `tests/s3-cors.test.ts::S3 CORS Enforcement > should echo origin for matching actual GET` -- expected undefined to be 'https://example.com' // Object.is equality
- [a] `tests/s3-cors.test.ts::S3 CORS Enforcement > should reject preflight after CORS config deleted` -- s3wire does not implement this operation (DELETE [cors])
- [a] `tests/s3-cors.test.ts::S3 CORS Enforcement > should support subdomain wildcard pattern` -- s3wire does not implement this operation (PUT [cors])
- [a] `tests/s3-multipart-checksum.test.ts::S3 multipart checksums > reports the composite SHA256 checksum of a multipart upload` -- expected undefined to be 'QKoSoqN0w4CVqh25roMpNp4FjVitT/VruK+UL…' // Object.is equality
- [a] `tests/s3-multipart-checksum.test.ts::S3 multipart checksums > stores what the uploader sends by default and validates the download` -- unexpected checksum on HeadObject: {"AcceptRanges":"bytes","LastModified":"2026-09-25T21:38:51.000Z","ContentLength":12582912,"ETag":"\"51963baeb0dd5592f239a9a1c52621d5-3\"","ContentType":"binary/octet-stream","Metadata":{},"$metadata":{"httpStatusCode":200,"requestId":"145C73751E18451A","attempts":
- [a] `tests/s3-multipart-checksum.test.ts::S3 multipart checksums > writes a copy of the multipart object as a single full object` -- expected undefined to be 'eGxUTvOK2CDEvaSt7ud/QseRZSvDG8R1FkJhm…' // Object.is equality
- [b] `tests/s3.test.ts::S3 > should get bucket location` -- expected undefined to be 'eu-central-1' // Object.is equality
- [b] `tests/s3.test.ts::S3 > should create bucket using signing region when body empty` -- expected undefined to be 'eu-central-1' // Object.is equality
- [b] `tests/s3.test.ts::S3 > should reject explicit us-east-1 LocationConstraint` -- promise resolved "{ …(2) }" instead of rejecting
- [a] `tests/s3.test.ts::S3 > should multipart copy object with non-ASCII key` -- s3wire does not implement this operation (UploadPartCopy [partNumber, uploadId, x-id])

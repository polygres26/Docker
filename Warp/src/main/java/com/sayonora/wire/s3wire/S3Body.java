package com.sayonora.wire.s3wire;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A streamed request body with its declared decoded length, the optional SigV4 payload-hash check and the
 * additional checksums the client declared (headers, or trailers of an {@code aws-chunked} body).
 */
final class S3Body {
    static final Pattern HEX64 = Pattern.compile("^[0-9a-fA-F]{64}$");
    private static final int MAX_XML_BODY = 4 * 1024 * 1024;

    private final HttpServletRequest req;
    private final InputStream stream;
    private final long length;
    private final DigestInputStream digest;
    private final String expectedHex;
    private final AwsChunkedInputStream chunked;

    private S3Body(HttpServletRequest req, InputStream stream, long length, DigestInputStream digest, String expectedHex,
            AwsChunkedInputStream chunked) {
        this.req = req;
        this.stream = stream;
        this.length = length;
        this.digest = digest;
        this.expectedHex = expectedHex;
        this.chunked = chunked;
    }

    InputStream stream() {
        return stream;
    }

    long length() {
        return length;
    }

    boolean verifyDigest() {
        return digest == null || S3SigV4Verifier.hex(digest.getMessageDigest().digest()).equalsIgnoreCase(expectedHex);
    }

    static S3Body open(HttpServletRequest req) throws IOException {
        String sha = req.getHeader("x-amz-content-sha256");
        InputStream in = req.getInputStream();
        long length;
        AwsChunkedInputStream chunked = null;
        if (sha != null && sha.startsWith("STREAMING-")) {
            String decoded = req.getHeader("x-amz-decoded-content-length");
            if (decoded == null) {
                throw new S3WireException(411, "MissingContentLength", "You must provide the Content-Length HTTP header.");
            }
            try {
                length = Long.parseLong(decoded.trim());
            } catch (NumberFormatException e) {
                throw new S3WireException(400, "InvalidArgument", "x-amz-decoded-content-length is not a number");
            }
            Object auth = req.getAttribute("s3wire.chunkAuth");
            chunked = sha.startsWith("STREAMING-AWS4-HMAC-SHA256-PAYLOAD") && auth instanceof S3SigV4Verifier.ChunkAuth ca
                    ? new AwsChunkedInputStream(in, ca) : new AwsChunkedInputStream(in);
            in = chunked;
        } else {
            length = req.getContentLengthLong();
            if (length < 0) {
                throw new S3WireException(411, "MissingContentLength", "You must provide the Content-Length HTTP header.");
            }
        }
        DigestInputStream digest = null;
        if (sha != null && HEX64.matcher(sha).matches()) {
            try {
                digest = new DigestInputStream(in, MessageDigest.getInstance("SHA-256"));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
            in = digest;
        }
        return new S3Body(req, in, length, digest, sha, chunked);
    }

    /** Algorithms the client declared for this request (headers, {@code x-amz-sdk-checksum-algorithm}, trailer names). */
    static Set<String> declaredAlgos(HttpServletRequest req) {
        Set<String> out = new LinkedHashSet<>();
        for (String h : new String[] {"x-amz-sdk-checksum-algorithm", "x-amz-checksum-algorithm"}) {
            String v = req.getHeader(h);
            if (v != null) {
                String a = Checksums.canonical(v);
                if (a == null) {
                    throw new S3WireException(400, "InvalidRequest", "Checksum algorithm provided is unsupported. "
                            + "Please try again with any of the valid types: [CRC32, CRC32C, SHA1, SHA256, CRC64NVME]");
                }
                out.add(a);
            }
        }
        for (String a : Checksums.ALL) {
            if (req.getHeader(Checksums.headerName(a)) != null) {
                out.add(a);
            }
        }
        String trailer = req.getHeader("x-amz-trailer");
        if (trailer != null) {
            for (String t : trailer.split(",")) {
                String n = t.trim().toLowerCase(Locale.ROOT);
                if (n.startsWith("x-amz-checksum-")) {
                    String a = Checksums.canonical(n.substring("x-amz-checksum-".length()));
                    if (a != null) {
                        out.add(a);
                    }
                }
            }
        }
        return out;
    }

    /** Checksums the client claimed: headers plus (after the body was read) trailers. */
    Map<String, String> claimed() {
        Map<String, String> out = new LinkedHashMap<>(Checksums.claimedFrom(n -> req.getHeader(n)));
        if (chunked != null) {
            for (Map.Entry<String, String> t : chunked.trailers().entrySet()) {
                if (t.getKey().startsWith("x-amz-checksum-")) {
                    String a = Checksums.canonical(t.getKey().substring("x-amz-checksum-".length()));
                    if (a != null) {
                        out.put(a, t.getValue());
                    }
                }
            }
        }
        return out;
    }

    /** Small XML-ish request body read fully (config documents, DeleteObjects, CompleteMultipartUpload). */
    static byte[] readSmall(HttpServletRequest req) throws IOException {
        InputStream in = req.getInputStream();
        String sha = req.getHeader("x-amz-content-sha256");
        if (sha != null && sha.startsWith("STREAMING-")) {
            Object auth = req.getAttribute("s3wire.chunkAuth");
            in = sha.startsWith("STREAMING-AWS4-HMAC-SHA256-PAYLOAD") && auth instanceof S3SigV4Verifier.ChunkAuth ca
                    ? new AwsChunkedInputStream(in, ca) : new AwsChunkedInputStream(in);
        }
        byte[] data = in.readNBytes(MAX_XML_BODY + 1);
        if (data.length > MAX_XML_BODY) {
            throw new S3WireException(400, "EntityTooLarge", "Request body too large");
        }
        if (sha != null && HEX64.matcher(sha).matches()) {
            try {
                String actual = S3SigV4Verifier.hex(MessageDigest.getInstance("SHA-256").digest(data));
                if (!actual.equalsIgnoreCase(sha)) {
                    throw new S3WireException(400, "XAmzContentSHA256Mismatch",
                            "The provided 'x-amz-content-sha256' header does not match what was computed.");
                }
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
        String md5 = req.getHeader("Content-MD5");
        if (md5 != null) {
            try {
                byte[] want = java.util.Base64.getDecoder().decode(md5.trim());
                if (!java.util.Arrays.equals(want, MessageDigest.getInstance("MD5").digest(data))) {
                    throw new S3WireException(400, "BadDigest", "The Content-MD5 you specified did not match what we received.");
                }
            } catch (IllegalArgumentException | NoSuchAlgorithmException e) {
                throw new S3WireException(400, "InvalidDigest", "The Content-MD5 you specified was invalid.");
            }
        }
        return data;
    }
}

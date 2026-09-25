package com.sayonora.wire.s3wire;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Decodes the {@code aws-chunked} body framing SigV4 streaming uploads use
 * ({@code hex-size;chunk-signature=...\r\n data \r\n ... 0;chunk-signature=...\r\n [trailers]\r\n}).
 * Per-chunk signatures are parsed past but NOT verified; trailing headers (checksums) are collected in {@link #trailers()}. Both the
 * signed ({@code STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]}) and unsigned ({@code STREAMING-UNSIGNED-PAYLOAD-TRAILER}) variants parse (only the request's
 * seed signature is, see {@link S3SigV4Verifier}).
 */
final class AwsChunkedInputStream extends InputStream {
    private final InputStream in;
    private long remaining;
    private boolean pendingCrlf;
    private boolean done;
    private final java.util.Map<String, String> trailers = new java.util.LinkedHashMap<>();

    /** Set for signed streaming payloads: every chunk (and the trailer) signature is verified. */
    private final S3SigV4Verifier.ChunkAuth auth;
    private String prevSignature;
    private byte[] buf = new byte[0];
    private int bufPos;
    private static final int MAX_SIGNED_CHUNK = 16 * 1024 * 1024;

    AwsChunkedInputStream(InputStream in) {
        this(in, null);
    }

    AwsChunkedInputStream(InputStream in, S3SigV4Verifier.ChunkAuth auth) {
        this.in = in;
        this.auth = auth;
        this.prevSignature = auth == null ? null : auth.seedSignature();
    }

    private static final String EMPTY_SHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    private static String sha256Hex(byte[] data, int off, int len) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
            md.update(data, off, len);
            return S3SigV4Verifier.hex(md.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void verifySignature(String kind, String extraHash, String provided) {
        try {
            String sts = kind + "\n" + auth.amzDate() + "\n" + auth.scope() + "\n" + prevSignature + "\n" + extraHash;
            String expected = S3SigV4Verifier.hex(S3SigV4Verifier.hmacBytes(auth.signingKey(), sts));
            if (provided == null || !java.security.MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                    provided.toLowerCase(java.util.Locale.ROOT).getBytes(StandardCharsets.UTF_8))) {
                throw new S3WireException(403, "SignatureDoesNotMatch",
                        "The request signature we calculated does not match the signature you provided.");
            }
            prevSignature = expected;
        } catch (S3WireException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String chunkSignatureOf(String header) {
        int i = header.indexOf("chunk-signature=");
        return i < 0 ? null : header.substring(i + "chunk-signature=".length()).trim();
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        if (auth != null) {
            return readSigned(b, off, len);
        }
        while (remaining == 0) {
            if (done) {
                return -1;
            }
            if (pendingCrlf) {
                readLine();
                pendingCrlf = false;
            }
            String header = readLine();
            int semi = header.indexOf(';');
            long size;
            try {
                size = Long.parseLong((semi < 0 ? header : header.substring(0, semi)).trim(), 16);
            } catch (NumberFormatException e) {
                throw new IOException("malformed aws-chunked header: " + header);
            }
            if (size == 0) {
                done = true;
                readTrailers();
                return -1;
            }
            remaining = size;
        }
        int n = in.read(b, off, (int) Math.min(len, remaining));
        if (n < 0) {
            throw new EOFException("aws-chunked body truncated");
        }
        remaining -= n;
        if (remaining == 0) {
            pendingCrlf = true;
        }
        return n;
    }

    /** Signed payloads: each chunk is buffered (max 16 MiB) and its signature verified before any byte is released. */
    private int readSigned(byte[] b, int off, int len) throws IOException {
        while (bufPos >= buf.length) {
            if (done) {
                return -1;
            }
            String header = readLine();
            int semi = header.indexOf(';');
            long size;
            try {
                size = Long.parseLong((semi < 0 ? header : header.substring(0, semi)).trim(), 16);
            } catch (NumberFormatException e) {
                throw new IOException("malformed aws-chunked header: " + header);
            }
            if (size > MAX_SIGNED_CHUNK) {
                throw new S3WireException(400, "InvalidArgument", "aws-chunked chunk larger than " + MAX_SIGNED_CHUNK + " bytes");
            }
            byte[] data = new byte[(int) size];
            int got = 0;
            while (got < size) {
                int n = in.read(data, got, (int) size - got);
                if (n < 0) {
                    throw new EOFException("aws-chunked body truncated");
                }
                got += n;
            }
            verifySignature("AWS4-HMAC-SHA256-PAYLOAD", EMPTY_SHA + "\n" + sha256Hex(data, 0, data.length),
                    chunkSignatureOf(header));
            if (size == 0) {
                done = true;
                readSignedTrailers();
                return -1;
            }
            readLine(); // CRLF after the data
            buf = data;
            bufPos = 0;
        }
        int n = Math.min(len, buf.length - bufPos);
        System.arraycopy(buf, bufPos, b, off, n);
        bufPos += n;
        return n;
    }

    /** Trailers of a signed stream end with {@code x-amz-trailer-signature:<sig>} over the canonical trailer text. */
    private void readSignedTrailers() throws IOException {
        StringBuilder canonical = new StringBuilder();
        String trailerSig = null;
        while (true) {
            String line;
            try {
                line = readLine();
            } catch (EOFException e) {
                break;
            }
            if (line.isEmpty()) {
                break;
            }
            int colon = line.indexOf(':');
            if (colon > 0) {
                String name = line.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT);
                String value = line.substring(colon + 1).trim();
                if (name.equals("x-amz-trailer-signature")) {
                    trailerSig = value;
                } else {
                    trailers.put(name, value);
                    canonical.append(name).append(':').append(value).append('\n');
                }
            }
        }
        if (trailerSig != null) {
            byte[] c = canonical.toString().getBytes(StandardCharsets.UTF_8);
            verifySignature("AWS4-HMAC-SHA256-TRAILER", sha256Hex(c, 0, c.length), trailerSig);
        }
    }

    /** Trailing headers after the last chunk ({@code x-amz-checksum-crc32:...}), lower-cased names. Complete once the stream returned -1. */
    java.util.Map<String, String> trailers() {
        return trailers;
    }

    private void readTrailers() throws IOException {
        while (true) {
            String line;
            try {
                line = readLine();
            } catch (EOFException e) {
                return;
            }
            if (line.isEmpty()) {
                return;
            }
            int colon = line.indexOf(':');
            if (colon > 0) {
                trailers.put(line.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT), line.substring(colon + 1).trim());
            }
        }
    }

    private String readLine() throws IOException {
        StringBuilder sb = new StringBuilder();
        int c;
        while ((c = in.read()) != '\n') {
            if (c < 0) {
                throw new EOFException("aws-chunked body truncated");
            }
            if (c != '\r') {
                sb.append((char) c);
            }
            if (sb.length() > 4096) {
                throw new IOException("aws-chunked header line too long");
            }
        }
        return new String(sb.toString().getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.ISO_8859_1);
    }
}

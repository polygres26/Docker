package com.sayonora.wire.s3wire;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Decodes the {@code aws-chunked} body framing SigV4 streaming uploads use
 * ({@code hex-size;chunk-signature=...\r\n data \r\n ... 0;chunk-signature=...\r\n [trailers]\r\n}).
 * Per-chunk signatures and trailing checksums are parsed past but NOT verified (only the request's
 * seed signature is, see {@link S3SigV4Verifier}).
 */
final class AwsChunkedInputStream extends InputStream {
    private final InputStream in;
    private long remaining;
    private boolean pendingCrlf;
    private boolean done;

    AwsChunkedInputStream(InputStream in) {
        this.in = in;
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
                in.readAllBytes(); // optional trailers (e.g. x-amz-checksum-*) + final CRLF
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

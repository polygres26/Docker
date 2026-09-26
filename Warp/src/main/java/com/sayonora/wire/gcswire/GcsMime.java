package com.sayonora.wire.gcswire;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Streaming reader of a {@code multipart/related} or {@code multipart/mixed} body: parts are read one after the other, a
 * part's content is an {@link InputStream} that ends at the boundary, so an uploaded object never has to fit in memory.
 */
final class GcsMime {

    record Part(Map<String, String> headers, InputStream body) {
    }

    private static final Pattern BOUNDARY = Pattern.compile("boundary=(?:\"([^\"]+)\"|([^;\\s]+))", Pattern.CASE_INSENSITIVE);

    static String boundaryOf(String contentType) {
        if (contentType == null) {
            return null;
        }
        Matcher m = BOUNDARY.matcher(contentType);
        return m.find() ? (m.group(1) != null ? m.group(1) : m.group(2)) : null;
    }

    private final InputStream in;
    private final byte[] delim;
    private final byte[] buf = new byte[64 * 1024 + 256];
    private int pos;
    private int lim;
    private boolean eof;
    private boolean started;
    private boolean finished;
    private PartStream current;

    GcsMime(InputStream in, String boundary) {
        this.in = in;
        this.delim = ("\r\n--" + boundary).getBytes(StandardCharsets.ISO_8859_1);
    }

    /** @return the next part or null after the closing boundary */
    Part next() throws IOException {
        if (finished) {
            return null;
        }
        if (current != null) {
            current.drain();
        }
        if (!started) {
            started = true;
            // the body starts directly with "--boundary": pretend the CRLF before it was there
            byte[] pre = "\r\n".getBytes(StandardCharsets.ISO_8859_1);
            System.arraycopy(pre, 0, buf, 0, 2);
            pos = 0;
            lim = 2;
            // skip any preamble up to the first delimiter
            PartStream skip = new PartStream();
            skip.drain();
            if (finished) {
                return null;
            }
        }
        // after a delimiter: "--" closes, otherwise CRLF then headers
        int a = readByte();
        int b = readByte();
        if (a == '-' && b == '-') {
            finished = true;
            return null;
        }
        // consume the rest of the delimiter line (transport padding) up to CRLF
        while (!(a == '\r' && b == '\n')) {
            a = b;
            b = readByte();
            if (b < 0) {
                finished = true;
                return null;
            }
        }
        Map<String, String> headers = new LinkedHashMap<>();
        while (true) {
            String line = readLine();
            if (line == null || line.isEmpty()) {
                break;
            }
            int c = line.indexOf(':');
            if (c > 0) {
                headers.put(line.substring(0, c).trim().toLowerCase(Locale.ROOT), line.substring(c + 1).trim());
            }
        }
        current = new PartStream();
        return new Part(headers, current);
    }

    private int readByte() throws IOException {
        if (pos >= lim && !fill()) {
            return -1;
        }
        return buf[pos++] & 0xff;
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        int c;
        while ((c = readByte()) >= 0) {
            if (c == '\r') {
                int d = readByte();
                if (d == '\n') {
                    return o.toString(StandardCharsets.ISO_8859_1);
                }
                o.write(c);
                if (d < 0) {
                    break;
                }
                o.write(d);
            } else {
                o.write(c);
            }
        }
        return o.size() == 0 ? null : o.toString(StandardCharsets.ISO_8859_1);
    }

    private boolean fill() throws IOException {
        if (eof) {
            return false;
        }
        if (pos > 0 && pos < lim) {
            System.arraycopy(buf, pos, buf, 0, lim - pos);
        }
        lim -= pos;
        pos = 0;
        int n = in.read(buf, lim, buf.length - lim);
        if (n < 0) {
            eof = true;
            return false;
        }
        lim += n;
        return true;
    }

    private final class PartStream extends InputStream {
        private boolean ended;

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n < 0 ? -1 : one[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (ended) {
                return -1;
            }
            while (true) {
                int idx = indexOfDelim();
                if (idx >= 0) {
                    if (idx == pos) {
                        pos += delim.length;
                        ended = true;
                        return -1;
                    }
                    int n = Math.min(len, idx - pos);
                    System.arraycopy(buf, pos, b, off, n);
                    pos += n;
                    return n;
                }
                int safe = lim - pos - (delim.length - 1);
                if (safe > 0) {
                    int n = Math.min(len, safe);
                    System.arraycopy(buf, pos, b, off, n);
                    pos += n;
                    return n;
                }
                int before = lim - pos;
                if (!fill()) {
                    // no closing delimiter: the rest is the content
                    ended = true;
                    finished = true;
                    int n = Math.min(len, lim - pos);
                    if (n <= 0) {
                        return -1;
                    }
                    System.arraycopy(buf, pos, b, off, n);
                    pos += n;
                    ended = pos >= lim;
                    return n;
                }
                if (lim - pos == before) {
                    return 0;
                }
            }
        }

        private int indexOfDelim() {
            outer:
            for (int i = pos; i + delim.length <= lim; i++) {
                for (int j = 0; j < delim.length; j++) {
                    if (buf[i + j] != delim[j]) {
                        continue outer;
                    }
                }
                return i;
            }
            return -1;
        }

        void drain() throws IOException {
            byte[] tmp = new byte[8192];
            while (read(tmp, 0, tmp.length) >= 0) {
                // discard
            }
        }
    }
}

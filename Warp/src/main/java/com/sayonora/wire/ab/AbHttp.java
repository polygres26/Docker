package com.sayonora.wire.ab;

import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/** Servlet helpers for the routing handler: a replayable request body and a response that captures while (optionally) passing through. */
final class AbHttp {

    private AbHttp() {
    }

    /** A request whose body was read for routing, replayable to the handler that gets it. */
    static final class CachedBody extends HttpServletRequestWrapper {
        private final byte[] body;

        CachedBody(HttpServletRequest r, byte[] body) {
            super(r);
            this.body = body;
        }

        @Override
        public ServletInputStream getInputStream() {
            ByteArrayInputStream in = new ByteArrayInputStream(body);
            return new ServletInputStream() {
                @Override
                public int read() {
                    return in.read();
                }

                @Override
                public int read(byte[] b, int off, int len) {
                    return in.read(b, off, len);
                }

                @Override
                public boolean isFinished() {
                    return in.available() == 0;
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setReadListener(jakarta.servlet.ReadListener l) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(new ByteArrayInputStream(body), StandardCharsets.UTF_8));
        }

        @Override
        public int getContentLength() {
            return body.length;
        }

        @Override
        public long getContentLengthLong() {
            return body.length;
        }
    }

    /**
     * Captures the status, headers and (capped) body of what a handler writes, plus a digest and count of ALL body bytes.
     * With {@code passthrough} true everything is also delivered to the real response; false detaches completely.
     */
    static final class CaptureResponse extends HttpServletResponseWrapper {
        private final boolean passthrough;
        private final long cap;
        private final ByteArrayOutputStream head = new ByteArrayOutputStream();
        private final MessageDigest md;
        private long total;
        private int status = 200;
        private final Map<String, String> headers = new LinkedHashMap<>();
        private String contentType;
        private String encoding;
        private ServletOutputStream out;
        private PrintWriter writer;
        private boolean committed;

        CaptureResponse(HttpServletResponse real, boolean passthrough, long cap) {
            super(real);
            this.passthrough = passthrough;
            this.cap = cap;
            try {
                this.md = MessageDigest.getInstance("SHA-256");
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }

        AbDiff.Capture capture(long nanos) {
            if (writer != null) {
                writer.flush();
            }
            AbDiff.Capture c = new AbDiff.Capture();
            c.status = status;
            headers.forEach((k, v) -> c.headers.put(k.toLowerCase(Locale.ROOT), v));
            if (contentType != null) {
                c.headers.put("content-type", contentType);
            }
            c.head = head.toByteArray();
            c.total = total;
            c.truncated = total > cap;
            c.sha256 = AbSigV4.hex(md.digest());
            c.nanos = nanos;
            return c;
        }

        private void note(byte[] b, int off, int len) {
            md.update(b, off, len);
            total += len;
            long room = cap - head.size();
            if (room > 0) {
                head.write(b, off, (int) Math.min(room, len));
            }
        }

        @Override
        public void setStatus(int sc) {
            status = sc;
            if (passthrough) {
                super.setStatus(sc);
            }
        }

        @Override
        public int getStatus() {
            return status;
        }

        @Override
        public void sendError(int sc, String msg) throws IOException {
            status = sc;
            if (passthrough) {
                super.sendError(sc, msg);
            }
        }

        @Override
        public void sendError(int sc) throws IOException {
            sendError(sc, null);
        }

        @Override
        public void setHeader(String name, String value) {
            headers.put(name, value);
            if (passthrough) {
                super.setHeader(name, value);
            }
        }

        @Override
        public void addHeader(String name, String value) {
            headers.merge(name, value, (a, b) -> a + "," + b);
            if (passthrough) {
                super.addHeader(name, value);
            }
        }

        @Override
        public void setIntHeader(String name, int value) {
            setHeader(name, Integer.toString(value));
        }

        @Override
        public void setDateHeader(String name, long date) {
            if (passthrough) {
                super.setDateHeader(name, date);
            }
        }

        @Override
        public String getHeader(String name) {
            return headers.get(name);
        }

        @Override
        public boolean containsHeader(String name) {
            return headers.containsKey(name);
        }

        @Override
        public void setContentType(String type) {
            contentType = type;
            if (passthrough) {
                super.setContentType(type);
            }
        }

        @Override
        public String getContentType() {
            return contentType;
        }

        @Override
        public void setCharacterEncoding(String charset) {
            encoding = charset;
            if (passthrough) {
                super.setCharacterEncoding(charset);
            }
        }

        @Override
        public String getCharacterEncoding() {
            return encoding != null ? encoding : passthrough ? super.getCharacterEncoding() : "ISO-8859-1";
        }

        @Override
        public void setContentLength(int len) {
            if (passthrough) {
                super.setContentLength(len);
            }
        }

        @Override
        public void setContentLengthLong(long len) {
            if (passthrough) {
                super.setContentLengthLong(len);
            }
        }

        @Override
        public void setBufferSize(int size) {
            if (passthrough) {
                super.setBufferSize(size);
            }
        }

        @Override
        public boolean isCommitted() {
            return passthrough ? super.isCommitted() : committed;
        }

        @Override
        public void flushBuffer() throws IOException {
            committed = true;
            if (passthrough) {
                super.flushBuffer();
            }
        }

        @Override
        public void reset() {
            headers.clear();
            head.reset();
            md.reset();
            total = 0;
            if (passthrough) {
                super.reset();
            }
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            if (out == null) {
                ServletOutputStream real = passthrough ? super.getOutputStream() : null;
                out = new ServletOutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        note(new byte[] {(byte) b}, 0, 1);
                        if (real != null) {
                            real.write(b);
                        }
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        note(b, off, len);
                        if (real != null) {
                            real.write(b, off, len);
                        }
                    }

                    @Override
                    public void flush() throws IOException {
                        committed = true;
                        if (real != null) {
                            real.flush();
                        }
                    }

                    @Override
                    public void close() throws IOException {
                        if (real != null) {
                            real.close();
                        }
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setWriteListener(WriteListener l) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            return out;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            if (writer == null) {
                Charset cs;
                try {
                    cs = Charset.forName(getCharacterEncoding());
                } catch (RuntimeException e) {
                    cs = StandardCharsets.ISO_8859_1;
                }
                writer = new PrintWriter(new java.io.OutputStreamWriter((OutputStream) getOutputStream(), cs), false);
            }
            return writer;
        }
    }
}

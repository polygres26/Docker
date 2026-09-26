package com.sayonora.wire.gcswire;

import com.google.gson.JsonElement;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The response of one request: status + headers, then a body written lazily. Backed by a servlet response (streaming) or by a
 * buffer (a sub-request of a batch).
 */
final class GcsResp {

    private final HttpServletResponse servlet;
    int status = 200;
    final Map<String, String> headers = new LinkedHashMap<>();
    private final ByteArrayOutputStream buffer;
    private boolean committed;
    /** true for HEAD: headers only */
    boolean headOnly;

    GcsResp(HttpServletResponse servlet) {
        this.servlet = servlet;
        this.buffer = null;
    }

    GcsResp() {
        this.servlet = null;
        this.buffer = new ByteArrayOutputStream();
    }

    GcsResp status(int s) {
        status = s;
        return this;
    }

    GcsResp header(String k, String v) {
        if (v != null) {
            headers.put(k, v);
        }
        return this;
    }

    boolean committed() {
        return committed || servlet != null && servlet.isCommitted();
    }

    byte[] buffered() {
        return buffer.toByteArray();
    }

    /** Commits status + headers (once) and returns the body stream. */
    OutputStream body() throws IOException {
        if (servlet == null) {
            committed = true;
            return buffer;
        }
        if (!committed) {
            committed = true;
            servlet.setStatus(status);
            headers.forEach(servlet::setHeader);
        }
        return servlet.getOutputStream();
    }

    void bytes(int status, String contentType, byte[] data) throws IOException {
        this.status = status;
        header("Content-Type", contentType);
        header("Content-Length", String.valueOf(data.length));
        OutputStream o = body();
        if (!headOnly && data.length > 0) {
            o.write(data);
        }
    }

    void json(int status, JsonElement e) throws IOException {
        bytes(status, "application/json; charset=UTF-8", e.toString().getBytes(StandardCharsets.UTF_8));
    }

    void empty(int status) throws IOException {
        this.status = status;
        header("Content-Length", "0");
        body();
    }
}

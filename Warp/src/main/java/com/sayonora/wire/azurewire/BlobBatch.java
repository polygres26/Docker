package com.sayonora.wire.azurewire;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Blob batch: a multipart/mixed body of DELETE Blob / Set Blob Tier sub-requests, each run through the normal blob
 * dispatch with the outer request's credentials (sub-request Authorization headers are not re-verified), answered
 * as a multipart/mixed body with one part per sub-request.
 */
final class BlobBatch {

    private BlobBatch() {
    }

    static void run(BlobService svc, AzReq outer, HttpServletResponse resp, AzureAuth.Result auth) throws IOException {
        auth.authorize('s', "dx");
        String ct = outer.header("Content-Type");
        Matcher bm = ct == null ? null : Pattern.compile("boundary=\"?([^\";]+)\"?", Pattern.CASE_INSENSITIVE).matcher(ct);
        if (bm == null || !bm.find() || !ct.toLowerCase().startsWith("multipart/mixed")) {
            throw AzErrors.invalidHeader("Content-Type", ct);
        }
        String boundary = bm.group(1);
        byte[] raw = outer.raw.getInputStream().readAllBytes();
        String body = new String(raw, StandardCharsets.ISO_8859_1);
        String[] parts = body.split("--" + Pattern.quote(boundary));
        String rboundary = "batchresponse_" + UUID.randomUUID();
        StringBuilder out = new StringBuilder();
        int n = 0;
        String kind = null;
        for (String part : parts) {
            String p = part.strip();
            if (p.isEmpty() || p.equals("--") || p.startsWith("--")) {
                continue;
            }
            n++;
            if (n > 256) {
                throw new AzureException(400, "InvalidInput", "The batch request has too many sub-requests (maximum 256).");
            }
            int hdrEnd = p.indexOf("\r\n\r\n");
            String partHeaders = hdrEnd < 0 ? "" : p.substring(0, hdrEnd);
            String http = hdrEnd < 0 ? p : p.substring(hdrEnd + 4);
            String contentId = "0";
            for (String line : partHeaders.split("\r\n")) {
                if (line.regionMatches(true, 0, "Content-ID:", 0, 11)) {
                    contentId = line.substring(11).trim();
                }
            }
            String[] lines = http.split("\r\n", -1);
            String[] rl = lines[0].split(" ");
            String method = rl[0];
            String target = rl.length > 1 ? rl[1] : "/";
            Map<String, String> hdrs = new LinkedHashMap<>();
            int i = 1;
            for (; i < lines.length && !lines[i].isEmpty(); i++) {
                int c = lines[i].indexOf(':');
                if (c > 0) {
                    hdrs.put(lines[i].substring(0, c).trim(), lines[i].substring(c + 1).trim());
                }
            }
            hdrs.putIfAbsent("x-ms-version", outer.header("x-ms-version") == null ? "2021-08-06" : outer.header("x-ms-version"));
            String uriPath = target;
            String query = null;
            if (target.startsWith("http://") || target.startsWith("https://")) {
                java.net.URI u = java.net.URI.create(target);
                uriPath = u.getRawPath();
                query = u.getRawQuery();
            } else if (target.contains("?")) {
                query = target.substring(target.indexOf('?') + 1);
                uriPath = target.substring(0, target.indexOf('?'));
            }
            String full;
            if (outer.hostStyle) {
                full = uriPath;
            } else {
                String prefix = "/" + outer.account + "/";
                full = uriPath.startsWith(prefix) || uriPath.equals("/" + outer.account) ? uriPath : "/" + outer.account + uriPath;
            }
            HttpServletRequest sub = SubRequest.create(outer.raw, method, full, query, hdrs, new byte[0], null, null);
            AzReq sr = new AzReq(sub, AzReq.BLOB, svc.cfg, UUID.randomUUID().toString());
            Capture cap = new Capture(resp);
            try {
                String op = method + ":" + sr.q("comp");
                if (kind == null) {
                    kind = op;
                }
                if (!kind.equals(op)) {
                    throw new AzureException(400, "InvalidInput", "All the sub-requests of a batch must be of the same kind.");
                }
                if (!("DELETE".equals(method) && sr.q("comp") == null) && !("PUT".equals(method) && "tier".equals(sr.q("comp")))) {
                    throw new AzureException(400, "InvalidInput", "Only Delete Blob and Set Blob Tier are allowed in a batch.");
                }
                String container = sr.first();
                String blob = sr.rest();
                if (container == null || blob == null) {
                    throw AzErrors.invalidUri();
                }
                svc.dispatch(sr, cap, auth, container, blob);
            } catch (AzureException e) {
                cap.reset();
                cap.setStatus(e.status);
                cap.setHeader("x-ms-error-code", e.code);
                String xml = AzHttp.xmlError(e, sr);
                cap.setContentType("application/xml");
                cap.body.writeBytes(xml.getBytes(StandardCharsets.UTF_8));
                cap.reasonOverride = e.getMessage();
            }
            out.append("--").append(rboundary).append("\r\nContent-Type: application/http\r\nContent-ID: ").append(contentId)
                    .append("\r\n\r\n");
            out.append("HTTP/1.1 ").append(cap.status).append(' ').append(cap.reason()).append("\r\n");
            out.append("x-ms-request-id: ").append(sr.requestId).append("\r\n");
            out.append("x-ms-version: ").append(hdrs.get("x-ms-version")).append("\r\n");
            for (Map.Entry<String, String> h : cap.headers.entrySet()) {
                out.append(h.getKey()).append(": ").append(h.getValue()).append("\r\n");
            }
            if (cap.body.size() > 0) {
                out.append("Content-Length: ").append(cap.body.size()).append("\r\n");
            }
            out.append("\r\n");
            if (cap.body.size() > 0) {
                out.append(cap.body.toString(StandardCharsets.UTF_8)).append("\r\n");
            }
        }
        out.append("--").append(rboundary).append("--\r\n");
        byte[] b = out.toString().getBytes(StandardCharsets.UTF_8);
        resp.setStatus(202);
        resp.setContentType("multipart/mixed; boundary=" + rboundary);
        resp.setContentLength(b.length);
        resp.getOutputStream().write(b);
    }

    /** Response wrapper that records status, headers and body instead of writing them. */
    static final class Capture extends HttpServletResponseWrapper {
        int status = 200;
        final Map<String, String> headers = new LinkedHashMap<>();
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        String reasonOverride;

        Capture(HttpServletResponse r) {
            super(r);
        }

        void reset0() {
            headers.clear();
            body.reset();
        }

        @Override
        public void reset() {
            reset0();
        }

        String reason() {
            if (reasonOverride != null) {
                return reasonOverride.replace("\r", " ").replace("\n", " ");
            }
            return switch (status) {
                case 200 -> "OK";
                case 202 -> "Accepted";
                case 204 -> "No Content";
                default -> "Status";
            };
        }

        @Override
        public void setStatus(int sc) {
            status = sc;
        }

        @Override
        public void setHeader(String name, String value) {
            if (value == null) {
                headers.remove(name);
            } else {
                headers.put(name, value);
            }
        }

        @Override
        public void addHeader(String name, String value) {
            headers.put(name, value);
        }

        @Override
        public void setContentType(String type) {
            headers.put("Content-Type", type);
        }

        @Override
        public void setContentLength(int len) {
        }

        @Override
        public ServletOutputStream getOutputStream() {
            return new ServletOutputStream() {
                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(WriteListener l) {
                }

                @Override
                public void write(int b) {
                    body.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    body.write(b, off, len);
                }
            };
        }
    }

    static List<String> unused() {
        return List.of();
    }
}

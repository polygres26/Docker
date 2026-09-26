package com.sayonora.wire.azurewire;

import com.sayonora.wire.core.BackendRegistry;
import com.sayonora.wire.core.StoreType;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * In-process access to one Azure Storage service (Blob, Queue or Table) of azurewire for the MCP tools: a REST request is
 * handed to the very same {@code BlobService}/{@code QueueService}/{@code TableService} the network listener uses
 * (validation, ETags, leases, OData, error codes), over a private request/response pair. Authentication is a private
 * per-process bearer token (the MCP endpoint's own token and governance apply instead of the storage account key).
 */
public final class AzureEmbedded {

    /** One answer: HTTP status, headers (case-insensitive names) and body. */
    public record Response(int status, Map<String, String> headers, byte[] body) {
        public String text() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    @FunctionalInterface
    private interface Handler {
        void handle(AzReq r, HttpServletResponse resp) throws IOException;
    }

    private final String service;
    private final AzureConfig cfg;
    private final Handler handler;
    private final String token = UUID.randomUUID().toString();

    private AzureEmbedded(String service, AzureConfig cfg, Handler handler) {
        this.service = service;
        this.cfg = cfg;
        this.handler = handler;
    }

    private static AzureConfig privateConfig(String token) {
        // no account keys are needed: the bearer path authenticates any account name
        return new AzureConfig(Map.of(), null, token);
    }

    public static AzureEmbedded blob(BackendRegistry reg) {
        String token = UUID.randomUUID().toString();
        AzureConfig cfg = privateConfig(token);
        BlobService svc = new BlobService(new BlobStore(new AzShards(reg, StoreType.AZBLOB)), cfg, new AzureAuth(cfg));
        return new AzureEmbedded(AzReq.BLOB, cfg, svc::handle).withToken(token);
    }

    public static AzureEmbedded queue(BackendRegistry reg) {
        String token = UUID.randomUUID().toString();
        AzureConfig cfg = privateConfig(token);
        QueueService svc = new QueueService(new QueueStore(new AzShards(reg, StoreType.AZQUEUE)), cfg, new AzureAuth(cfg));
        return new AzureEmbedded(AzReq.QUEUE, cfg, svc::handle).withToken(token);
    }

    public static AzureEmbedded table(BackendRegistry reg) {
        String token = UUID.randomUUID().toString();
        AzureConfig cfg = privateConfig(token);
        TableService svc = new TableService(new TableStore(new AzShards(reg, StoreType.AZTABLE)), cfg, new AzureAuth(cfg));
        return new AzureEmbedded(AzReq.TABLE, cfg, svc::handle).withToken(token);
    }

    private String bearer;

    private AzureEmbedded withToken(String t) {
        this.bearer = t;
        return this;
    }

    /** The storage accounts clients of the wire listeners sign with (WARP_AZURE_ACCOUNTS / dev account). */
    public static java.util.Set<String> configuredAccounts() {
        try {
            AzureConfig c = AzureConfig.fromEnv();
            java.util.Set<String> out = new java.util.LinkedHashSet<>();
            String spec = System.getenv("WARP_AZURE_ACCOUNTS");
            if (spec != null) {
                for (String part : spec.split(";")) {
                    int colon = part.indexOf(':');
                    if (colon > 0 && c.account(part.substring(0, colon).trim()) != null) {
                        out.add(part.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT));
                    }
                }
            }
            if (c.account(AzureConfig.DEV_ACCOUNT) != null) {
                out.add(AzureConfig.DEV_ACCOUNT);
            }
            return out;
        } catch (RuntimeException e) {
            return java.util.Set.of();
        }
    }

    /**
     * @param path resource below the account, starting with '/' or empty for the account root (already percent-encoded
     *             where needed, e.g. {@code /container/blob%20name})
     */
    public Response request(String account, String method, String path, String rawQuery, Map<String, String> headers, byte[] body) {
        Map<String, String> h = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        h.put("Host", "localhost");
        h.put("Authorization", "Bearer " + bearer);
        h.put("x-ms-version", "2021-08-06");
        h.put("x-ms-date", java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(Instant.now().atZone(java.time.ZoneOffset.UTC)));
        if (headers != null) {
            h.putAll(headers);
        }
        byte[] b = body == null ? new byte[0] : body;
        h.put("Content-Length", String.valueOf(b.length));
        HttpServletRequest req = request(method, "/" + account + (path == null ? "" : path), rawQuery, h, b);
        Resp out = new Resp();
        HttpServletResponse resp = out.proxy();
        AzReq r = null;
        try {
            r = new AzReq(req, service, cfg, UUID.randomUUID().toString());
            handler.handle(r, resp);
        } catch (AzureException e) {
            out.reset();
            e.headers.forEach(out.headers::put);
            out.headers.put("x-ms-error-code", e.code);
            out.status = e.status;
            if (!"HEAD".equals(method) && r != null) {
                boolean table = AzReq.TABLE.equals(service);
                out.body.writeBytes((table ? AzHttp.jsonError(e, r, "application/json;odata=nometadata") : AzHttp.xmlError(e, r))
                        .getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return new Response(out.status, out.headers, out.body.toByteArray());
    }

    // ------------------------------------------------------------------ private servlet request/response

    private static Object defaultFor(Class<?> t) {
        if (t == boolean.class) {
            return false;
        }
        if (t == int.class) {
            return 0;
        }
        if (t == long.class) {
            return 0L;
        }
        return null;
    }

    private static HttpServletRequest request(String method, String uri, String query, Map<String, String> headers, byte[] body) {
        InvocationHandler ih = (proxy, m, args) -> switch (m.getName()) {
            case "getMethod" -> method;
            case "getRequestURI" -> uri;
            case "getQueryString" -> query == null || query.isEmpty() ? null : query;
            case "getHeader" -> headers.get((String) args[0]);
            case "getHeaders" -> {
                String v = headers.get((String) args[0]);
                yield v == null ? Collections.emptyEnumeration() : Collections.enumeration(java.util.List.of(v));
            }
            case "getHeaderNames" -> Collections.enumeration(headers.keySet());
            case "getContentLengthLong" -> (long) body.length;
            case "getContentLength" -> body.length;
            case "getScheme" -> "http";
            case "getRemoteAddr" -> "127.0.0.1";
            case "getInputStream" -> new ServletInputStream() {
                private final ByteArrayInputStream in = new ByteArrayInputStream(body);

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
                public void setReadListener(ReadListener l) {
                }
            };
            case "toString" -> "AzureEmbedded request " + method + " " + uri;
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> defaultFor(m.getReturnType());
        };
        return (HttpServletRequest) Proxy.newProxyInstance(HttpServletRequest.class.getClassLoader(),
                new Class<?>[] {HttpServletRequest.class}, ih);
    }

    private static final class Resp {
        int status = 200;
        Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        boolean committed;

        void reset() {
            status = 200;
            headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            body.reset();
            committed = false;
        }

        HttpServletResponse proxy() {
            ServletOutputStream sos = new ServletOutputStream() {
                @Override
                public void write(int b) {
                    committed = true;
                    body.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) {
                    committed = true;
                    body.write(b, off, len);
                }

                @Override
                public boolean isReady() {
                    return true;
                }

                @Override
                public void setWriteListener(WriteListener l) {
                }
            };
            InvocationHandler ih = (proxy, m, args) -> {
                switch (m.getName()) {
                    case "setStatus" -> status = (Integer) args[0];
                    case "setHeader", "addHeader" -> headers.put((String) args[0], String.valueOf(args[1]));
                    case "setContentType" -> headers.put("Content-Type", (String) args[0]);
                    case "setContentLength", "setContentLengthLong" -> headers.put("Content-Length", String.valueOf(args[0]));
                    case "getOutputStream" -> {
                        return sos;
                    }
                    case "isCommitted" -> {
                        return committed;
                    }
                    case "reset", "resetBuffer" -> reset();
                    case "getStatus" -> {
                        return status;
                    }
                    case "getHeader" -> {
                        return headers.get((String) args[0]);
                    }
                    case "containsHeader" -> {
                        return headers.containsKey((String) args[0]);
                    }
                    case "flushBuffer" -> {
                    }
                    case "hashCode" -> {
                        return System.identityHashCode(proxy);
                    }
                    case "equals" -> {
                        return proxy == args[0];
                    }
                    default -> {
                        return defaultFor(m.getReturnType());
                    }
                }
                return defaultFor(m.getReturnType());
            };
            return (HttpServletResponse) Proxy.newProxyInstance(HttpServletResponse.class.getClassLoader(),
                    new Class<?>[] {HttpServletResponse.class}, ih);
        }
    }
}

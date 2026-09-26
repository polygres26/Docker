package com.sayonora.wire.awswire;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs an {@link H2Exchange} through {@link AwsHttp}: the exchange is presented to it as a servlet request and response (a
 * {@link Proxy} implementing just the methods the AWS handlers use), the buffered response is then written back as HEADERS plus
 * DATA. SubscribeToShard, which streams, is handled here separately.
 */
final class H2Dispatch {

    private H2Dispatch() {
    }

    static HttpServletRequest request(H2Exchange ex) {
        return (HttpServletRequest) Proxy.newProxyInstance(H2Dispatch.class.getClassLoader(), new Class<?>[] {HttpServletRequest.class},
                (proxy, m, args) -> {
                    switch (m.getName()) {
                        case "getHeader": {
                            return ex.header((String) args[0]);
                        }
                        case "getHeaders": {
                            List<String> v = ex.headers.get(((String) args[0]).toLowerCase(java.util.Locale.ROOT));
                            return Collections.enumeration(v == null ? List.<String>of() : v);
                        }
                        case "getHeaderNames":
                            return Collections.enumeration(ex.headers.keySet());
                        case "getMethod":
                            return ex.method;
                        case "getRequestURI":
                            return ex.path;
                        case "getQueryString":
                            return ex.query;
                        case "getContentType":
                            return ex.header("content-type");
                        case "getContentLength":
                            return ex.body.length;
                        case "getContentLengthLong":
                            return (long) ex.body.length;
                        case "getScheme":
                            return "http";
                        case "getServerName":
                        case "getLocalName":
                            return ex.localName;
                        case "getLocalPort":
                        case "getServerPort":
                            return ex.localPort;
                        case "getRemoteAddr":
                        case "getRemoteHost":
                            return ex.remoteAddr;
                        case "getProtocol":
                            return "HTTP/2.0";
                        case "getInputStream": {
                            ByteArrayInputStream in = new ByteArrayInputStream(ex.body);
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
                        case "toString":
                            return "H2Request " + ex.method + " " + ex.path;
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> r = m.getReturnType();
                            if (r == boolean.class) {
                                return false;
                            }
                            if (r == int.class) {
                                return 0;
                            }
                            if (r == long.class) {
                                return 0L;
                            }
                            return null;
                    }
                });
    }

    /** The buffered servlet response; {@link #commit} writes it out as HEADERS + DATA(end). */
    static final class Response {
        int status = 200;
        final Map<String, String> headers = new LinkedHashMap<>();
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        PrintWriter writer;
        String contentType;
        final HttpServletResponse proxy;

        Response() {
            this.proxy = (HttpServletResponse) Proxy.newProxyInstance(H2Dispatch.class.getClassLoader(),
                    new Class<?>[] {HttpServletResponse.class}, (p, m, args) -> {
                        switch (m.getName()) {
                            case "setStatus":
                                status = (Integer) args[0];
                                return null;
                            case "getStatus":
                                return status;
                            case "setHeader":
                            case "addHeader":
                                headers.put(((String) args[0]).toLowerCase(java.util.Locale.ROOT), String.valueOf(args[1]));
                                return null;
                            case "setContentType":
                                contentType = (String) args[0];
                                return null;
                            case "getContentType":
                                return contentType;
                            case "setCharacterEncoding":
                            case "setContentLength":
                            case "setContentLengthLong":
                            case "flushBuffer":
                                return null;
                            case "getWriter":
                                if (writer == null) {
                                    writer = new PrintWriter(new OutputStreamWriter(body, StandardCharsets.UTF_8));
                                }
                                return writer;
                            case "getOutputStream":
                                return new ServletOutputStream() {
                                    @Override
                                    public void write(int b) {
                                        body.write(b);
                                    }

                                    @Override
                                    public void write(byte[] b, int off, int len) {
                                        body.write(b, off, len);
                                    }

                                    @Override
                                    public boolean isReady() {
                                        return true;
                                    }

                                    @Override
                                    public void setWriteListener(jakarta.servlet.WriteListener l) {
                                        throw new UnsupportedOperationException();
                                    }
                                };
                            case "containsHeader":
                                return headers.containsKey(((String) args[0]).toLowerCase(java.util.Locale.ROOT));
                            case "toString":
                                return "H2Response";
                            case "hashCode":
                                return System.identityHashCode(p);
                            case "equals":
                                return p == args[0];
                            default:
                                Class<?> r = m.getReturnType();
                                return r == boolean.class ? (Object) false : r == int.class ? (Object) 0 : null;
                        }
                    });
        }

        void commit(H2Exchange ex) {
            if (writer != null) {
                writer.flush();
            }
            if (contentType != null) {
                headers.put("content-type", contentType);
            }
            headers.put("server", "warp-awswire");
            ex.head(status, headers);
            ex.data(body.toByteArray(), true);
        }
    }

    /** The whole HTTP/2 path of one exchange for a single service. */
    static void serve(AwsRuntime rt, AwsHttp http, AwsService service, H2Exchange ex, String metricsProtocol) throws IOException {
        String target = ex.header("x-amz-target");
        if (service instanceof KinesisService kin && target != null && target.endsWith(".SubscribeToShard")) {
            JsonObject req;
            try {
                String ct = ex.header("content-type");
                req = (ct != null && ct.contains("cbor")) ? Cbor.decode(ex.body).getAsJsonObject()
                        : ex.body.length == 0 ? new JsonObject() : JsonParser.parseString(new String(ex.body, StandardCharsets.UTF_8)).getAsJsonObject();
            } catch (RuntimeException e) {
                req = new JsonObject();
            }
            kin.subscribeToShard(req, ex);
            return;
        }
        Response resp = new Response();
        http.serve(service, request(ex), resp.proxy, metricsProtocol);
        resp.commit(ex);
    }
}

package com.sayonora.wire.firestorewire;

import io.grpc.Attributes;
import io.grpc.netty.shaded.io.grpc.netty.GrpcHttp2ConnectionHandler;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.ProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.channel.ChannelFutureListener;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.channel.SimpleChannelInboundHandler;
import io.grpc.netty.shaded.io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpHeaderNames;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpUtil;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpVersion;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * One TCP port for gRPC (HTTP/2 prior knowledge, what every Google client library and emulator-host setting uses) and REST/JSON
 * (HTTP/1.1), like the official Firestore and Datastore emulators do. The first bytes of a connection decide: the HTTP/2 preface
 * {@code "PRI * HTTP/2.0"} goes to gRPC untouched; anything else becomes an HTTP/1.1 connection served by a {@link Rest} handler.
 * The gRPC server keeps its normal negotiator, so nothing about gRPC handling changes.
 */
public final class GrpcRestMux implements InternalProtocolNegotiator.ProtocolNegotiator {

    /** A parsed HTTP request. */
    public record Req(String method, String path, Map<String, List<String>> query, Map<String, String> headers, byte[] body) {
        public String header(String name) {
            return headers.get(name.toLowerCase(java.util.Locale.ROOT));
        }

        public String param(String name) {
            List<String> v = query.get(name);
            return v == null || v.isEmpty() ? null : v.get(0);
        }
    }

    /** An HTTP response. */
    public record Resp(int status, String contentType, byte[] body, Map<String, String> headers) {
        public static Resp json(int status, String json) {
            return new Resp(status, "application/json; charset=UTF-8", json.getBytes(StandardCharsets.UTF_8), Map.of());
        }
    }

    /** REST side: called on a worker thread (may block on the database). */
    public interface Rest {
        Resp handle(Req req);
    }

    private static final ExecutorService POOL = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "wire-rest");
        t.setDaemon(true);
        return t;
    });

    private final InternalProtocolNegotiator.ProtocolNegotiator delegate;
    private final Rest rest;

    public GrpcRestMux(InternalProtocolNegotiator.ProtocolNegotiator delegate, Rest rest) {
        this.delegate = delegate;
        this.rest = rest;
    }

    @Override
    public AsciiString scheme() {
        return delegate.scheme();
    }

    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return new Sniffer(delegate.newHandler(grpcHandler), rest);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static final class Sniffer extends ChannelInboundHandlerAdapter {
        private final ChannelHandler h2;
        private final Rest rest;
        private ByteBuf acc;
        private boolean done;

        Sniffer(ChannelHandler h2, Rest rest) {
            this.h2 = h2;
            this.rest = rest;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (done || !(msg instanceof ByteBuf buf)) {
                ctx.fireChannelRead(msg);
                return;
            }
            acc = acc == null ? buf : Unpooled.wrappedBuffer(acc, buf);
            int n = Math.min(acc.readableBytes(), 4);
            byte[] head = new byte[n];
            acc.getBytes(acc.readerIndex(), head);
            byte[] pri = "PRI ".getBytes(StandardCharsets.US_ASCII);
            boolean prefixOfPri = true;
            for (int i = 0; i < n; i++) {
                if (head[i] != pri[i]) {
                    prefixOfPri = false;
                    break;
                }
            }
            if (prefixOfPri && n < 4) {
                return; // undecided
            }
            done = true;
            ByteBuf leftover = acc;
            acc = null;
            if (prefixOfPri) {
                ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.withAttributes(InternalProtocolNegotiationEvent.getDefault(),
                        Attributes.EMPTY);
                ctx.pipeline().replace(this, ctx.name() + "-h2", h2);
                ctx.fireUserEventTriggered(pne);
                ctx.fireChannelRead(leftover);
            } else {
                ctx.pipeline().replace(this, "http-codec", new HttpServerCodec());
                ctx.pipeline().addAfter("http-codec", "http-agg", new HttpObjectAggregator(64 << 20));
                ctx.pipeline().addAfter("http-agg", "http-rest", new RestHandler(rest));
                ctx.pipeline().fireChannelRead(leftover);
            }
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            if (acc != null) {
                acc.release();
                acc = null;
            }
        }
    }

    private static final class RestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final Rest rest;

        RestHandler(Rest rest) {
            this.rest = rest;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            byte[] body = new byte[req.content().readableBytes()];
            req.content().readBytes(body);
            String uri = req.uri();
            String rawPath = uri;
            String rawQuery = "";
            int q = uri.indexOf('?');
            if (q >= 0) {
                rawPath = uri.substring(0, q);
                rawQuery = uri.substring(q + 1);
            }
            Map<String, List<String>> query = new LinkedHashMap<>();
            if (!rawQuery.isEmpty()) {
                for (String kv : rawQuery.split("&")) {
                    if (kv.isEmpty()) {
                        continue;
                    }
                    int e = kv.indexOf('=');
                    String k = decode(e < 0 ? kv : kv.substring(0, e));
                    String v = e < 0 ? "" : decode(kv.substring(e + 1));
                    query.computeIfAbsent(k, x -> new java.util.ArrayList<>()).add(v);
                }
            }
            Map<String, String> headers = new LinkedHashMap<>();
            req.headers().forEach(h -> headers.put(h.getKey().toLowerCase(java.util.Locale.ROOT), h.getValue()));
            boolean keep = HttpUtil.isKeepAlive(req);
            if (req.method().name().equals("OPTIONS")) {
                // CORS preflight (browser SDKs): allow everything, like the official emulators
                DefaultFullHttpResponse pre = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
                pre.headers().set("Access-Control-Allow-Origin", "*");
                pre.headers().set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
                pre.headers().set("Access-Control-Allow-Headers", headers.getOrDefault("access-control-request-headers", "*"));
                pre.headers().set("Access-Control-Max-Age", "3600");
                pre.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                var pf = ctx.writeAndFlush(pre);
                if (!keep) {
                    pf.addListener(ChannelFutureListener.CLOSE);
                }
                return;
            }
            Req r = new Req(req.method().name(), decodePath(rawPath), query, headers, body);
            POOL.execute(() -> {
                Resp resp;
                try {
                    resp = rest.handle(r);
                } catch (RuntimeException e) {
                    resp = Resp.json(500, "{\"error\":{\"code\":500,\"message\":\"Internal error: " + String.valueOf(e.getMessage()).replace("\"", "'")
                            + "\",\"status\":\"INTERNAL\"}}");
                }
                DefaultFullHttpResponse out = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(resp.status()),
                        Unpooled.wrappedBuffer(resp.body()));
                out.headers().set(HttpHeaderNames.CONTENT_TYPE, resp.contentType());
                out.headers().set(HttpHeaderNames.CONTENT_LENGTH, resp.body().length);
                out.headers().set("Access-Control-Allow-Origin", "*");
                resp.headers().forEach((k, v) -> out.headers().set(k, v));
                if (!keep) {
                    out.headers().set(HttpHeaderNames.CONNECTION, "close");
                }
                var f = ctx.writeAndFlush(out);
                if (!keep) {
                    f.addListener(ChannelFutureListener.CLOSE);
                }
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }

        private static String decode(String s) {
            try {
                return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                return s;
            }
        }

        private static String decodePath(String p) {
            try {
                return java.net.URLDecoder.decode(p.replace("+", "%2B"), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                return p;
            }
        }
    }
}

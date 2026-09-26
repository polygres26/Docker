package com.sayonora.wire.awswire;

import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.channel.embedded.EmbeddedChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.DefaultHttp2WindowUpdateFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2DataFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Error;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2FrameStream;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Headers;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2HeadersFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2ResetFrame;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Settings;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.eclipse.jetty.io.AbstractConnection;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.io.EndPoint;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.thread.Invocable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Jetty connection that speaks HTTP/2 by driving Netty's {@code Http2FrameCodec} inside an {@link EmbeddedChannel}: bytes read
 * from Jetty's endpoint are fed to the codec, the frames it emits (HEADERS/DATA per stream) are assembled into
 * {@link H2Exchange}s, which run on a worker executor -- never on the selector thread, since a handler talks to Postgres -- and
 * everything the codec writes is drained to the endpoint. The embedded channel is not thread-safe, so every touch of it holds its
 * monitor. Flow control: the codec manages the remote (send) window; for received DATA this class returns the consumed bytes with a
 * WINDOW_UPDATE. HTTP/2 push and priority are not used; a stream reset by the peer marks its exchange cancelled.
 */
final class H2cConnection extends AbstractConnection implements Connection.UpgradeTo {

    private static final Logger log = LoggerFactory.getLogger(H2cConnection.class);
    private static final int MAX_BODY = 64 * 1024 * 1024;
    private static final boolean DEBUG = "true".equalsIgnoreCase(System.getenv("WARP_AWSWIRE_DEBUG_H2"));

    private final EmbeddedChannel ch;
    private final ByteBufferPool pool;
    private final Consumer<H2Exchange> handler;
    private final Executor workers;
    private final Map<Integer, Stream> streams = new HashMap<>();
    private final ArrayDeque<ByteBuffer> writeQueue = new ArrayDeque<>();
    private boolean writing;
    private volatile boolean closed;

    private final class Stream extends H2Exchange {
        final Http2FrameStream frameStream;
        final ByteArrayOutputStream in = new ByteArrayOutputStream();
        volatile boolean reset;
        boolean headSent;

        Stream(Http2FrameStream s) {
            this.frameStream = s;
        }

        @Override
        public void head(int status, Map<String, String> responseHeaders) {
            Http2Headers h = new DefaultHttp2Headers().status(String.valueOf(status));
            responseHeaders.forEach((k, v) -> h.add(k.toLowerCase(java.util.Locale.ROOT), v));
            synchronized (ch) {
                if (closed || reset) {
                    return;
                }
                headSent = true;
                ch.write(new DefaultHttp2HeadersFrame(h, false).stream(frameStream));
                ch.flush();
                drain();
            }
        }

        @Override
        public void data(byte[] bytes, boolean end) {
            synchronized (ch) {
                if (closed || reset) {
                    return;
                }
                ch.write(new DefaultHttp2DataFrame(Unpooled.wrappedBuffer(bytes), end).stream(frameStream));
                ch.flush();
                if (end) {
                    streams.remove(frameStream.id());
                }
                drain();
            }
        }

        @Override
        public boolean cancelled() {
            return reset || closed;
        }
    }

    H2cConnection(EndPoint endPoint, Executor executor, ByteBufferPool pool, Consumer<H2Exchange> handler, Executor workers) {
        super(endPoint, executor);
        this.pool = pool;
        this.handler = handler;
        this.workers = workers;
        Http2Settings settings = Http2Settings.defaultSettings().maxConcurrentStreams(256);
        this.ch = new EmbeddedChannel(Http2FrameCodecBuilder.forServer().initialSettings(settings).autoAckSettingsFrame(true)
                .autoAckPingFrame(true).build(), new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                onFrame(ctx, msg);
            }
        });
    }

    // -------------------------------------------------------------------------------------------- inbound

    @Override
    public void onOpen() {
        super.onOpen();
        synchronized (ch) {
            ch.flush();
            drain();
        }
        fillInterested();
    }

    @Override
    public void onUpgradeTo(ByteBuffer prefilled) {
        feed(prefilled);
    }

    @Override
    public void onFillable() {
        ByteBuffer buf = pool.acquire(16 * 1024, false);
        try {
            while (true) {
                // EndPoint.fill appends after the buffer's limit (flush mode): empty it with position = limit = 0, do not clear()
                org.eclipse.jetty.util.BufferUtil.clear(buf);
                int n = getEndPoint().fill(buf);
                if (DEBUG) {
                    log.warn("h2c fill n={}", n);
                }
                if (n > 0) {
                    feed(buf);
                } else if (n == 0) {
                    fillInterested();
                    return;
                } else {
                    shutdown();
                    getEndPoint().close();
                    return;
                }
            }
        } catch (Throwable t) {
            log.debug("h2c read failed: {}", t.toString());
            shutdown();
            getEndPoint().close(t);
        } finally {
            pool.release(buf);
        }
    }

    private void feed(ByteBuffer b) {
        if (DEBUG) {
            log.warn("h2c feed {} bytes", b.remaining());
        }
        byte[] copy = new byte[b.remaining()];
        b.get(copy);
        synchronized (ch) {
            if (closed) {
                return;
            }
            try {
                ch.writeInbound(Unpooled.wrappedBuffer(copy));
                ch.flush(); // pushes the WINDOW_UPDATEs and SETTINGS/PING acks queued while reading, and any DATA a new window now allows
                ch.checkException();
            } catch (Throwable t) {
                log.warn("h2c protocol error: {}", t.toString());
                shutdown();
                getEndPoint().close(t);
                return;
            }
            drain();
        }
    }

    private void onFrame(ChannelHandlerContext ctx, Object msg) {
        if (DEBUG) {
            log.warn("h2c frame {}", msg);
        }
        try {
            if (msg instanceof Http2HeadersFrame hf) {
                Stream s = new Stream(hf.stream());
                Http2Headers h = hf.headers();
                s.method = String.valueOf(h.method());
                String path = String.valueOf(h.path());
                int q = path.indexOf('?');
                s.path = q < 0 ? path : path.substring(0, q);
                s.query = q < 0 ? null : path.substring(q + 1);
                h.forEach(e -> {
                    String name = e.getKey().toString();
                    if (!name.startsWith(":")) {
                        s.headers.computeIfAbsent(name, k -> new ArrayList<>()).add(e.getValue().toString());
                    }
                });
                if (h.authority() != null) {
                    s.headers.computeIfAbsent("host", k -> new ArrayList<>()).add(h.authority().toString());
                }
                InetSocketAddress remote = getEndPoint().getRemoteSocketAddress() instanceof InetSocketAddress a ? a : null;
                s.remoteAddr = remote == null ? "127.0.0.1" : remote.getAddress().getHostAddress();
                InetSocketAddress local = getEndPoint().getLocalSocketAddress() instanceof InetSocketAddress a ? a : null;
                s.localPort = local == null ? 0 : local.getPort();
                streams.put(hf.stream().id(), s);
                if (hf.isEndStream()) {
                    dispatch(s);
                }
            } else if (msg instanceof Http2DataFrame df) {
                Stream s = streams.get(df.stream().id());
                int bytes = df.initialFlowControlledBytes();
                if (s != null) {
                    ByteBuf c = df.content();
                    byte[] arr = new byte[c.readableBytes()];
                    c.readBytes(arr);
                    if (s.in.size() + arr.length > MAX_BODY) {
                        ctx.write(new DefaultHttp2ResetFrame(Http2Error.REFUSED_STREAM).stream(df.stream()));
                        streams.remove(df.stream().id());
                    } else {
                        s.in.write(arr, 0, arr.length);
                        if (df.isEndStream()) {
                            dispatch(s);
                        }
                    }
                }
                if (bytes > 0) {
                    ctx.write(new DefaultHttp2WindowUpdateFrame(bytes).stream(df.stream()));
                    ctx.write(new DefaultHttp2WindowUpdateFrame(bytes));
                    ctx.flush();
                }
                df.release();
            } else if (msg instanceof Http2ResetFrame rf) {
                Stream s = streams.remove(rf.stream().id());
                if (s != null) {
                    s.reset = true;
                }
            }
        } finally {
            if (!(msg instanceof Http2DataFrame)) {
                io.grpc.netty.shaded.io.netty.util.ReferenceCountUtil.release(msg);
            }
        }
    }

    private void dispatch(Stream s) {
        s.body = s.in.toByteArray();
        workers.execute(() -> {
            try {
                handler.accept(s);
            } catch (Throwable t) {
                log.warn("h2c handler failed: {}", t.toString());
                if (!s.headSent) {
                    s.head(500, Map.of("content-type", "text/plain"));
                }
                s.data("internal error".getBytes(java.nio.charset.StandardCharsets.UTF_8), true);
            }
        });
    }

    // -------------------------------------------------------------------------------------------- outbound

    /** Moves everything the codec wrote into the endpoint's write queue. Caller holds the channel monitor. */
    private void drain() {
        ByteBuf out;
        while ((out = ch.readOutbound()) != null) {
            byte[] b = new byte[out.readableBytes()];
            out.readBytes(b);
            out.release();
            if (b.length > 0) {
                writeQueue.add(ByteBuffer.wrap(b));
            }
        }
        if (!writing && !writeQueue.isEmpty()) {
            writeNext();
        }
    }

    private void writeNext() {
        ByteBuffer[] batch = writeQueue.toArray(new ByteBuffer[0]);
        writeQueue.clear();
        writing = true;
        getEndPoint().write(new Callback() {
            @Override
            public void succeeded() {
                synchronized (ch) {
                    writing = false;
                    if (!writeQueue.isEmpty()) {
                        writeNext();
                    }
                }
            }

            @Override
            public void failed(Throwable x) {
                shutdown();
                getEndPoint().close(x);
            }

            @Override
            public Invocable.InvocationType getInvocationType() {
                return Invocable.InvocationType.NON_BLOCKING;
            }
        }, batch);
    }

    private void shutdown() {
        synchronized (ch) {
            closed = true;
            streams.values().forEach(s -> s.reset = true);
            streams.clear();
            try {
                ch.finishAndReleaseAll();
            } catch (Throwable ignored) {
                // already closing
            }
        }
    }

    @Override
    public void onClose(Throwable cause) {
        shutdown();
        super.onClose(cause);
    }
}

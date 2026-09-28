package com.sayonora.warp.tls;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.netty.shaded.io.grpc.netty.GrpcHttp2ConnectionHandler;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiators;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandler;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC-over-TLS on top of the shared, hot-reloading {@link TlsProvider}. Unlike {@code NettyServerBuilder.sslContext(..)}
 * (a fixed snapshot) the {@link TlsProvider#nettyContext(boolean)} is asked for on every new connection, so a renewed
 * certificate is picked up without a restart. The negotiator runs the TLS handshake first, then hands the decrypted
 * stream to either plain gRPC (HTTP/2) or, for the Firestore/Datastore ports, to a next handler that also serves
 * REST over HTTP/1.1 (ALPN {@code h2} / {@code http/1.1}).
 */
public final class NettyTls {

    private static final Logger log = LoggerFactory.getLogger(NettyTls.class);

    private NettyTls() {
    }

    /** gRPC only (ALPN h2): a drop-in for {@code NettyServerBuilder.protocolNegotiator(..)}. */
    public static InternalProtocolNegotiator.ProtocolNegotiator grpc(TlsProvider provider) {
        InternalProtocolNegotiator.ProtocolNegotiator plain = InternalProtocolNegotiators.serverPlaintext();
        return new Negotiator(plain, provider, false, (h, next) -> next, true);
    }

    /**
     * TLS in front of a multiplexer that decides gRPC vs HTTP/1.1 itself (see {@code GrpcRestMux}): {@code wrap} builds the
     * post-handshake handler from the plaintext negotiator's handler.
     */
    public static InternalProtocolNegotiator.ProtocolNegotiator mux(TlsProvider provider,
            InternalProtocolNegotiator.ProtocolNegotiator plain, Function<ChannelHandler, ChannelHandler> wrap) {
        return new Negotiator(plain, provider, true, (h, next) -> wrap.apply(next), false);
    }

    private interface Next {
        ChannelHandler build(GrpcHttp2ConnectionHandler grpc, ChannelHandler plainHandler);
    }

    private record Negotiator(InternalProtocolNegotiator.ProtocolNegotiator plain, TlsProvider provider, boolean mux,
            Next next, boolean firePne) implements InternalProtocolNegotiator.ProtocolNegotiator {

        @Override
        public AsciiString scheme() {
            return AsciiString.of("https");
        }

        @Override
        public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
            return new TlsFirst(provider, mux, () -> next.build(grpcHandler, plain.newHandler(grpcHandler)), firePne);
        }

        @Override
        public void close() {
            plain.close();
        }
    }

    private static final class TlsFirst extends ChannelInboundHandlerAdapter {
        private final TlsProvider provider;
        private final boolean mux;
        private final java.util.function.Supplier<ChannelHandler> next;
        private final boolean firePne;

        TlsFirst(TlsProvider provider, boolean mux, java.util.function.Supplier<ChannelHandler> next, boolean firePne) {
            this.provider = provider;
            this.mux = mux;
            this.next = next;
            this.firePne = firePne;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            SslHandler ssl = provider.nettyContext(mux).newHandler(ctx.alloc());
            ctx.pipeline().addBefore(ctx.name(), "warp-tls", ssl);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof SslHandshakeCompletionEvent done) {
                if (!done.isSuccess()) {
                    log.debug("TLS handshake failed from {}: {}", ctx.channel().remoteAddress(), String.valueOf(done.cause()));
                    ctx.close();
                    return;
                }
                SslHandler ssl = ctx.pipeline().get(SslHandler.class);
                ChannelHandler n = next.get();
                ctx.pipeline().replace(this, ctx.name() + "-app", n);
                if (firePne) {
                    Attributes attrs = Attributes.newBuilder()
                            .set(Grpc.TRANSPORT_ATTR_SSL_SESSION, ssl.engine().getSession()).build();
                    ctx.fireUserEventTriggered(InternalProtocolNegotiationEvent.withAttributes(
                            InternalProtocolNegotiationEvent.getDefault(), attrs));
                }
                return;
            }
            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.debug("TLS negotiation error from {}: {}", ctx.channel().remoteAddress(), cause.toString());
            ctx.close();
        }
    }
}

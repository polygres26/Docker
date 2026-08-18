package com.polygres.wire.grpc;

import com.polygres.wire.acl.Cidr;
import com.polygres.wire.acl.ClientAcl;
import com.polygres.wire.acl.ProxyProtocolV2;
import io.grpc.Attributes;
import io.grpc.netty.shaded.io.grpc.netty.GrpcHttp2ConnectionHandler;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.grpc.netty.InternalProtocolNegotiator;
import io.grpc.netty.shaded.io.grpc.netty.ProtocolNegotiationEvent;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufInputStream;
import io.grpc.netty.shaded.io.netty.buffer.CompositeByteBuf;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandler;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInboundHandlerAdapter;
import io.grpc.netty.shaded.io.netty.util.AsciiString;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps a real grpc-netty-shaded {@link InternalProtocolNegotiator.ProtocolNegotiator} (plaintext
 * or TLS) to read and strip a PPv2 preamble off the raw connection before the wrapped negotiator's
 * own chain ever sees a byte -- the gRPC analogue of what {@code Main#acceptOraWireTlsLoop}'s
 * accept-then-wrap restructure does for orawire's TCPS listener, and what
 * {@code ConnectionGate#acceptTcp} does for every plain-{@code ServerSocket} frontend.
 *
 * <p><b>Why this exists as a custom {@code ProtocolNegotiator} rather than the simpler
 * {@link AclInterceptor} approach used for IP-only ACL</b>: {@link AclInterceptor} rejects
 * per-call, after gRPC's own HTTP/2 handshake has already completed -- fine for a raw IP check
 * (the real peer address is already known at that point), but PPv2 bytes must be consumed
 * *before* HTTP/2 framing starts, which needs a hook earlier than any interceptor can reach. This
 * class is that hook: {@code NettyServerBuilder#protocolNegotiator} (a genuinely public method,
 * confirmed via direct decompilation, not assumed) lets a caller substitute the entire
 * negotiation chain.
 *
 * <p><b>Why the resolved address rides a custom {@link Attributes.Key}, not
 * {@code Grpc.TRANSPORT_ATTR_REMOTE_ADDR}</b> -- see {@link GrpcProxyProtocol}'s javadoc for the
 * decompiled proof that grpc-netty-shaded's own {@code WaitUntilActiveHandler} unconditionally
 * overwrites that specific key from the raw {@code Channel.remoteAddress()} later in every
 * negotiator chain, discarding anything set on it earlier.
 *
 * <p><b>Only wired to gRPC's plaintext listener</b> -- see {@code PolyWireGrpcServer#startTls}'s
 * javadoc for why wrapping {@code InternalProtocolNegotiators.serverTls(...)} the same way this
 * class wraps {@code serverPlaintext()} was tried and found, live, to silently break the TLS
 * handshake (a real, reported, unresolved gap, not a theoretical one).
 */
final class PpV2ProtocolNegotiator implements InternalProtocolNegotiator.ProtocolNegotiator {

    private final InternalProtocolNegotiator.ProtocolNegotiator delegate;
    private final ClientAcl acl;
    private final List<Cidr> trustedProxies;

    PpV2ProtocolNegotiator(InternalProtocolNegotiator.ProtocolNegotiator delegate, ClientAcl acl, List<Cidr> trustedProxies) {
        this.delegate = delegate;
        this.acl = acl;
        this.trustedProxies = trustedProxies;
    }

    @Override
    public AsciiString scheme() {
        return delegate.scheme();
    }

    @Override
    public ChannelHandler newHandler(GrpcHttp2ConnectionHandler grpcHandler) {
        return new PpV2Handler(delegate.newHandler(grpcHandler), acl, trustedProxies);
    }

    @Override
    public void close() {
        delegate.close();
    }

    /**
     * Buffers bytes until a full PPv2 header is available, evaluates the ACL against the
     * PPv2-derived source address (closing the channel outright on reject or a malformed/missing
     * header -- same "loud, fatal, this listener requires PPv2" policy as
     * {@link ProxyProtocolV2#readHeader}), then replaces itself with the wrapped negotiator's real
     * handler and forwards both the enriched {@link ProtocolNegotiationEvent} (carrying the
     * resolved address under {@link GrpcProxyProtocol#PROXIED_REMOTE_ADDRESS}) and any leftover
     * bytes already buffered past the header (the real HTTP/2 preface, if it arrived in the same
     * TCP segment as the PPv2 header).
     */
    private static final class PpV2Handler extends ChannelInboundHandlerAdapter {

        private static final Logger log = LoggerFactory.getLogger(PpV2Handler.class);
        private static final int FIXED_PREFIX_LEN = 16; // 12-byte signature + verCmd + famTrans + 2-byte length

        private final ChannelHandler delegateHandler;
        private final ClientAcl acl;
        private final List<Cidr> trustedProxies;
        private CompositeByteBuf cumulation;
        private boolean done;

        PpV2Handler(ChannelHandler delegateHandler, ClientAcl acl, List<Cidr> trustedProxies) {
            this.delegateHandler = delegateHandler;
            this.acl = acl;
            this.trustedProxies = trustedProxies;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            cumulation = ctx.alloc().compositeBuffer();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (done) {
                ctx.fireChannelRead(msg);
                return;
            }
            if (!(msg instanceof io.grpc.netty.shaded.io.netty.buffer.ByteBuf buf)) {
                ctx.fireChannelRead(msg); // not expected pre-negotiation, but don't silently drop it
                return;
            }
            cumulation.addComponent(true, buf);

            // Signature checked the moment enough bytes exist for it (SIGNATURE_LENGTH == 12),
            // strictly before the length field at offset 14-15 is ever read -- found live: reading
            // the length field first and trusting it blindly, before confirming this is really a
            // PPv2 header at all, meant a real (non-PPv2) client's first 16 bytes (e.g. a genuine
            // HTTP/2 preface's "PRI * HTTP/2.0\r\n") got misread as a bogus multi-kilobyte length,
            // and the handler then waited forever for bytes that would never arrive -- the
            // connection just hung until the client's own deadline, instead of failing fast the
            // same way a genuinely-malformed PPv2 header already correctly does.
            if (cumulation.readableBytes() < ProxyProtocolV2.SIGNATURE_LENGTH) {
                return; // wait for more bytes
            }
            byte[] sigCandidate = new byte[ProxyProtocolV2.SIGNATURE_LENGTH];
            cumulation.getBytes(0, sigCandidate);
            if (!ProxyProtocolV2.signatureMatches(sigCandidate)) {
                InetAddress rawPeerForLog = channelRemoteAddress(ctx);
                log.warn("ACL: rejecting gRPC connection from {} -- PROXY protocol v2 signature missing/invalid "
                        + "-- this listener requires PPv2 (POLYWIRE_ACL_PPV2_ENABLED=true)", rawPeerForLog);
                failAndClose(ctx);
                return;
            }
            if (cumulation.readableBytes() < FIXED_PREFIX_LEN) {
                return; // wait for more bytes
            }
            int length = cumulation.getUnsignedShort(14);
            int totalHeaderLen = FIXED_PREFIX_LEN + length;
            if (cumulation.readableBytes() < totalHeaderLen) {
                return; // wait for more bytes
            }

            InetAddress rawPeer = channelRemoteAddress(ctx);
            if (!trustedProxies.isEmpty() && (rawPeer == null || !matchesAny(rawPeer, trustedProxies))) {
                log.warn("ACL: rejecting gRPC connection from {} -- PPv2 is enabled on this listener but this "
                        + "peer is not in POLYWIRE_ACL_TRUSTED_PROXIES", rawPeer);
                failAndClose(ctx);
                return;
            }

            ProxyProtocolV2.Result header;
            io.grpc.netty.shaded.io.netty.buffer.ByteBuf headerSlice = cumulation.readSlice(totalHeaderLen).retain();
            try (ByteBufInputStream in = new ByteBufInputStream(headerSlice, true)) {
                header = ProxyProtocolV2.readHeader(in);
            } catch (java.io.IOException e) {
                log.warn("ACL: rejecting gRPC connection from {} -- {}", rawPeer, e.getMessage());
                failAndClose(ctx);
                return;
            }
            InetAddress effectiveClient = header.sourceAddress().orElse(rawPeer);
            if (effectiveClient == null || !acl.isAllowed(effectiveClient)) {
                log.warn("ACL: rejecting gRPC connection from {}", effectiveClient);
                failAndClose(ctx);
                return;
            }

            done = true;
            Attributes attrs = Attributes.newBuilder()
                    .set(GrpcProxyProtocol.PROXIED_REMOTE_ADDRESS, effectiveClient)
                    .build();
            ProtocolNegotiationEvent pne = InternalProtocolNegotiationEvent.withAttributes(
                    InternalProtocolNegotiationEvent.getDefault(), attrs);

            // Capture and null the field BEFORE calling pipeline().replace() -- Netty invokes this
            // handler's own handlerRemoved() synchronously as part of replace(), and handlerRemoved
            // releases+nulls the `cumulation` field defensively; without this reordering, that runs
            // out from under the code below and the second access throws (found live: a real
            // NullPointerException surfaced exactly this way during verification).
            io.grpc.netty.shaded.io.netty.buffer.ByteBuf leftover = cumulation;
            cumulation = null;

            ctx.pipeline().replace(ctx.name(), ctx.name() + "-ppv2-delegate", delegateHandler);
            ctx.fireUserEventTriggered(pne);
            if (leftover.isReadable()) {
                ctx.fireChannelRead(leftover); // leftover bytes past the header -- the real HTTP/2 preface
            } else {
                leftover.release();
            }
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
        }

        private void failAndClose(ChannelHandlerContext ctx) {
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
            done = true;
            ctx.close();
        }

        private static InetAddress channelRemoteAddress(ChannelHandlerContext ctx) {
            SocketAddress addr = ctx.channel().remoteAddress();
            return addr instanceof InetSocketAddress isa ? isa.getAddress() : null;
        }

        private static boolean matchesAny(InetAddress address, List<Cidr> cidrs) {
            for (Cidr cidr : cidrs) {
                if (cidr.contains(address)) {
                    return true;
                }
            }
            return false;
        }
    }
}

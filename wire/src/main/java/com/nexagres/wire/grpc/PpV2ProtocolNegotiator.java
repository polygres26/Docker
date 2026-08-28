package com.nexagres.wire.grpc;

import com.nexagres.wire.acl.Cidr;
import com.nexagres.wire.acl.ClientAcl;
import com.nexagres.wire.acl.ProxyProtocolV2;
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

    private static final class PpV2Handler extends ChannelInboundHandlerAdapter {

        private static final Logger log = LoggerFactory.getLogger(PpV2Handler.class);
        private static final int FIXED_PREFIX_LEN = 16;

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
                ctx.fireChannelRead(msg);
                return;
            }
            cumulation.addComponent(true, buf);

            if (cumulation.readableBytes() < ProxyProtocolV2.SIGNATURE_LENGTH) {
                return;
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
                return;
            }
            int length = cumulation.getUnsignedShort(14);
            int totalHeaderLen = FIXED_PREFIX_LEN + length;
            if (cumulation.readableBytes() < totalHeaderLen) {
                return;
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

            io.grpc.netty.shaded.io.netty.buffer.ByteBuf leftover = cumulation;
            cumulation = null;

            ctx.pipeline().replace(ctx.name(), ctx.name() + "-ppv2-delegate", delegateHandler);
            ctx.fireUserEventTriggered(pne);
            if (leftover.isReadable()) {
                ctx.fireChannelRead(leftover);
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

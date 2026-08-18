package com.polygres.wire.grpc;

import io.grpc.Attributes;

/**
 * The custom {@link Attributes.Key} a PPv2-derived client address is carried under, once parsed
 * by {@link PpV2ProtocolNegotiator} -- deliberately a key of this project's own, not
 * {@code io.grpc.Grpc.TRANSPORT_ATTR_REMOTE_ADDR}. Found live while implementing this: grpc-netty-
 * shaded's own {@code ProtocolNegotiators$WaitUntilActiveHandler.replaceOnActive} (decompiled
 * directly, not assumed) unconditionally rebuilds {@code TRANSPORT_ATTR_REMOTE_ADDR} from the raw
 * Netty {@code Channel.remoteAddress()} as part of every negotiator chain (plaintext and TLS
 * alike) -- so a value attached to that specific key earlier in negotiation is silently discarded
 * once the delegate negotiator's own chain runs. {@code Attributes.Builder#set} only overwrites
 * the exact key(s) it's called with, though -- every other key already present (including this
 * one) survives untouched -- so a key of this project's own composes cleanly instead of fighting
 * that invariant. {@link AclInterceptor} checks this key first, falling back to
 * {@code TRANSPORT_ATTR_REMOTE_ADDR} (the real raw peer -- correct when PPv2 isn't in play) when
 * absent.
 */
final class GrpcProxyProtocol {

    static final Attributes.Key<java.net.InetAddress> PROXIED_REMOTE_ADDRESS =
            Attributes.Key.create("com.polygres.wire.grpc.proxied-remote-address");

    private GrpcProxyProtocol() {
    }
}

package com.polygres.wire.acl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Optional;

/**
 * Parses the binary PROXY protocol v2 preamble (Willy Tarreau/HAProxy's spec --
 * <a href="https://www.haproxy.org/download/1.8/doc/proxy-protocol.txt">proxy-protocol.txt</a>) a
 * real client's true source address, injected by an L4 load balancer/proxy sitting in front of
 * PolyWire (HAProxy, nginx, Envoy, AWS NLB) that terminated the actual client TCP connection
 * itself. Read once, synchronously, as the very first bytes on a freshly-accepted socket, before
 * any wire protocol's own handshake starts reading -- exactly where every other real PPv2
 * receiver (pgbouncer, Redis, etcd) reads it.
 *
 * <p>Deliberately v2 (binary) only, not v1 (human-readable text) -- v2 is what every real load
 * balancer named above emits by default and what this project's own {@link ClientAcl} needs
 * (v1 carries the same information, just textually; supporting both would double the parsing
 * surface for a case this project's own deployment targets don't need).
 *
 * <p><b>Not autodetected per-connection</b> -- a listener either always expects PPv2 (every real
 * receiver's own convention, since a load balancer either always injects it for a given listener
 * or never does) or never does, driven by {@code POLYWIRE_ACL_PPV2_ENABLED}. A missing/malformed
 * signature on a listener that expects it is treated as a protocol violation, not silently
 * ignored -- see {@link #readHeader}'s javadoc.
 */
public final class ProxyProtocolV2 {

    private static final byte[] SIGNATURE = {
            0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A
    };

    private static final int FAMILY_INET = 0x1;
    private static final int FAMILY_INET6 = 0x2;
    private static final int COMMAND_MASK = 0x0F;
    private static final int COMMAND_LOCAL = 0x0;

    private ProxyProtocolV2() {
    }

    public record Result(Optional<InetAddress> sourceAddress) {
    }

    /**
     * Reads and consumes a full PPv2 header from {@code in} (the very first bytes of a freshly
     * accepted connection). Returns an empty {@link Result#sourceAddress()} for a {@code LOCAL}
     * command (health-check probes from the proxy itself carry no real client -- callers should
     * fall back to the raw socket's own peer address for these, same as PPv2-disabled behavior)
     * or an unsupported/unspecified address family; a real {@code PROXY} command over INET/INET6
     * always yields a present address.
     *
     * @throws IOException if the signature doesn't match at all (this listener expects PPv2 on
     *     every connection -- see class javadoc -- so a missing/wrong preamble means either a
     *     misconfigured upstream or a client trying to connect directly, bypassing the load
     *     balancer this listener assumes fronts it; both are treated as fatal for this connection,
     *     never silently passed through) or the header is truncated/malformed.
     */
    public static Result readHeader(InputStream rawIn) throws IOException {
        DataInputStream in = rawIn instanceof DataInputStream d ? d : new DataInputStream(rawIn);
        byte[] sig = new byte[SIGNATURE.length];
        in.readFully(sig);
        if (!java.util.Arrays.equals(sig, SIGNATURE)) {
            throw new IOException("PROXY protocol v2 signature missing/invalid -- this listener requires PPv2 "
                    + "(POLYWIRE_ACL_PPV2_ENABLED=true); either the upstream isn't sending it, or a client is "
                    + "connecting directly, bypassing the expected load balancer");
        }
        int verCmd = in.readUnsignedByte();
        int version = (verCmd >> 4) & 0x0F;
        int command = verCmd & COMMAND_MASK;
        if (version != 2) {
            throw new IOException("PROXY protocol version " + version + " not supported (only v2)");
        }
        int famTrans = in.readUnsignedByte();
        int family = (famTrans >> 4) & 0x0F;
        int length = in.readUnsignedShort();

        if (command == COMMAND_LOCAL) {
            in.skipBytes(length); // health-check/local connection -- no real client address to extract
            return new Result(Optional.empty());
        }

        InetAddress source;
        int consumed;
        if (family == FAMILY_INET) {
            byte[] src = new byte[4];
            in.readFully(src);
            in.skipBytes(4); // dst_addr
            in.skipBytes(4); // src_port + dst_port
            consumed = 12;
            source = InetAddress.getByAddress(src);
        } else if (family == FAMILY_INET6) {
            byte[] src = new byte[16];
            in.readFully(src);
            in.skipBytes(16); // dst_addr
            in.skipBytes(4); // src_port + dst_port
            consumed = 36;
            source = InetAddress.getByAddress(src);
        } else {
            // UNSPEC or UNIX -- no routable client IP to extract (a Unix-domain-socket-fronting
            // proxy has no IP-level client address at all); skip the whole address block and fall
            // back to the raw socket's own peer, same as the LOCAL-command case above.
            in.skipBytes(length);
            return new Result(Optional.empty());
        }
        int remaining = length - consumed;
        if (remaining > 0) {
            in.skipBytes(remaining); // TLVs (AWS NLB's VPC/ENI metadata, etc.) -- not consumed by this pass
        }
        return new Result(Optional.of(source));
    }
}

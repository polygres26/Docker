package com.nexagres.wire.acl;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Optional;

public final class ProxyProtocolV2 {

    private static final byte[] SIGNATURE = {
            0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A
    };

    public static final int SIGNATURE_LENGTH = SIGNATURE.length;

    public static boolean signatureMatches(byte[] candidate) {
        return java.util.Arrays.equals(candidate, SIGNATURE);
    }

    private static final int FAMILY_INET = 0x1;
    private static final int FAMILY_INET6 = 0x2;
    private static final int COMMAND_MASK = 0x0F;
    private static final int COMMAND_LOCAL = 0x0;

    private ProxyProtocolV2() {
    }

    public record Result(Optional<InetAddress> sourceAddress) {
    }

    public static Result readHeader(InputStream rawIn) throws IOException {
        DataInputStream in = rawIn instanceof DataInputStream d ? d : new DataInputStream(rawIn);
        byte[] sig = new byte[SIGNATURE.length];
        in.readFully(sig);
        if (!java.util.Arrays.equals(sig, SIGNATURE)) {
            throw new IOException("PROXY protocol v2 signature missing/invalid -- this listener requires PPv2 "
                    + "(WARP_ACL_PPV2_ENABLED=true); either the upstream isn't sending it, or a client is "
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
            in.skipBytes(length);
            return new Result(Optional.empty());
        }

        InetAddress source;
        int consumed;
        if (family == FAMILY_INET) {
            byte[] src = new byte[4];
            in.readFully(src);
            in.skipBytes(4);
            in.skipBytes(4);
            consumed = 12;
            source = InetAddress.getByAddress(src);
        } else if (family == FAMILY_INET6) {
            byte[] src = new byte[16];
            in.readFully(src);
            in.skipBytes(16);
            in.skipBytes(4);
            consumed = 36;
            source = InetAddress.getByAddress(src);
        } else {
            
            in.skipBytes(length);
            return new Result(Optional.empty());
        }
        int remaining = length - consumed;
        if (remaining > 0) {
            in.skipBytes(remaining);
        }
        return new Result(Optional.of(source));
    }
}

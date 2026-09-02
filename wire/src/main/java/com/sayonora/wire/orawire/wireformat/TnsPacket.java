package com.sayonora.wire.orawire.wireformat;

public final class TnsPacket {

    private static final int HEADER_LENGTH = 8;

    private final TnsPacketType type;
    private final int flags;
    private final byte[] payload;

    public TnsPacket(TnsPacketType type, int flags, byte[] payload) {
        this.type = type;
        this.flags = flags;
        this.payload = payload;
    }

    public TnsPacketType type() {
        return type;
    }

    public int flags() {
        return flags;
    }

    public byte[] payload() {
        return payload;
    }

    public byte[] encode(boolean largeSdu) {
        
        return encode(largeSdu, true);
    }

    public byte[] encode(boolean largeSdu, boolean endOfResponse) {
        
        int preambleLength = HEADER_LENGTH + (type == TnsPacketType.DATA ? 2 : 0);
        byte[] out = new byte[preambleLength + payload.length];
        int length = out.length;
        if (largeSdu) {
            out[0] = (byte) ((length >> 24) & 0xFF);
            out[1] = (byte) ((length >> 16) & 0xFF);
            out[2] = (byte) ((length >> 8) & 0xFF);
            out[3] = (byte) (length & 0xFF);
        } else {
            out[0] = (byte) ((length >> 8) & 0xFF);
            out[1] = (byte) (length & 0xFF);
            out[2] = 0;
            out[3] = 0;
        }
        out[4] = (byte) type.code();
        out[5] = (byte) flags;
        out[6] = 0;
        out[7] = 0;
        if (type == TnsPacketType.DATA) {
            
            int dataFlags = endOfResponse ? 0x2000 : 0x0000;
            out[8] = (byte) ((dataFlags >> 8) & 0xFF);
            out[9] = (byte) (dataFlags & 0xFF);
        }
        System.arraycopy(payload, 0, out, preambleLength, payload.length);
        return out;
    }

    public static int headerLength() {
        return HEADER_LENGTH;
    }
}

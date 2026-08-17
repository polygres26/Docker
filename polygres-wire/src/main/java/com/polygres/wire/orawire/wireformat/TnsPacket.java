package com.polygres.wire.orawire.wireformat;

/**
 * A single TNS packet: 8-byte header followed by a payload. Confirmed
 * against a live packet capture of python-oracledb's actual CONNECT packet:
 * offset 0-3 length (2 or 4 bytes BE depending on framing mode, see below),
 * offset 4 packet type, offset 5 packet flags, remaining header bytes
 * reserved/checksum (0). An earlier version of this class put type/flags at
 * offsets 2-3, which broke on the very first real-client packet.
 *
 * Framing mode: the CONNECT/ACCEPT exchange always uses a 2-byte length
 * field (bytes 0-1) followed by 2 reserved bytes (bytes 2-3) — confirmed by
 * capture. Once the client receives our ACCEPT response, python-oracledb
 * unconditionally switches to a 4-byte length field (bytes 0-3, no separate
 * reserved bytes) for every subsequent packet in both directions
 * (transport.pyx: `_full_packet_size = True`, set unconditionally in
 * ConnectMessage.process's ACCEPT branch, not gated on the advertised
 * protocol_version value). Callers must track and pass this mode
 * explicitly — see {@link com.polygres.wire.orawire.wireformat.TnsPacketReader#setLargeSdu}.
 */
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
        // Every response this server sends is exactly one physical DATA packet (no multi-packet
        // streaming in this narrow slice) — see encode(largeSdu, endOfResponse) for the one
        // exception found live this session (O5LogonHandler's rich-tier phase-two response).
        return encode(largeSdu, true);
    }

    /**
     * @param endOfResponse DATA packets only: whether to set TNS_DATA_FLAGS_END_OF_RESPONSE
     * (0x2000) on this specific physical packet. Real Oracle servers, confirmed live via MITM
     * capture (real sqlplus &lt;-&gt; real Oracle Database 23ai), fragment sufficiently large
     * logical messages into multiple physical DATA packets (observed: a 2618-byte logical
     * message split into a 1967-byte then a 651-byte packet) with this flag clear on every
     * fragment except the last — {@link #encode(boolean)} always passes {@code true} since
     * nothing in this codebase fragments messages, but the one place that was found live to
     * require it (see O5LogonHandler.sendDataFragmented) needs to control this explicitly.
     */
    public byte[] encode(boolean largeSdu, boolean endOfResponse) {
        // DATA packets carry an extra 2-byte data_flags field right after the
        // 8-byte header (spec §1.1); other packet types don't.
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
            out[3] = 0; // reserved
        }
        out[4] = (byte) type.code();
        out[5] = (byte) flags;
        out[6] = 0;
        out[7] = 0; // checksum/reserved
        if (type == TnsPacketType.DATA) {
            // data_flags (big-endian uint16) — TNS_DATA_FLAGS_END_OF_RESPONSE (0x2000). Omitting
            // it entirely (previously always 0x0000) left python-oracledb tolerant (it falls back
            // to treating any successfully-processed message as complete when this capability
            // isn't signaled — messages/base.pyx) but left ojdbc11 waiting indefinitely for
            // more data within the SAME logical call: confirmed live via the driver's own
            // internal diagnostic logging (oracle.jdbc.diagnostic.enableLogging=true) — its
            // very next receive() call read straight from stale/empty buffer state (no new
            // socket read logged at all) instead of blocking to read a new packet, because it
            // still believed the prior call's response might not be finished yet.
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

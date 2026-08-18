package com.polygres.wire.orawire.backend;

/**
 * Strips TNS packet framing from a raw byte capture (see {@link NativeByteCaptureProxy}), leaving
 * just the concatenated TTC message payload — so it can be relayed to PolyWire's own frontend
 * client verbatim (re-wrapped in PolyWire's own TNS packet header via the existing {@code
 * TnsPacket}/{@code TtcWriter} code), instead of reconstructing DESCRIBE_INFO/ROW_DATA/terminator
 * fields from JDBC metadata.
 *
 * <p>Only handles large-SDU (4-byte length) DATA-packet framing — confirmed correct for any
 * capture window that starts after the CONNECT/ACCEPT exchange (i.e. after login), which is the
 * only case this class is ever used for: {@link NativeByteCaptureProxy}'s capture is cleared right
 * before each EXECUTE, well after O5LOGON completes. Per-packet header layout (confirmed live,
 * same investigation that fixed {@code ResponseWriter}'s DESCRIBE_INFO/inline-exhaustion bugs):
 * UB4 length (total packet length, header included) + UB1 type + UB1 flags + UB2 header checksum
 * (unused, always 0 in every capture seen) + UB2 data_flags — 10 bytes total for a DATA packet,
 * not the 8 an earlier pass through this investigation wrongly assumed (that earlier 8-byte
 * assumption only ever "worked" by accident: the header-checksum field is always 0x0000 and
 * happened to look like the start of real payload content in every packet examined by eye).
 */
public final class NativeTtcFrameUtil {

    private static final int TNS_TYPE_DATA = 6;
    private static final int DATA_HEADER_LENGTH = 10;

    private NativeTtcFrameUtil() {
    }

    /** Concatenates the payload of every DATA packet found in {@code raw}, in order, skipping
     *  any non-DATA packets (markers, etc.) entirely. */
    public static byte[] stripFraming(byte[] raw) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream(raw.length);
        int i = 0;
        while (i + 4 <= raw.length) {
            int length = ((raw[i] & 0xFF) << 24) | ((raw[i + 1] & 0xFF) << 16)
                    | ((raw[i + 2] & 0xFF) << 8) | (raw[i + 3] & 0xFF);
            if (length <= 0 || i + length > raw.length) {
                // Incomplete trailing packet in this capture window — stop rather than misparse.
                break;
            }
            int type = raw[i + 4] & 0xFF;
            int headerLen = (type == TNS_TYPE_DATA) ? DATA_HEADER_LENGTH : 8;
            if (type == TNS_TYPE_DATA && length >= headerLen) {
                out.write(raw, i + headerLen, length - headerLen);
            }
            i += length;
        }
        return out.toByteArray();
    }
}

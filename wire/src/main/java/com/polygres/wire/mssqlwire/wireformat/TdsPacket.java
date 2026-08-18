package com.polygres.wire.mssqlwire.wireformat;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * TDS's own packet framing — an 8-byte header (distinct from Oracle's TNS packet header and from
 * Postgres/MySQL's own framing) wrapping every logical message:
 *
 * <pre>
 * byte 0    : Type   (see {@link TdsPacketType})
 * byte 1    : Status (bit 0 = EOM, "last packet of this message")
 * bytes 2-3 : Length, big-endian, header included
 * bytes 4-5 : SPID, big-endian (0 from client; a synthetic per-session id from the server)
 * byte 6    : PacketID (1-based, wraps at 256; informational only, not verified by real clients)
 * byte 7    : Window (unused, always 0)
 * </pre>
 *
 * A logical message (PRELOGIN, LOGIN7, SQL_BATCH, or a tabular-result response) can legally span
 * several physical packets when it exceeds the negotiated packet size; {@link #readMessage}
 * reassembles that automatically by concatenating payloads until it sees {@code STATUS_EOM}. This
 * first pass's {@link #writeMessage} deliberately always emits a single physical packet (with EOM
 * set) regardless of the negotiated size — real clients (including {@code mssql-jdbc}, verified
 * live) read packets purely off each header's own length field and don't require the server to
 * respect the client-declared {@code PacketSize} from LOGIN7, so this is a safe simplification for
 * a narrow first pass; splitting large responses across multiple physical packets is not attempted
 * here.
 */
public final class TdsPacket {

    private static final int HEADER_LEN = 8;

    private int packetIdCounter = 0;

    public record Message(byte type, byte[] payload) {
    }

    public Message readMessage(DataInputStream in) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        byte firstType = 0;
        while (true) {
            byte[] header = new byte[HEADER_LEN];
            in.readFully(header);
            byte type = header[0];
            byte status = header[1];
            int length = ((header[2] & 0xFF) << 8) | (header[3] & 0xFF);
            int bodyLen = length - HEADER_LEN;
            if (bodyLen < 0) {
                throw new IOException("TDS packet length " + length + " smaller than header");
            }
            byte[] body = new byte[bodyLen];
            in.readFully(body);
            if (payload.size() == 0) {
                firstType = type;
            }
            payload.write(body);
            if ((status & TdsPacketType.STATUS_EOM) != 0) {
                break;
            }
        }
        return new Message(firstType, payload.toByteArray());
    }

    public void writeMessage(OutputStream out, byte type, byte[] payload) throws IOException {
        int total = HEADER_LEN + payload.length;
        byte[] header = new byte[HEADER_LEN];
        header[0] = type;
        header[1] = TdsPacketType.STATUS_EOM;
        header[2] = (byte) ((total >>> 8) & 0xFF);
        header[3] = (byte) (total & 0xFF);
        header[4] = 0; // SPID hi
        header[5] = 0; // SPID lo
        header[6] = (byte) (++packetIdCounter & 0xFF);
        header[7] = 0; // window
        out.write(header);
        out.write(payload);
        out.flush();
    }

    public TdsPacket() {
        // one instance per session -- packetIdCounter is per-connection state.
    }
}

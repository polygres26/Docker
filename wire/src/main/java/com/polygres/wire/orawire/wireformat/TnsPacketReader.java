package com.polygres.wire.orawire.wireformat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads TNS packets off the wire. Server-side counterpart to the framing
 * python-oracledb's ReadBuffer performs in impl/thin/packet.pyx. See
 * {@link TnsPacket} for the framing-mode (2-byte vs 4-byte length field)
 * rules — callers must call {@link #setLargeSdu} once our ACCEPT response
 * has been sent.
 */
public final class TnsPacketReader {

    private final DataInputStream in;
    private boolean largeSdu = false;
    private boolean anoEligible = false;

    public TnsPacketReader(InputStream in) {
        this.in = new DataInputStream(in);
    }

    public void setLargeSdu(boolean largeSdu) {
        this.largeSdu = largeSdu;
    }

    public boolean isLargeSdu() {
        return largeSdu;
    }

    /**
     * Set by {@link com.polygres.wire.orawire.frontend.ConnectHandshake} once it knows whether the
     * negotiated ACCEPT advertised ANO availability (rich shape, protocol version &gt;=320) —
     * read by {@link com.polygres.wire.orawire.session.SessionHandler} to decide whether to run
     * {@link com.polygres.wire.orawire.frontend.AnoNegotiation} before {@link
     * com.polygres.wire.orawire.frontend.ProtocolNegotiation}. Same cross-stage-state pattern as
     * {@link #largeSdu} above — no shared session/context object exists in this codebase, so
     * negotiated-but-not-yet-consumed handshake state lives here.
     */
    public void setAnoEligible(boolean anoEligible) {
        this.anoEligible = anoEligible;
    }

    public boolean isAnoEligible() {
        return anoEligible;
    }

    public TnsPacket readPacket() throws IOException {
        byte[] header = new byte[TnsPacket.headerLength()];
        in.readFully(header);
        int length = largeSdu
                ? (((header[0] & 0xFF) << 24) | ((header[1] & 0xFF) << 16)
                        | ((header[2] & 0xFF) << 8) | (header[3] & 0xFF))
                : (((header[0] & 0xFF) << 8) | (header[1] & 0xFF));
        TnsPacketType type = TnsPacketType.fromCode(header[4] & 0xFF);
        int flags = header[5] & 0xFF;
        int preambleLength = TnsPacket.headerLength();
        if (type == TnsPacketType.DATA) {
            byte[] dataFlags = new byte[2];
            in.readFully(dataFlags); // spec §1.1: DATA packets carry 2 extra data_flags bytes
            preambleLength += 2;
        }
        byte[] payload = new byte[length - preambleLength];
        in.readFully(payload);
        return new TnsPacket(type, flags, payload);
    }
}

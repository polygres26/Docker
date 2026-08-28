package com.nexagres.wire.orawire.wireformat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

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
            in.readFully(dataFlags);
            preambleLength += 2;
        }
        byte[] payload = new byte[length - preambleLength];
        in.readFully(payload);
        return new TnsPacket(type, flags, payload);
    }
}

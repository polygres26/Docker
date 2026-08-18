package com.polygres.wire.mssqlwire.wireformat;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;

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
        header[4] = 0;
        header[5] = 0;
        header[6] = (byte) (++packetIdCounter & 0xFF);
        header[7] = 0;
        out.write(header);
        out.write(payload);
        out.flush();
    }

    public TdsPacket() {
        
    }
}

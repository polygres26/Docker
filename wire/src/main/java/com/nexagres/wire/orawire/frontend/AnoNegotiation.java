package com.nexagres.wire.orawire.frontend;

import com.nexagres.wire.orawire.wireformat.TnsPacket;
import com.nexagres.wire.orawire.wireformat.TnsPacketReader;
import com.nexagres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;

public final class AnoNegotiation {

    private static final long NSN_MAGIC = 0xDEADBEEFL;

    private static final String ANO_RESPONSE_B64 =
        "3q2+7wB1AAAAAAAEAAAEAAMAAAAAAAQABRcaIAAAAgAGAB8ADgAB3q2+7wADAAAAAgAEAAEAAQACAAAAAAAEAAUXGiAA"
            + "AAIABvv/AAIAAgAAAAAABAAFFxogAAABAAIAAAMAAgAAAAAABAAFFxogAAABAAIA";

    public void perform(TnsPacketReader reader, OutputStream out) throws IOException {
        readAnoRequest(reader);
        sendAnoResponse(out, reader.isLargeSdu());
    }

    private void readAnoRequest(TnsPacketReader reader) throws IOException {
        TnsPacket packet = reader.readPacket();
        byte[] payload = packet.payload();
        if (payload.length < 4) {
            throw new IOException("expected NSN/ANO packet, got " + payload.length + "-byte payload");
        }
        long magic = ((payload[0] & 0xFFL) << 24) | ((payload[1] & 0xFFL) << 16)
                | ((payload[2] & 0xFFL) << 8) | (payload[3] & 0xFFL);
        if (magic != NSN_MAGIC) {
            throw new IOException("expected NSN/ANO magic 0xDEADBEEF, got 0x" + Long.toHexString(magic));
        }
        
    }

    private void sendAnoResponse(OutputStream out, boolean largeSdu) throws IOException {
        byte[] payload = Base64.getDecoder().decode(ANO_RESPONSE_B64);
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, payload);
        out.write(packet.encode(largeSdu));
        out.flush();
    }
}

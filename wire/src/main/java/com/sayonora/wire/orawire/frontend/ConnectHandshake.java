package com.sayonora.wire.orawire.frontend;

import com.sayonora.wire.orawire.ttc.TtcReader;
import com.sayonora.wire.orawire.ttc.TtcWriter;
import com.sayonora.wire.orawire.wireformat.TnsPacket;
import com.sayonora.wire.orawire.wireformat.TnsPacketReader;
import com.sayonora.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public final class ConnectHandshake {

    private static final int TNS_MAX_CONNECT_DATA = 230;
    
    private static final int ACCEPT_MAX_SUPPORTED_VERSION = 320;
    private static final int ACCEPT_PROTOCOL_VERSION_LEGACY = 317;
    private static final int ACCEPT_SDU = 8192;
    private static final int ACCEPT_TDU = 8192;
    private static final int ACCEPT_TDU_RICH = 2_097_152;

    public ConnectDescriptor perform(TnsPacketReader reader, OutputStream out) throws IOException {
        TnsPacket connectPacket = reader.readPacket();
        if (connectPacket.type() != TnsPacketType.CONNECT) {
            throw new IOException("Expected CONNECT packet, got " + connectPacket.type());
        }
        TtcReader r = new TtcReader(connectPacket.payload());
        int clientDesiredVersion = r.readUint16BE();
        r.skip(2);
        int clientServiceOptions = r.readUint16BE();
        r.skip(2);
        r.skip(2);
        r.skip(2);
        r.skip(2);
        int clientByteOrder = r.readUint16BE();
        int connectStringLen = r.readUint16BE();
        r.skip(2);
        r.skip(4);
        r.skip(1);
        r.skip(1);
        r.skip(8 * 3);
        r.skip(4);
        r.skip(4);
        r.skip(4);
        r.skip(4);
        
        byte[] descriptorBytes;
        if (r.hasRemaining()) {
            descriptorBytes = r.readRemaining();
        } else if (connectStringLen > 0) {
            
            TnsPacket dataPacket = reader.readPacket();
            if (dataPacket.type() != TnsPacketType.DATA) {
                throw new IOException("Expected DATA packet carrying connect descriptor, got " + dataPacket.type());
            }
            descriptorBytes = dataPacket.payload();
        } else {
            descriptorBytes = new byte[0];
        }
        String connectString = new String(descriptorBytes, StandardCharsets.US_ASCII);
        ConnectDescriptor descriptor = ConnectDescriptor.parse(connectString);

        int negotiatedVersion = negotiateVersion(clientDesiredVersion);
        sendAccept(out, negotiatedVersion, clientServiceOptions, clientByteOrder);
        reader.setLargeSdu(true);

        // A negotiated version >= 320 is what puts O5LogonHandler on its "rich"/ANO-eligible auth
        // path -- confirmed live: sqlcl requests 319 (legacy path, works), real native-OCI sqlplus
        // requests 320 (rich path). See O5LogonHandler's isDblinkProgram/PHASE_ONE_TERMINATOR_SHORT
        // for the real, captured differences within that rich path itself.
        reader.setAnoEligible(negotiatedVersion >= 320);
        return descriptor;
    }

    private static int negotiateVersion(int clientDesiredVersion) {
        if (clientDesiredVersion < 320) {
            return ACCEPT_PROTOCOL_VERSION_LEGACY;
        }
        return Math.min(clientDesiredVersion, ACCEPT_MAX_SUPPORTED_VERSION);
    }

    private void sendAccept(OutputStream out, int negotiatedVersion, int clientServiceOptions, int clientByteOrder)
            throws IOException {
        
        boolean richShape = negotiatedVersion >= 320;
        TtcWriter w = new TtcWriter();
        w.writeUint16BE(negotiatedVersion);
        w.writeUint16BE(richShape ? clientServiceOptions : 0);
                                                                
        w.writeUint16BE(0);
        w.writeUint16BE(0);
        w.writeUint16BE(richShape ? clientByteOrder : 0);
        w.writeUint16BE(0);
                             
        w.writeUint16BE(richShape ? 61 : 0);
        if (richShape) {
            
            w.writeUint8(0x41);
            w.writeUint8(0x41);
        } else {
            w.writeUint8(0);
            w.writeUint8(0x08);
                                 
        }
        w.writeUint16BE(0);
        w.writeUint16BE(0);
        w.writeUint16BE(0);
        w.writeUint16BE(0);
        w.writeUint32BE(ACCEPT_SDU);
                                      
        w.writeUint32BE(richShape ? ACCEPT_TDU_RICH : ACCEPT_TDU);
        w.writeUint8(0);
        
        if (negotiatedVersion >= 318) {
            w.writeUint32BE(0x1a000000L);
        }
        if (negotiatedVersion >= 319) {
            w.writeRaw(databaseUuidBytes());
        }

        TnsPacket accept = new TnsPacket(TnsPacketType.ACCEPT, 0, w.toByteArray());
        out.write(accept.encode(false));
        out.flush();
    }

    private static byte[] databaseUuidBytes() {
        java.util.UUID uuid = java.util.UUID.randomUUID();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }
}

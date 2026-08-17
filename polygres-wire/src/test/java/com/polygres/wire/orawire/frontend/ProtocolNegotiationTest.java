package com.polygres.wire.orawire.frontend;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.polygres.wire.orawire.ttc.TtcWriter;
import com.polygres.wire.orawire.wireformat.TnsPacket;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
import com.polygres.wire.orawire.wireformat.TnsPacketType;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Parses our PROTOCOL/DATA_TYPES responses with the exact client-side
 * reader logic from reference/protocol_negotiation_spec.md §2.2/§2.3/§3.2,
 * confirming the response is well-formed and that
 * server_compile_caps[TNS_CCAP_FIELD_VERSION] matches what
 * PROTOCOL_RESPONSE_B64 actually carries.
 *
 * The PROTOCOL response is unconditional (same real capture regardless of
 * client), but the DATA_TYPES response is not: {@link
 * ProtocolNegotiation#pythonThinClient} picks a minimal 3-byte empty-list
 * shape for python-oracledb (which this test's {@code sendProtocolRequest}
 * declares itself as, matching real python-oracledb traffic) versus the
 * full DATA_TYPES_RESPONSE_B64 real-capture replay for everything else
 * (ojdbc11) — see that field's javadoc for why the two clients need
 * different shapes here. This test only exercises the python-thin-client
 * path; there's no equivalent test yet for the ojdbc11-shaped branch.
 *
 * field_version's expected value was updated once, live, this session:
 * ProtocolNegotiation's PROTOCOL response used to be a hand-assembled,
 * minimal placeholder pinning field_version at plain 12.2 (8) — since
 * replaced with a byte-exact replay of a real Oracle Database 23 Free
 * session's own response (see PROTOCOL_RESPONSE_B64's javadoc for why:
 * it's what finally got ojdbc11 past O5LOGON). This test's assertion was
 * still pinned to the old, no-longer-true value — caught by `mvn test`
 * failing after that swap landed. 27 is read directly off the real
 * capture, not chosen; if PROTOCOL_RESPONSE_B64 is ever byte-diffed
 * against yet another real session, update this constant to match, don't
 * just delete the assertion.
 */
class ProtocolNegotiationTest {

    @Test
    void serverResponsesParseCorrectlyAndPinFieldVersion() throws Exception {
        PipedOutputStream clientToServer = new PipedOutputStream();
        PipedInputStream serverReadsFromClient = new PipedInputStream(clientToServer, 1 << 16);
        ByteArrayOutputStream serverToClient = new ByteArrayOutputStream();

        CompletableFuture<Void> serverDone = CompletableFuture.runAsync(() -> {
            try {
                new ProtocolNegotiation().perform(new TnsPacketReader(serverReadsFromClient), serverToClient);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        sendProtocolRequest(clientToServer);
        sendDataTypesRequest(clientToServer);
        serverDone.get();

        DataInputStream serverBytes = new DataInputStream(new ByteArrayInputStream(serverToClient.toByteArray()));
        TnsPacket protocolResponse = new TnsPacketReader(serverBytes).readPacket();
        int fieldVersion = parseProtocolResponse(protocolResponse.payload());
        assertEquals(27, fieldVersion,
                "ttc_field_version byte must match PROTOCOL_RESPONSE_B64's real capture (see class javadoc)");

        TnsPacket dataTypesResponse = new TnsPacketReader(serverBytes).readPacket();
        assertEquals(3, dataTypesResponse.payload().length, "DATA_TYPES response should be msgType byte + 2-byte empty-list terminator (python-thin-client shape, see class javadoc)");
        assertEquals(2, dataTypesResponse.payload()[0] & 0xFF); // MSG_TYPE_DATA_TYPES
        assertEquals(0, dataTypesResponse.payload()[1] & 0xFF); // uint16BE(0) high byte
        assertEquals(0, dataTypesResponse.payload()[2] & 0xFF); // uint16BE(0) low byte
    }

    /** Client-side PROTOCOL response parse, transcribed from spec §2.2/§2.3. */
    private int parseProtocolResponse(byte[] payload) {
        int pos = 0;
        assertEquals(1, payload[pos++] & 0xFF); // MSG_TYPE_PROTOCOL
        pos++; // server_version
        pos++; // zero byte
        while (payload[pos++] != 0) {
            // banner, NUL-terminated
        }
        pos += 2; // charset id (uint16 LE)
        pos++; // server_flags
        int numElem = (payload[pos] & 0xFF) | ((payload[pos + 1] & 0xFF) << 8);
        pos += 2;
        pos += numElem * 5;
        int fdoLength = ((payload[pos] & 0xFF) << 8) | (payload[pos + 1] & 0xFF);
        pos += 2;
        byte[] fdo = java.util.Arrays.copyOfRange(payload, pos, pos + fdoLength);
        pos += fdoLength;
        assertEquals(true, fdoLength >= 7, "fdo must satisfy client's minimum length check");
        int ix = 6 + fdo[5] + fdo[6];
        assertEquals(true, fdoLength >= ix + 5, "fdo must satisfy client's second length check");

        int compileCapsLen = payload[pos++] & 0xFF;
        byte[] compileCaps = java.util.Arrays.copyOfRange(payload, pos, pos + compileCapsLen);
        pos += compileCapsLen;
        return compileCaps[7] & 0xFF; // TNS_CCAP_FIELD_VERSION index
    }

    private void sendProtocolRequest(PipedOutputStream out) throws IOException {
        TtcWriter w = new TtcWriter();
        w.writeUint8(1); // MSG_TYPE_PROTOCOL
        w.writeUint8(6);
        w.writeUint8(0);
        w.writeBytesWithLength("python-oracledb".getBytes());
        w.writeUint8(0);
        send(out, w);
    }

    private void sendDataTypesRequest(PipedOutputStream out) throws IOException {
        TtcWriter w = new TtcWriter();
        w.writeUint8(2); // MSG_TYPE_DATA_TYPES
        w.writeUint16LE(873);
        w.writeUint16LE(873);
        w.writeUint8(0x03);
        w.writeBytesWithLength(new byte[8]);
        w.writeBytesWithLength(new byte[7]);
        w.writeUint16BE(0); // empty type list
        send(out, w);
    }

    private void send(PipedOutputStream out, TtcWriter w) throws IOException {
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, w.toByteArray());
        out.write(packet.encode(false));
        out.flush();
    }
}

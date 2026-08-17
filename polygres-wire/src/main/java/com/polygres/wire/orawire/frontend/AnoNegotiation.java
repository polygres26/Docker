package com.polygres.wire.orawire.frontend;

import com.polygres.wire.orawire.wireformat.TnsPacket;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
import com.polygres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Base64;

/**
 * ANO (Advanced Networking Option) negotiation, run once between {@link ConnectHandshake} and
 * {@link ProtocolNegotiation} for clients that negotiated protocol version &gt;=320 (real OCI
 * clients — sqlplus, rwloadsim — not python-oracledb/ojdbc11, which stay on the legacy &lt;320
 * ACCEPT path where {@code flags1} still forces {@code noAnoServices} and this exchange never
 * happens at all).
 *
 * <p>This is <b>not</b> a plain TTC message — a real OCI client's whole capability negotiation at
 * this tier (ANO's four services: authentication, encryption, data integrity, supervisor) rides
 * inside a distinct envelope confirmed live via MITM capture (a real {@code sqlplus} client
 * proxied to a real Oracle Database 23ai instance): every packet in this exchange starts with the
 * 4-byte magic {@code DE AD BE EF} ("NSN" — Native Services Negotiation), not a small TTC message-
 * type integer. This differs from what was assumed before this class existed (a bare TTC message
 * type 222) — that assumption was never actually confirmed against real traffic; this class is
 * built directly against the real bytes.
 *
 * <p><b>Scope, deliberately narrow</b>: this negotiates every ANO service down to "none" — no
 * encryption, no checksumming, no ANO-specific authentication algorithm, no supervisor service.
 * The session stays plaintext exactly as it already does on the legacy (&lt;320) path. Concretely,
 * this class does not attempt to parse the client's requested algorithm lists at all (see below
 * for why that's safe here) — real Diffie-Hellman-style key exchange and RC4/AES session wrapping
 * are <b>not implemented</b> and remain explicit future work; any client whose own {@code
 * sqlnet.ora} <i>requires</i> (not just allows) one of these services will still fail past this
 * point.
 *
 * <p><b>Why a static byte-exact reply is safe here, not a parsed-and-resolved response</b>: the
 * response below is replayed verbatim from the real Oracle server's own reply, captured live
 * against a client with no {@code sqlnet.ora} restrictions configured (so its own request already
 * carried only the server-default/no-crypto-required preferences) — i.e. it already <i>is</i> a
 * real "resolved to no crypto" negotiation outcome, not a synthesized one. This follows the same
 * precedent already established in this package (see {@link ProtocolNegotiation}'s
 * {@code PROTOCOL_RESPONSE_B64}): byte-exact replay of a real, internally-consistent capture,
 * rather than hand-assembling a response from an incompletely reverse-engineered grammar. The
 * NSN envelope's internal structure (repeated {@code 04 00 05 17 1a 20 00} sub-packet markers, a
 * nested nested-magic sub-block within the first service) was examined during capture and found
 * consistent between the client's request and the server's own reply in both sessions this was
 * tested against, supporting that it is not connection-specific data that would break under
 * verbatim replay — but full grammar-level parsing was not required to reach the "negotiate
 * everything to none" scope decision, so this class deliberately doesn't attempt it.
 */
public final class AnoNegotiation {

    private static final long NSN_MAGIC = 0xDEADBEEFL;

    /**
     * Real Oracle server's own NSN response, replayed verbatim (see class javadoc) — captured
     * live resolving all four ANO services (authentication, encryption, data integrity,
     * supervisor) to no algorithm selected, against a client with no ANO requirements configured.
     */
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
        // The rest of the client's request (its per-service ranked algorithm-ID lists) is
        // intentionally not parsed — see class javadoc: this negotiates every service to "none"
        // unconditionally via a static reply, so there's nothing in the request that changes the
        // response.
    }

    private void sendAnoResponse(OutputStream out, boolean largeSdu) throws IOException {
        byte[] payload = Base64.getDecoder().decode(ANO_RESPONSE_B64);
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, payload);
        out.write(packet.encode(largeSdu));
        out.flush();
    }
}

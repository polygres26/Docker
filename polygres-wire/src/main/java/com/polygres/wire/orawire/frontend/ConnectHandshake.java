package com.polygres.wire.orawire.frontend;

import com.polygres.wire.orawire.ttc.TtcReader;
import com.polygres.wire.orawire.ttc.TtcWriter;
import com.polygres.wire.orawire.wireformat.TnsPacket;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
import com.polygres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Server side of the TNS connect exchange, per a live packet capture of a
 * real python-oracledb client and python-oracledb's own
 * impl/thin/messages/connect.pyx (ConnectMessage.send/.process — the
 * client's write/read side; ours is the mirror).
 *
 * The CONNECT packet is NOT just "8-byte header + descriptor string" (an
 * earlier version of this class assumed that and silently misparsed real
 * traffic): it carries 66 bytes of fixed connect-negotiation fields first
 * (version, SDU/TDU, flags, connect_string_len, etc.), and the descriptor
 * string itself is either appended inline (if it fits under
 * TNS_MAX_CONNECT_DATA = 230 bytes) or sent as a separate immediately-following
 * DATA packet (connect.pyx:135-138) — real clients' descriptors routinely
 * exceed 230 bytes once CID/PROGRAM/HOST/USER metadata is included, so the
 * separate-packet case is the common one, not an edge case.
 *
 * Also per connect.pyx:80, our ACCEPT response unconditionally switches the
 * client to 4-byte-length ("large SDU") packet framing for every packet
 * after this one — see {@link TnsPacket}'s javadoc.
 */
public final class ConnectHandshake {

    private static final int TNS_MAX_CONNECT_DATA = 230;
    // >= 315 (min accepted). A real Oracle-to-Oracle negotiation between two full clients (e.g.
    // sqlplus/OCI talking to a real Oracle 23ai server, captured live) settles at 320, matching
    // whatever the client itself declared as its desired version — not a fixed constant like this
    // class used to reply with unconditionally. See {@link #negotiateVersion} for why a fixed 317
    // (a deliberate choice to dodge version>=318/319's extra trailing fields — see the two
    // version-gated writes in sendAccept below) works for ojdbc11/python-oracledb but a real OCI
    // client (sqlplus, ORA-12592) rejects it outright: found live that OCI's own ACCEPT parser is
    // considerably stricter than either thin reimplementation's.
    private static final int ACCEPT_MAX_SUPPORTED_VERSION = 320;
    private static final int ACCEPT_PROTOCOL_VERSION_LEGACY = 317; // the old fixed response, still used below 318
    private static final int ACCEPT_SDU = 8192;
    private static final int ACCEPT_TDU = 8192;
    private static final int ACCEPT_TDU_RICH = 2_097_152; // rich-shape (>=320) real TDU, captured live — see sendAccept

    public ConnectDescriptor perform(TnsPacketReader reader, OutputStream out) throws IOException {
        TnsPacket connectPacket = reader.readPacket();
        if (connectPacket.type() != TnsPacketType.CONNECT) {
            throw new IOException("Expected CONNECT packet, got " + connectPacket.type());
        }
        TtcReader r = new TtcReader(connectPacket.payload());
        int clientDesiredVersion = r.readUint16BE(); // was skipped entirely before — now drives negotiateVersion
        r.skip(2); // version minimum
        int clientServiceOptions = r.readUint16BE(); // was skipped — now echoed back, see sendAccept
        r.skip(2); // sdu
        r.skip(2); // tdu
        r.skip(2); // protocol characteristics
        r.skip(2); // line turnaround
        int clientByteOrder = r.readUint16BE(); // was skipped — now echoed back, see sendAccept
        int connectStringLen = r.readUint16BE();
        r.skip(2); // offset to connect data
        r.skip(4); // max receivable data
        r.skip(1); // nsi flags
        r.skip(1); // nsi flags
        r.skip(8 * 3); // 3x obsolete uint64 fields
        r.skip(4); // sdu (large)
        r.skip(4); // tdu (large)
        r.skip(4); // connect flags 1
        r.skip(4); // connect flags 2
        // 66 bytes of fixed fields consumed above

        byte[] descriptorBytes;
        if (r.hasRemaining()) {
            descriptorBytes = r.readRemaining();
        } else if (connectStringLen > 0) {
            // descriptor exceeded TNS_MAX_CONNECT_DATA and was sent as its own DATA packet
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
        reader.setLargeSdu(true); // connect.pyx:80 — unconditional on the client, once it processes our ACCEPT
        // Rich shape (>=320) now advertises real ANO availability (see sendAccept) — record it so
        // SessionHandler knows to run AnoNegotiation before ProtocolNegotiation. Below 320,
        // flags1 still forces noAnoServices, so the client never sends an NSN/ANO packet at all.
        reader.setAnoEligible(negotiatedVersion >= 320);
        return descriptor;
    }

    /**
     * Below 320: keep replying with the old fixed 317 — the exact shape already verified live
     * against ojdbc11 (decompiled bytecode) and python-oracledb (which itself declares a desired
     * version of 319 — still routed to the legacy path here, deliberately, since that's the exact
     * shape already proven against it; nothing about a real client's own desired version obligates
     * matching it exactly, a server is always free to negotiate down). 320 and above: echo back
     * the client's own desired version (capped at {@link #ACCEPT_MAX_SUPPORTED_VERSION}) — the
     * real Oracle-to-Oracle negotiation this was reverse-engineered from (a real server talking to
     * a real OCI/sqlplus client, captured live) settles at exactly 320, not lower, and {@link
     * #sendAccept}'s version-gated trailing fields (>=318 compression-info, >=319 database UUID)
     * follow that. 320 as the cutover point, not 318, is a deliberate risk boundary: it only
     * changes behavior for a version class of client (>=320) nothing currently works for anyway
     * (real OCI's ORA-12592 against the old fixed 317), so there's no regression surface among
     * already-working clients to worry about.
     */
    private static int negotiateVersion(int clientDesiredVersion) {
        if (clientDesiredVersion < 320) {
            return ACCEPT_PROTOCOL_VERSION_LEGACY;
        }
        return Math.min(clientDesiredVersion, ACCEPT_MAX_SUPPORTED_VERSION);
    }

    /**
     * Field layout below is not the old python-oracledb-only guess (which worked for
     * python-oracledb's parser but threw {@code IndexOutOfBoundsException} out of the real Java
     * thin driver's {@code oracle.net.ns.NIOAcceptPacket.readPayloadBuffer()} — found live running
     * Swingbench's {@code charbench}/{@code oewizard}, which use ojdbc11, not python-oracledb).
     * This layout was reverse-engineered by decompiling {@code NIOAcceptPacket.class} directly
     * (javap -c against ojdbc11.jar's actual bytecode, not guessed from docs) to get the exact
     * absolute byte offsets and field semantics the Java driver reads. All the "must be 0" fields
     * below are load-bearing: each is a length/flag the driver's own reader branches on to decide
     * whether to read further trailing data (e.g. a non-zero connect-data length or reconnect-
     * address length sends the driver off trying to read bytes we don't send) — 0 keeps every one
     * of those optional-data branches closed, which is what keeps this a fixed 33-byte payload.
     */
    private void sendAccept(OutputStream out, int negotiatedVersion, int clientServiceOptions, int clientByteOrder)
            throws IOException {
        // Below the 320 cutover, every field below keeps its exact old value — the already-proven
        // shape for ojdbc11/python-oracledb, byte-for-byte unchanged. At/above 320, several fields
        // switch to echoing the client's own CONNECT values or matching a real Oracle server's
        // observed response — found live, field by field, that a real OCI client (sqlplus) is far
        // stricter here than either thin reimplementation: it doesn't just want *some* accept
        // payload of the right length, several fields have to match what a real peer would send.
        boolean richShape = negotiatedVersion >= 320;
        TtcWriter w = new TtcWriter();
        w.writeUint16BE(negotiatedVersion);
        w.writeUint16BE(richShape ? clientServiceOptions : 0); // protocol_options — echoed at >=320 (real Oracle
                                                                // does this; TNS_GSO_DONT_CARE-equivalent 0 below)
        w.writeUint16BE(0); // provisional sdu — superseded below by the real (int) value the driver reads instead
        w.writeUint16BE(0); // provisional tdu — superseded below
        w.writeUint16BE(richShape ? clientByteOrder : 0); // "my" host byte-order marker — echoed at >=320
        w.writeUint16BE(0); // trailing connect-data length — 0: we send none, so the driver's own
                             // trailing-read branch (offset 10-11 > 0) never fires
        // trailing connect-data buffer offset — unused since length above is 0, but a real Oracle
        // server (captured live) always sets it to the packet's own total size (self-referential,
        // "offset past the end") rather than 0 even when there's no data — matched here for the
        // rich shape (a fixed 61 = 8-byte TNS header + this method's fixed 53-byte payload) since
        // this class is otherwise byte-exact with a real capture at this point and there was no
        // reason to leave one more unexplained difference in place.
        w.writeUint16BE(richShape ? 61 : 0);
        if (richShape) {
            // flags0/flags1 = 0x41/0x41, byte-exact match to a real Oracle server's own ACCEPT at
            // this version tier (confirmed live via a real sqlplus<->real-Oracle-23ai MITM
            // capture). Setting these bits advertises real ANO availability, which is what makes
            // a real OCI client proceed into the NSN/ANO negotiation exchange (see
            // AnoNegotiation) instead of skipping it — previously this class deliberately kept
            // the same flags1=0x08 "noAnoServices" bit used below 320, since nothing spoke ANO
            // yet; now that AnoNegotiation exists, the real flags are used instead. ANO itself is
            // negotiated down to "none" for every service (see AnoNegotiation's javadoc) — the
            // session stays plaintext, so this change carries no crypto/encryption risk on its
            // own; it only changes which negotiation exchange the client attempts.
            w.writeUint8(0x41);
            w.writeUint8(0x41);
        } else {
            w.writeUint8(0); // flags0
            w.writeUint8(0x08); // flags1 — bit 0x08 sets the driver's noAnoServices=true, which is what actually
                                 // skips the ANO (Advanced Networking Option) negotiation exchange entirely; found
                                 // live — without it the real Java driver launches into an ANO handshake our server
                                 // doesn't speak, and the session dies. Must NOT also set TNS_NSI_NA_REQUIRED
                                 // (0x10) or the client demands Native Network Encryption instead.
        }
        w.writeUint16BE(0); // timeout — 0 keeps the driver out of its connection-pooling/session-id read path
        w.writeUint16BE(0); // tick
        w.writeUint16BE(0); // reconnect-address length — 0: skips that trailing read too
        w.writeUint16BE(0); // reconnect-address buffer-position field — unused since length above is 0
        w.writeUint32BE(ACCEPT_SDU); // real SDU, absolute offset 24 — read unconditionally once the
                                      // driver sees protocol_version >= 315 (ours is 317)
        // Real TDU, absolute offset 28. At >=320, a real Oracle server's own observed value here
        // (captured live) is 2097152 (0x200000) — much larger than the 8192 every lower-version
        // client gets, and not something ojdbc11/python-oracledb have ever been tested against, so
        // it's only used on the rich-shape path, not swapped in for everyone.
        w.writeUint32BE(richShape ? ACCEPT_TDU_RICH : ACCEPT_TDU);
        w.writeUint8(0); // network-compression flags byte, absolute offset 32 — 0 = compression off
        // Version-gated trailing fields — present only when negotiatedVersion actually requires
        // them (see negotiateVersion's javadoc for who gets which shape). Found live against a
        // real Oracle server/OCI client pair at version 320: a 4-byte compression-info field
        // (0x1a000000 in every capture so far — treated as a static capability word, not
        // instance-specific, so replayed verbatim like ProtocolNegotiation's captures), then a
        // 16-byte value in the database-UUID slot. The UUID is sent as a zero-filled placeholder,
        // not a byte-exact replay — unlike the compression-info word, that field looked genuinely
        // instance-specific (a real database's own UUID), so replaying one real server's exact
        // value into every PolyWire deployment would be actively wrong, not just unnecessary.
        // Whether a real OCI client accepts a placeholder here rather than a "real-looking" UUID is
        // exactly what live verification against real sqlplus is for.
        if (negotiatedVersion >= 318) {
            w.writeUint32BE(0x1a000000L); // compression-info, absolute offset 33
        }
        if (negotiatedVersion >= 319) {
            w.writeRaw(databaseUuidBytes()); // database UUID, absolute offset 37
        }

        TnsPacket accept = new TnsPacket(TnsPacketType.ACCEPT, 0, w.toByteArray());
        out.write(accept.encode(false)); // ACCEPT itself is still legacy 2-byte-length framing
        out.flush();
    }

    /** DIAGNOSTIC — see the "database UUID" comment above sendAccept's trailing-field writes. A fresh random 16 bytes, not a real database UUID, just to test whether a real OCI client only cares that this slot looks non-zero at all. */
    private static byte[] databaseUuidBytes() {
        java.util.UUID uuid = java.util.UUID.randomUUID();
        java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }
}

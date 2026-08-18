package com.polygres.wire.orawire.frontend;

import com.polygres.wire.orawire.ttc.TtcReader;
import com.polygres.wire.orawire.ttc.TtcWriter;
import com.polygres.wire.orawire.wireformat.TnsPacket;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
import com.polygres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;

/**
 * PROTOCOL / DATA_TYPES capability negotiation, per
 * reference/protocol_negotiation_spec.md. Runs after TNS CONNECT/ACCEPT and
 * before O5LOGON.
 *
 * Both responses are replayed verbatim from a live packet capture of a real
 * Oracle Database 23 Free instance (see PROTOCOL_RESPONSE_B64's javadoc) —
 * an earlier hand-assembled version (driver name/charset/flags built field
 * by field, minimal/placeholder capability arrays) left out real fields
 * entirely (a character-set graph array) and used capability bytes that,
 * while individually well-intentioned, weren't internally consistent with
 * each other, causing subtle downstream ORA-17401s during O5LOGON. Byte-
 * exact replay of a real, internally-consistent pair sidesteps needing to
 * fully understand every field's semantics.
 *
 * Does not advertise fast-auth or end-of-response support (our
 * ConnectHandshake's ACCEPT already sends flags=0, satisfying the
 * precondition per spec §4), so both PROTOCOL and DATA_TYPES are simple,
 * non-pipelined, single-packet-per-message exchanges — this code doesn't
 * attempt byte-exact parsing of the client's declared data-type list (spec
 * §3.1 has fields whose exact repeat-count wasn't fully pinned down); it's
 * safe to just stop reading partway into that packet's payload once we
 * have what we need, since the next read starts a fresh TNS packet.
 */
public final class ProtocolNegotiation {

    private static final int MSG_TYPE_PROTOCOL = 1;
    private static final int MSG_TYPE_DATA_TYPES = 2;
    // Confirmed live (real sqlplus <-> real Oracle 23ai MITM capture): the extended
    // PROTOCOL-equivalent message a real OCI client sends at protocol version >=320, right after
    // ANO negotiation (see AnoNegotiation) completes. Same underlying purpose as MSG_TYPE_PROTOCOL
    // (driver-name exchange) but a longer, differently-shaped request header — see
    // readProtocolRequest — and a differently-shaped reply, replayed verbatim like
    // PROTOCOL_RESPONSE_B64 below — see PROTOCOL_RESPONSE_EXTENDED_B64.
    private static final int MSG_TYPE_PROTOCOL_EXTENDED =
        com.polygres.wire.orawire.ttc.TtcConstants.MSG_TYPE_PROTOCOL_EXTENDED;

    /**
     * Set by readProtocolRequest from the client's own driver-name string, used by
     * sendDataTypesResponse to pick the right response shape. See DATA_TYPES_RESPONSE_B64's
     * javadoc for why this distinction exists: python-oracledb's DataTypesMessage
     * ._process_message reads straight into a (data_type, conv_data_type) pair loop right
     * after the msgtype byte, with NO charset/ncharset/capability prefix — unlike ojdbc11's
     * T4C8TTIdty, which does read such a prefix. Sending the ojdbc-shaped verbatim capture to
     * python-oracledb makes it misinterpret the charset/capability bytes as garbled type-pair
     * data, walking off the end of the intended message and eventually misreading an
     * unrelated byte elsewhere as a message type — surfaced as DPY-5002 ("read integer of
     * length 25 when expecting integer of no more than length 4"), confirmed live.
     */
    private boolean pythonThinClient;

    /**
     * Set by readProtocolRequest when the client's request was {@link #MSG_TYPE_PROTOCOL_EXTENDED}
     * rather than the older {@link #MSG_TYPE_PROTOCOL} — drives sendProtocolResponse to reply with
     * PROTOCOL_RESPONSE_EXTENDED_B64 instead of PROTOCOL_RESPONSE_B64. Only ever true at protocol
     * version &gt;=320, right after AnoNegotiation.
     */
    private boolean extendedProtocol;

    public void perform(TnsPacketReader reader, OutputStream out) throws IOException {
        readProtocolRequest(reader);
        sendProtocolResponse(out, reader.isLargeSdu());
        readDataTypesRequest(reader);
        sendDataTypesResponse(out, reader.isLargeSdu());
    }

    private void readProtocolRequest(TnsPacketReader reader) throws IOException {
        TnsPacket packet = reader.readPacket();
        TtcReader r = new TtcReader(packet.payload());
        int msgType = r.readUint8();
        if (msgType == MSG_TYPE_PROTOCOL_EXTENDED) {
            extendedProtocol = true;
            // Confirmed live: 9 bytes of fixed header follow the message-type byte before the
            // driver-name string starts, vs. MSG_TYPE_PROTOCOL's 2-byte (version + zero) header
            // below. Their exact field semantics weren't resolved during capture (the response is
            // a static replay either way — see PROTOCOL_RESPONSE_EXTENDED_B64 — so nothing here
            // depends on interpreting them), so they're skipped rather than guessed at.
            r.skip(9);
        } else if (msgType == MSG_TYPE_PROTOCOL) {
            extendedProtocol = false;
            r.readUint8(); // client protocol version, unused
            r.readUint8(); // zero byte
        } else {
            throw new IOException("expected PROTOCOL message, got type " + msgType);
        }
        // Driver name is a raw NUL-terminated string, NOT length-prefixed —
        // confirmed against a live capture of python-oracledb 4.0.2's actual
        // PROTOCOL request bytes ("...70 79 74 68 6f 6e 2d 6f 72 61 63 6c 65
        // 64 62 00" = ASCII "python-oracledb" + one NUL byte, no length
        // byte). This corrects protocol_negotiation_spec.md §2.1, which
        // assumed a length-prefixed encoding. Confirmed to hold for the extended (type 34)
        // request shape too — same driver-name encoding, just a longer fixed header ahead of it.
        StringBuilder driverName = new StringBuilder();
        int b;
        while ((b = r.readUint8()) != 0) {
            driverName.append((char) b);
        }
        pythonThinClient = driverName.toString().toLowerCase(java.util.Locale.ROOT).contains("python-oracledb");
        // anything else in this packet is ignored (see class javadoc) — for the extended shape
        // this includes a substantial amount of trailing session-attribute data (AUTH_TERMINAL,
        // AUTH_PROGRAM_NM, AUTH_MACHINE, etc.) real OCI clients bundle into this same packet;
        // ignoring it is safe by the same "stop reading partway, next read starts a fresh TNS
        // packet" reasoning already established for the non-extended shape.
    }

    /**
     * Full real PROTOCOL response, replayed verbatim from a live packet capture of a
     * real Oracle Database 23 Free instance talking to a real ojdbc11 client (proxied
     * through a byte-logging TCP relay — tcpdump wasn't available, so this used a
     * small Java relay instead, same technique as the DATA_TYPES fix below but this
     * time paired with a matching DATA_TYPES response from the SAME session rather
     * than a different repo's capture).
     *
     * Superseded a hand-assembled version (driver name/charset/flags fields built
     * manually, then compile/runtime-capability arrays reconstructed field-by-field
     * from the `orawire` repo's own CompileTimeCapabilitiesImpl/RunTimeCapabilitiesImpl
     * constants). That intermediate version was a real, verified fix on its own — it
     * eliminated the ORA-17401 T4CConnection.reNegotiateTTCProDty failure entirely by
     * finally advertising KPCCAP_CTB_TTC4_RENEG — but its DATA_TYPES pairing (a
     * *different* capture, from `orawire`'s own server) turned out to still be
     * capability-inconsistent with the richer PROTOCOL response, producing a new,
     * different ORA-17401 ("[ 0, ]") right after. Replaying both halves from the same
     * real session removes that whole class of cross-capture mismatch.
     *
     * Also includes an 50-byte, 10-entry character-set graph (num_elem=10) this
     * server never sent at all before — entirely absent from every earlier version of
     * this method, not just wrong-valued.
     */
    private static final String PROTOCOL_RESPONSE_B64 =
        "AQYATGludXgzOTB4L0xpbnV4LTIuNC54AGkDIQoAZgNAAwFAA2YDAWYDSAMBSANmAwFmA1IDAVIDZgMBZgNhAwFhA2YDAWYDHwMIHwNmAwEAZAAAAGABJA8FCwwDDAwFBAUNBgkHCAUFBQUFDwUFBQUFCgUFBQUFBAUGBwgII0cjRwgRIwgRQbBHAIMDaQfQAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA2BgEBAe8PARsBAQEBAQEBf/8DEAMDAQH/Af//AQ4BAf8BBgz2CX8FD/8NCwD/AwAAAAAABwICDQIBAAEYAH8BAgAAAAA=";

    /**
     * Real extended-PROTOCOL response, replayed verbatim from the SAME live MITM capture
     * (real sqlplus &lt;-&gt; real Oracle Database 23ai) as {@link AnoNegotiation}'s response —
     * this is what a real server sends back after receiving a {@link #MSG_TYPE_PROTOCOL_EXTENDED}
     * request. Notably: it carries a single leading byte ({@code 0x1c} in the capture) before the
     * {@link #MSG_TYPE_PROTOCOL} byte and driver-name string that PROTOCOL_RESPONSE_B64 doesn't
     * have — its exact meaning wasn't resolved during capture (possibly a sequence/marker byte
     * specific to this negotiation tier), so, per this class's established byte-exact-replay
     * precedent (see PROTOCOL_RESPONSE_B64's own javadoc), it's replayed as-is rather than
     * guessed at. A real OCI client accepted this exact reply live, proceeding on to a normal
     * (non-extended) DATA_TYPES exchange immediately after — see readDataTypesRequest, which
     * needed no changes for this path.
     */
    private static final String PROTOCOL_RESPONSE_EXTENDED_B64 =
        "HAEIAExpbnV4MzkweC9MaW51eC0yLjQueABpAwEKAGYDQAMBQANmAwFmA0gDAUgDZgMBZgNSAwFSA2YDAWYDYQMBYQNm"
            + "AwFmAx8DCB8DZgMBAGQAAABgASQPBQsMAwwMBQQFDQYJBwgFBQUFBQ8FBQUFBQoFBQUFBQQFBgcICCNHI0cIESMIEUGw"
            + "RwCDA2kH0AMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANgYBAQHvDwEbAQEBAQEBAX//AxADAwEB"
            + "/wH//wEOAQH/AQYM9gl/BQ//DQsA/wMAAAAAAAcCAg0CAQABGAB/AQIAAAAAAQ==";

    private void sendProtocolResponse(OutputStream out, boolean largeSdu) throws IOException {
        byte[] payload = java.util.Base64.getDecoder()
                .decode(extendedProtocol ? PROTOCOL_RESPONSE_EXTENDED_B64 : PROTOCOL_RESPONSE_B64);
        sendData(out, payload, largeSdu);
    }

    private void readDataTypesRequest(TnsPacketReader reader) throws IOException {
        // The real Java thin driver (ojdbc11, found live running Swingbench — python-oracledb
        // doesn't do this) sends a zero-payload DATA packet ahead of its actual DATA_TYPES
        // message, apparently as a plain flush/marker rather than protocol content. Skip any
        // empty DATA packets rather than treating one as a malformed message.
        TnsPacket packet = reader.readPacket();
        while (packet.payload().length == 0) {
            packet = reader.readPacket();
        }
        TtcReader r = new TtcReader(packet.payload());
        int msgType = r.readUint8();
        if (msgType != MSG_TYPE_DATA_TYPES) {
            throw new IOException("expected DATA_TYPES message, got type " + msgType);
        }
        // charset id, ncharset id, encoding flags, client's caps arrays, and the
        // per-data-type list all follow but aren't needed server-side (see class javadoc)
    }

    // Real DATA_TYPES response, paired with PROTOCOL_RESPONSE_B64 above — both replayed
    // verbatim from the SAME real Oracle Database 23 Free session, so their capability
    // advertisements are guaranteed internally consistent (see PROTOCOL_RESPONSE_B64's
    // javadoc for why cross-capture pairing broke this before).
    private static final String DATA_TYPES_RESPONSE_B64 =
        "AgABAAEAAQAAAAIAAgAKAAAACAAIAAEAAAAMAAwACgAAABcAFwABAAAAGAAYAAEAAAAZABkAAQAAABoAGgABAAAAGwAbAAEAAAAcABwAAQAAAB0AHQABAAAAHgAeAAEAAAAfAB8AAQAAACAAIAABAAAAIQAhAAEAAAAKAAoAAQAAAAsACwABAAAAKAAoAAEAAAApACkA" +
        "AQAAAHUAdQABAAAAeAB4AAEAAAEiASIAAQAAASMBIwABAAABJAEkAAEAAAElASUAAQAAASYBJgABAAABKgEqAAEAAAErASsAAQAAASwBLAABAAABLQEtAAEAAAEuAS4AAQAAAS8BLwABAAABMQExAAEAAAEyATIAAQAAATMBMwABAAABNAE0AAEAAAE1ATUAAQAAATYB" +
        "NgABAAABNwE3AAEAAAE4ATgAAQAAATkBOQABAAABOwE7AAEAAAE8ATwAAQAAAT0BPQABAAABPgE+AAEAAAE/AT8AAQAAAUABQAABAAABQQFBAAEAAAFCAUIAAQAAAUMBQwABAAABRwFHAAEAAAFIAUgAAQAAAUkBSQABAAABSwFLAAEAAAFNAU0AAQAAAVMBUwABAAAB" +
        "VAFUAAEAAAFVAVUAAQAAAVYBVgABAAABVwFXAAEAAAFYAVgAAQAAAVkBWQABAAABWgFaAAEAAAFcAVwAAQAAAV0BXQABAAABYgFiAAEAAAFjAWMAAQAAAWcBZwABAAABawFrAAEAAAF8AXwAAQAAAX0BfQABAAABfgF+AAEAAAGAAYAAAQAAAYEBgQABAAABggGCAAEA" +
        "AAGDAYMAAQAAAYQBhAABAAABhQGFAAEAAAGGAYYAAQAAAYcBhwABAAABiQGJAAEAAAGKAYoAAQAAAYsBiwABAAABjAGMAAEAAAGNAY0AAQAAAY4BjgABAAABjwGPAAEAAAGQAZAAAQAAAZEBkQABAAABlAGUAAEAAAGVAZUAAQAAAZYBlgABAAABlwGXAAEAAAGdAZ0A" +
        "AQAAAZ4BngABAAABnwGfAAEAAAGgAaAAAQAAAaEBoQABAAABogGiAAEAAAGjAaMAAQAAAaQBpAABAAABpQGlAAEAAAGmAaYAAQAAAacBpwABAAABqAGoAAEAAAGpAakAAQAAAaoBqgABAAABqwGrAAEAAAGtAa0AAQAAAa4BrgABAAABrwGvAAEAAAGwAbAAAQAAAbEB" +
        "sQABAAABwQHBAAEAAAHCAcIAAQAAAcYBxgABAAABxwHHAAEAAAHIAcgAAQAAAckByQABAAABygHKAAEAAAHLAcsAAQAAAcwBzAABAAABzQHNAAEAAAHOAc4AAQAAAc8BzwABAAAB0gHSAAEAAAHTAdMAAQAAAdQB1AABAAAB1QHVAAEAAAHWAdYAAQAAAdcB1wABAAAB" +
        "2AHYAAEAAAHZAdkAAQAAAdoB2gABAAAB2wHbAAEAAAHcAdwAAQAAAd0B3QABAAAB3gHeAAEAAAHfAd8AAQAAAeAB4AABAAAB4QHhAAEAAAHiAeIAAQAAAeMB4wABAAAB5AHkAAEAAAHlAeUAAQAAAeYB5gABAAAB6gHqAAEAAAHrAesAAQAAAewB7AABAAAB7QHtAAEA" +
        "AAHuAe4AAQAAAe8B7wABAAAB8AHwAAEAAAHyAfIAAQAAAfMB8wABAAAB9AH0AAEAAAH1AfUAAQAAAfYB9gABAAAB/QH9AAEAAAH+Af4AAQAAAgECAQABAAACAgICAAEAAAIEAgQAAQAAAgUCBQABAAACBgIGAAEAAAIHAgcAAQAAAggCCAABAAACCQIJAAEAAAIKAgoA" +
        "AQAAAgsCCwABAAACDAIMAAEAAAINAg0AAQAAAg4CDgABAAACDwIPAAEAAAIQAhAAAQAAAhECEQABAAACEgISAAEAAAITAhMAAQAAAhQCFAABAAACFQIVAAEAAAIWAhYAAQAAAhcCFwABAAACGAIYAAEAAAIZAhkAAQAAAhoCGgABAAACGwIbAAEAAAIfAh8AAQAAAiAA" +
        "AAIhAAACIgAAAiMAAAIkAAACJQAAAiYAAAInAAACKAAAAikAAAIqAAACKwAAAiwAAAItAAACLgAAAi8AAAIwAjAAAQAAAjEAAAIyAAACMwIzAAEAAAI0AjQAAQAAAjYAAAI3AAACOAAAAjkAAAI6AAACOwAAAjwCPAABAAACPQI9AAEAAAI+Aj4AAQAAAj8CPwABAAAC" +
        "QAJAAAEAAAJBAAACQgJCAAEAAAJDAkMAAQAAAkQCRAABAAACRQJFAAEAAAJGAkYAAQAAAkcCRwABAAACSAJIAAEAAAJJAkkAAQAAAkoAAAJLAAACTAAAAk0AAAJOAk4AAQAAAk8CTwABAAACUAJQAAEAAAJRAlEAAQAAAlICUgABAAACUwJTAAEAAAJUAlQAAQAAAlUC" +
        "VQABAAACVgJWAAEAAAJXAlcAAQAAAlgCWAABAAACWQJZAAEAAAJaAloAAQAAAlsCWwABAAACXAJcAAEAAAJdAl0AAQAAAmMCYwABAAACZAJkAAEAAAJlAmUAAQAAAmYCZgABAAACZwJnAAEAAAJoAmgAAQAAAmkCaQABAAACbQAAAm4CbgABAAACbwJvAAEAAAJwAnAA" +
        "AQAAAnECcQABAAACcgJyAAEAAAJzAnMAAQAAAnQCdAABAAACdQJ1AAEAAAJ2AnYAAQAAAncCdwABAAACeAJ4AAEAAAJ5AAACegAAAnsAAAJ8AnwAAQAAAn0CfQABAAACfgJ+AAEAAAJ/An8AAQAAAoACgAABAAACgQAAAoIAAAKDAAAChAAAAoUAAAKGAoYAAQAAAocC" +
        "hwABAAACiAKIAAEAAAKJAAACigAAAosAAAKMAowAAQAAAo0CjQABAAACjwAAApACkAABAAACkQAAApIAAAKTAAAClAKUAAEAAAKVApUAAQAAApYAAAKXApcAAQAAApgAAAKZApkAAQAAApoAAAKcAAACnQAAAp4AAAADAAIACgAAAAQAAgAKAAAABQABAAEAAAAGAAIA" +
        "CgAAAAcAAgAKAAAACQABAAEAAAANAAAADgAAAA8AFwABAAAAEAAAABEAAAASAAAAEwAAABQAAAAVAAAAFgAAACcAAAA6AAAARAACAAoAAABFAAAARgAAAEoAAABMAAAAWwACAAoAAABeAAEAAQAAAF8AFwABAAAAYABgAAEAAABhAGAAAQAAAGQAZAABAAAAZQBlAAEA" +
        "AABmAGYAAQAAAGgAAABpAAAAagBqAAEAAABsAG0AAQAAAG0AbQABAAAAbgBvAAEAAABvAG8AAQAAAHAAcAABAAAAcQBxAAEAAAByAHIAAQAAAHMAAAB0AGYAAQAAAHYAAAB3AHcAAQAAAHkAAAB6AAAAewAAAH8AfwABAAAAiAAAAJIAkgABAAAAkwAAAJgAAgAKAAAA" +
        "mQACAAoAAACaAAIACgAAAJsAAQABAAAAnAAMAAoAAACsAAIACgAAALIAsgABAAAAswCzAAEAAAC0ALQAAQAAALUAtQABAAAAtgC2AAEAAAC3ALcAAQAAALgADAAKAAAAuQAAALoAAAC7AAAAvAAAAL0AAAC+AAAAvwAAAMAAAADDAHAAAQAAAMQAcQABAAAAxQByAAEA" +
        "AADGAHcAAQAAAMcAAADQANAAAQAAANEAAADnAOcAAQAAAOgA5wABAAAA6QDpAAEAAADxAG0AAQAAAPUAAAD2AAAA+gAAAPsAAAD8APwAAQAAAgMAAAAA";

    /**
     * python-oracledb's own DataTypesMessage._process_message expects nothing but a
     * (data_type, conv_data_type[, 4 skipped bytes]) pair loop terminated by data_type==0,
     * starting immediately after the msgtype byte — no charset/ncharset/capability prefix.
     * An empty list (immediate 0x0000) is exactly what this server sent before the
     * ojdbc11-oriented verbatim-capture fix in DATA_TYPES_RESPONSE_B64 was introduced, and
     * per that constant's own history was "proven not to regress python-oracledb" — this
     * restores that exact minimal shape for python-oracledb specifically, confirmed live to
     * fix DPY-5002 (see pythonThinClient's javadoc).
     */
    /**
     * Real extended-tier DATA_TYPES response, replayed verbatim from the SAME live MITM capture
     * (real sqlplus &lt;-&gt; real Oracle Database 23ai) as AnoNegotiation's and
     * PROTOCOL_RESPONSE_EXTENDED_B64's captures. Byte-diffed against DATA_TYPES_RESPONSE_B64
     * below: the same underlying type-pair table, but with 14 extra header bytes between the
     * msgtype byte and the point where the two captures' content starts matching — their exact
     * semantics weren't resolved (same "replay rather than guess" reasoning as
     * PROTOCOL_RESPONSE_EXTENDED_B64's leading byte). Required: sending the legacy-shaped
     * DATA_TYPES_RESPONSE_B64 to a real OCI client at this tier made it abort the session with a
     * TNS MARKER packet immediately after receiving it (confirmed live) — real OCI clients parse
     * this response considerably more strictly than ojdbc11/python-oracledb do, the same lesson
     * already learned once for ConnectHandshake's ACCEPT.
     */
    private static final String DATA_TYPES_RESPONSE_EXTENDED_B64 =
        "AoAAAAA8PDyAAAAAAAAALQABAAEAAQAAAAIAAgAKAAAACAAIAAEAAAAMAAwACgAAABcAFwABAAAAGAAYAAEAAAAZABkAGAAAABoA" +
        "GgAZAAAAGwAbAAoAAAAcABwAFgAAAB0AHQAXAAAAHgAeABcAAAAfAB8AGQAAACAAIAAMAAAAIQAhAAwAAAAKAAoAAQAAAAsACwAB" +
        "AAAAKAAoAAEAAAApACkAAQAAAHUAdQABAAAAeAB4AAEAAAEiASIAAQAAASMBIwABAAABJAEkAAEAAAElASUAAQAAASYBJgABAAAB" +
        "KgEqAAEAAAErASsAAQAAASwBLAABAAABLQEtAAEAAAEuAS4AAQAAAS8BLwABAAABMQExAAEAAAEyATIAAQAAATMBMwABAAABNAE0" +
        "AAEAAAE1ATUAAQAAATYBNgABAAABNwE3AAEAAAE4ATgAAQAAATkBOQABAAABOwE7AAEAAAE8ATwAAQAAAT0BPQABAAABPgE+AAEA" +
        "AAE/AT8AAQAAAUABQAABAAABQQFBAAEAAAFCAUIAAQAAAUMBQwABAAABRwFHAAEAAAFIAUgAAQAAAUkBSQABAAABSwFLAAEAAAFN" +
        "AU0AAQAAAVMBUwABAAABVAFUAAEAAAFVAVUAAQAAAVYBVgABAAABVwFXAAEAAAFYAVgAAQAAAVkBWQABAAABWgFaAAEAAAFcAVwA" +
        "AQAAAV0BXQABAAABYgFiAAEAAAFjAWMAAQAAAWcBZwABAAABawFrAAEAAAF8AXwAAQAAAX0BfQABAAABfgF+AAEAAAGAAYAAAQAA" +
        "AYEBgQABAAABggGCAAEAAAGDAYMAAQAAAYQBhAABAAABhQGFAAEAAAGGAYYAAQAAAYcBhwABAAABiQGJAAEAAAGKAYoAAQAAAYsB" +
        "iwABAAABjAGMAAEAAAGNAY0AAQAAAY4BjgABAAABjwGPAAEAAAGQAZAAAQAAAZEBkQABAAABlAGUAAEAAAGVAZUAAQAAAZYBlgAB" +
        "AAABlwGXAAEAAAGdAZ0AAQAAAZ4BngABAAABnwGfAAEAAAGgAaAAAQAAAaEBoQABAAABogGiAAEAAAGjAaMAAQAAAaQBpAABAAAB" +
        "pQGlAAEAAAGmAaYAAQAAAacBpwABAAABqAGoAAEAAAGpAakAAQAAAaoBqgABAAABqwGrAAEAAAGtAa0AAQAAAa4BrgABAAABrwGv" +
        "AAEAAAGwAbAAAQAAAbEBsQABAAABwQHBAAEAAAHCAcIAAQAAAcYBxgABAAABxwHHAAEAAAHIAcgAAQAAAckByQABAAABygHKAAEA" +
        "AAHLAcsAAQAAAcwBzAABAAABzQHNAAEAAAHOAc4AAQAAAc8BzwABAAAB0gHSAAEAAAHTAdMAAQAAAdQB1AABAAAB1QHVAAEAAAHW" +
        "AdYAAQAAAdcB1wABAAAB2AHYAAEAAAHZAdkAAQAAAdoB2gABAAAB2wHbAAEAAAHcAdwAAQAAAd0B3QABAAAB3gHeAAEAAAHfAd8A" +
        "AQAAAeAB4AABAAAB4QHhAAEAAAHiAeIAAQAAAeMB4wABAAAB5AHkAAEAAAHlAeUAAQAAAeYB5gABAAAB6gHqAAEAAAHrAesAAQAA" +
        "AewB7AABAAAB7QHtAAEAAAHuAe4AAQAAAe8B7wABAAAB8AHwAAEAAAHyAfIAAQAAAfMB8wABAAAB9AH0AAEAAAH1AfUAAQAAAfYB" +
        "9gABAAAB/QH9AAEAAAH+Af4AAQAAAgECAQABAAACAgICAAEAAAIEAgQAAQAAAgUCBQABAAACBgIGAAEAAAIHAgcAAQAAAggCCAAB" +
        "AAACCQIJAAEAAAIKAgoAAQAAAgsCCwABAAACDAIMAAEAAAINAg0AAQAAAg4CDgABAAACDwIPAAEAAAIQAhAAAQAAAhECEQABAAAC" +
        "EgISAAEAAAITAhMAAQAAAhQCFAABAAACFQIVAAEAAAIWAhYAAQAAAhcCFwABAAACGAIYAAEAAAIZAhkAAQAAAhoCGgABAAACGwIb" +
        "AAEAAAIfAh8AAQAAAiACIAABAAACIQIhAAEAAAIiAiIAAQAAAiMCIwABAAACJAIkAAEAAAIlAiUAAQAAAiYCJgABAAACJwInAAEA" +
        "AAIoAigAAQAAAikCKQABAAACKgIqAAEAAAIrAisAAQAAAiwCLAABAAACLQItAAEAAAIuAi4AAQAAAi8CLwABAAACMAIwAAEAAAIx" +
        "AjEAAQAAAjICMgABAAACMwIzAAEAAAI0AjQAAQAAAjYCNgABAAACNwI3AAEAAAI4AjgAAQAAAjkCOQABAAACOgI6AAEAAAI7AjsA" +
        "AQAAAjwCPAABAAACPQI9AAEAAAI+Aj4AAQAAAj8CPwABAAACQAJAAAEAAAJBAkEAAQAAAkICQgABAAACQwJDAAEAAAJEAkQAAQAA" +
        "AkUCRQABAAACRgJGAAEAAAJHAkcAAQAAAkgCSAABAAACSQJJAAEAAAJKAkoAAQAAAksCSwABAAACTAJMAAEAAAJNAk0AAQAAAk4C" +
        "TgABAAACTwJPAAEAAAJQAlAAAQAAAlECUQABAAACUgJSAAEAAAJTAlMAAQAAAlQCVAABAAACVQJVAAEAAAJWAlYAAQAAAlcCVwAB" +
        "AAACWAJYAAEAAAJZAlkAAQAAAloCWgABAAACWwJbAAEAAAJcAlwAAQAAAl0CXQABAAACYwJjAAEAAAJkAmQAAQAAAmUCZQABAAAC" +
        "ZgJmAAEAAAJnAmcAAQAAAmgCaAABAAACaQJpAAEAAAJtAm0AAQAAAm4CbgABAAACbwJvAAEAAAJwAnAAAQAAAnECcQABAAACcgJy" +
        "AAEAAAJzAnMAAQAAAnQCdAABAAACdQJ1AAEAAAJ2AnYAAQAAAncCdwABAAACeAJ4AAEAAAJ5AnkAAQAAAnoCegABAAACewJ7AAEA" +
        "AAJ8AnwAAQAAAn0CfQABAAACfgJ+AAEAAAJ/An8AAQAAAoACgAABAAACgQKBAAEAAAKCAoIAAQAAAoMCgwABAAAChAKEAAEAAAKF" +
        "AoUAAQAAAoYChgABAAAChwKHAAEAAAKIAogAAQAAAokCiQABAAACigKKAAEAAAKLAosAAQAAAowCjAABAAACjQKNAAEAAAKPAo8A" +
        "AQAAApACkAABAAACkQKRAAEAAAKSApIAAQAAApMCkwABAAAClAKUAAEAAAKVApUAAQAAApYClgABAAAClwKXAAEAAAKYApgAAQAA" +
        "ApkCmQABAAACmgKaAAEAAAKcApwAAQAAAp0CnQABAAACngKeAAEAAAADAAIACgAAAAQAAgAKAAAABQABAAEAAAAGAAIACgAAAAcA" +
        "AgAKAAAACQABAAEAAAANAAAADgAAAA8AFwABAAAAEAAAABEAAAASAAAAEwAAABQAAAAVAAAAFgAAACcAAAA6ADoAAQAAAEQAAgAK" +
        "AAAARQAAAEYAAABKAG0AAQAAAEwAAABbAAIACgAAAF4AAQABAAAAXwAXAAEAAABgAGAAAQAAAGEAYAABAAAAZABkAAEAAABlAGUA" +
        "AQAAAGYAZgABAAAAaAAAAGkAAABqAGoAAQAAAGwAbQABAAAAbQBtAAEAAABuAG8AAQAAAG8AbwABAAAAcABwAAEAAABxAHEAAQAA" +
        "AHIAcgABAAAAcwAAAHQAZgABAAAAdgAAAHcAdwABAAAAeQB5AAEAAAB6AHoAAQAAAHsAewABAAAAfwB/AAEAAACIAAAAkgCSAAEA" +
        "AACTAJMAAQAAAJgAAgAKAAAAmQACAAoAAACaAAIACgAAAJsAAQABAAAAnAAMAAoAAACsAAIACgAAALIAsgABAAAAswCzAAEAAAC0" +
        "ALQAAQAAALUAtQABAAAAtgC2AAEAAAC3ALcAAQAAALgADAAKAAAAuQCyAAEAAAC6ALMAAQAAALsAtAABAAAAvAC1AAEAAAC9ALYA" +
        "AQAAAL4AtwABAAAAvwAAAMAAAADDAHAAAQAAAMQAcQABAAAAxQByAAEAAADGAHcAAQAAAMcAfwABAAAA0ADQAAEAAADRAAAA5wDn" +
        "AAEAAADoAOcAAQAAAOkA6QABAAAA8QBtAAEAAAD1APUAAQAAAPYA9gABAAAA+gAAAPsAAAD8APwAAQAAAgMCAwABAAAAAA==";

    private void sendDataTypesResponse(OutputStream out, boolean largeSdu) throws IOException {
        byte[] payload;
        if (pythonThinClient) {
            payload = new byte[] { (byte) MSG_TYPE_DATA_TYPES, 0, 0 };
        } else if (extendedProtocol) {
            payload = java.util.Base64.getDecoder().decode(DATA_TYPES_RESPONSE_EXTENDED_B64);
        } else {
            payload = java.util.Base64.getDecoder().decode(DATA_TYPES_RESPONSE_B64);
        }
        sendData(out, payload, largeSdu);
    }

    private void sendData(OutputStream out, byte[] payload, boolean largeSdu) throws IOException {
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, payload);
        out.write(packet.encode(largeSdu));
        out.flush();
    }
}

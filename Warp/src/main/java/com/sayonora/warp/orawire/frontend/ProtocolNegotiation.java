package com.sayonora.warp.orawire.frontend;

import com.sayonora.warp.orawire.frontend.auth.AuthConstants;
import com.sayonora.warp.orawire.ttc.TtcReader;
import com.sayonora.warp.orawire.ttc.TtcWriter;
import com.sayonora.warp.orawire.wireformat.TnsPacket;
import com.sayonora.warp.orawire.wireformat.TnsPacketReader;
import com.sayonora.warp.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;

public final class ProtocolNegotiation {

    private static final int MSG_TYPE_PROTOCOL = 1;
    private static final int MSG_TYPE_DATA_TYPES = 2;
    
    private static final int MSG_TYPE_PROTOCOL_EXTENDED =
        com.sayonora.warp.orawire.ttc.TtcConstants.MSG_TYPE_PROTOCOL_EXTENDED;

    private boolean legacyThinProtocolClient;

    private boolean extendedProtocol;

    // The raw data-types-request payload, kept so sendDataTypesResponse can tell which of two
    // real, distinct request shapes it's answering -- see that method's javadoc.
    private byte[] dataTypesRequestPayload;

    public void perform(TnsPacketReader reader, OutputStream out) throws IOException {
        readProtocolRequest(reader);
        sendProtocolResponse(out, reader.isLargeSdu());
        // Real bug this fixes, found live: for the EXTENDED protocol variant (real, native-OCI
        // a real, native-OCI client -- see ConnectHandshake's own comment on the version split), the
        // client's data-types/capability info travels INSIDE the PROTOCOL message itself (the
        // CharacterSet/Flags/CharacterSetGraph/FormatDescriptorObject/CompileTimeCapabilities/
        // RunTimeCapabilities fields readProtocolRequest already scans past -- see the reference
        // proserver/Record.rl TTIPRO grammar). There is no separate DATA_TYPES exchange AT ALL for
        // this variant -- neither a client request to read NOR a server response to send: a raw
        // TNS packet capture of a real Oracle server's response confirmed its phase-one auth
        // response begins at the exact byte immediately following its protocol response, with
        // zero bytes of any data-types content in between. Only the legacy (non-extended) flow
        // every legacy (non-native-OCI) client use has a real, separate DATA_TYPES round trip. This codebase
        // used to unconditionally read AND send one regardless -- the read blocked forever
        // waiting for a packet the client never sends (confirmed live via a Warp thread dump), and
        // even after skipping just the read, the extraneous 3KB send confused the real client into
        // aborting the connection with TNS MARKER (break/reset) packets instead of ever completing
        // its login.
        if (!extendedProtocol) {
            readDataTypesRequest(reader);
            sendDataTypesResponse(out, reader.isLargeSdu());
        }
    }

    private void readProtocolRequest(TnsPacketReader reader) throws IOException {
        TnsPacket packet = reader.readPacket();
        TtcReader r = new TtcReader(packet.payload());
        int msgType = r.readUint8();
        if (msgType == MSG_TYPE_PROTOCOL_EXTENDED) {
            extendedProtocol = true;
            r.skip(9);
        } else if (msgType == MSG_TYPE_PROTOCOL) {
            extendedProtocol = false;
            r.readUint8();
            r.readUint8();
        } else {
            throw new IOException("expected PROTOCOL message, got type " + msgType);
        }

        StringBuilder driverName = new StringBuilder();
        int b;
        while ((b = r.readUint8()) != 0) {
            driverName.append((char) b);
        }
        legacyThinProtocolClient = driverName.toString().toLowerCase(java.util.Locale.ROOT).contains("python-oracledb");

        // Real bug this fixes -- see TnsPacketReader's own javadoc for the full story: a real,
        // native-OCI non-dblink client packs its PROTOCOL message and further pipelined messages
        // (its own data-types request, and even the O5LOGON login call meant for a LATER stage)
        // into the SAME physical packet. Everything after the driver name belongs to someone else
        // -- push it back instead of silently discarding it (the bug that hung a real non-dblink native-OCI client).
        //
        // What comes right after the driver name for the EXTENDED protocol variant (real
        // native-OCI non-dblink, msgType 0x22) is a capability-negotiation structure this codebase
        // has never needed to fully understand (a CharacterSet/Flags/CharacterSetGraph/
        // FormatDescriptorObject/CompileTimeCapabilities/RunTimeCapabilities record, per the
        // reference proserver/Record.rl state machine -- but that grammar only documents the
        // legacy, non-extended shape, and a live byte capture confirmed the real extended shape
        // carries additional undocumented capability-vector content the legacy grammar doesn't
        // account for). Rather than guess at that undocumented layout, scan forward for the one
        // byte pattern we DO know unambiguously: {@code MSG_TYPE_FUNCTION, FUNC_AUTH_PHASE_ONE}
        // (3, 118) -- confirmed live to mark exactly where a pipelined O5LOGON call actually
        // starts, immediately followed by app_user1's own length-prefixed username. Everything
        // between the driver name and that marker is this message's own opaque trailing content;
        // we've never needed to interpret it, so treating it as a skippable blob (rather than the
        // old code's total discard of it, or a byte-exact parse we can't fully justify) is both
        // simpler and no less correct than what shipped before. Absent from a well-behaved
        // client's packet (every legacy (non-native-OCI) client never pipeline this early), so this changes
        // nothing for them -- the scan just finds nothing and pushBackUnconsumed is a no-op below.
        int payloadEnd = packet.payload().length;
        int pipelinedAuthCallStart = findFunctionCallMarker(packet.payload(), r.position(),
                AuthConstants.FUNC_AUTH_PHASE_ONE);
        int consumedLength = pipelinedAuthCallStart >= 0 ? pipelinedAuthCallStart : payloadEnd;
        reader.pushBackUnconsumed(packet.payload(), consumedLength);
    }

    /** Scans {@code payload[from..]} for the two-byte marker {@code MSG_TYPE_FUNCTION(3),
     * functionCode}, returning its start index or -1 if not found -- see the extensive comment at
     * this method's one call site for why this scan-based recovery exists instead of a full parse
     * of the extended PROTOCOL message's undocumented trailing capability structure. */
    private static int findFunctionCallMarker(byte[] payload, int from, int functionCode) {
        for (int i = from; i < payload.length - 1; i++) {
            if ((payload[i] & 0xFF) == com.sayonora.warp.orawire.ttc.TtcConstants.MSG_TYPE_FUNCTION
                    && (payload[i + 1] & 0xFF) == functionCode) {
                return i;
            }
        }
        return -1;
    }

    private static final String PROTOCOL_RESPONSE_B64 =
        "AQYATGludXgzOTB4L0xpbnV4LTIuNC54AGkDIQoAZgNAAwFAA2YDAWYDSAMBSANmAwFmA1IDAVIDZgMBZgNhAwFhA2YDAWYDHwMIHwNmAwEAZAAAAGABJA8FCwwDDAwFBAUNBgkHCAUFBQUFDwUFBQUFCgUFBQUFBAUGBwgII0cjRwgRIwgRQbBHAIMDaQfQAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA2BgEBAe8PARsBAQEBAQEBf/8DEAMDAQH/Af//AQ4BAf8BBgz2CX8FD/8NCwD/AwAAAAAABwICDQIBAAEYAH8BAgAAAAA=";

    // Was structurally wrong in two ways until this fix, both found live via a raw TNS packet
    // proxy in front of a real Oracle instance (see TnsPacketReader's own javadoc for the same
    // capture technique): (1) a spurious leading byte (0x1c) before the real msgType, shifting
    // every byte after it by one -- the same off-by-one shape that made the SHORT-extended
    // (legacy) response's driver name parse correctly by luck elsewhere in this file, but here
    // desynced this constant's entire content; (2) truncated by 17 bytes right after the
    // capabilities section, missing a byte mid-structure and 15 bytes of trailing content a real
    // server always sends. A real, native-OCI non-dblink client (which -- unlike every legacy,
    // non-native-OCI client -- actually negotiates the EXTENDED protocol variant, see ConnectHandshake's
    // own comment on why) detects the resulting corruption and aborts with TNS MARKER (break/
    // reset) packets instead of ever completing its login, confirmed live via this exact
    // capture-and-diff process. This is the real server's own response, byte-for-byte (driver
    // name/platform string included -- "Linux390x/Linux-2.4.x" is a fixed platform identifier
    // Oracle always reports here, not session-specific content).
    private static final String PROTOCOL_RESPONSE_EXTENDED_B64 =
        "AQgATGludXgzOTB4L0xpbnV4LTIuNC54AGkDowoAZgNAAwFAA2YDAWYDSAMBSANmAwFmA1IDAVIDZgMBZgNhAwFhA2YD"
            + "AWYDHwMIHwNmAwEAZAAAAGABJA8FCwwDDAwFBAUNBgkHCAUFBQUFDwUFBQUFCgUFBQUFBAUGBwgII0cjRwgRIwgR"
            + "QbBHAIMDaQfQAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA3BgEBAe8PARsBAQEBAQEBf/8D"
            + "EAMDAQH/Af//AQ4BAf8BBgz2CX8FD/8NCwD/AwAAAAAABwICBA0CAQABGAB/AQIAAAAAAQKAAAAAPDw8gAAAAAAA"
            + "ACs=";

    private void sendProtocolResponse(OutputStream out, boolean largeSdu) throws IOException {
        byte[] payload = java.util.Base64.getDecoder()
                .decode(extendedProtocol ? PROTOCOL_RESPONSE_EXTENDED_B64 : PROTOCOL_RESPONSE_B64);
        sendData(out, payload, largeSdu);
    }

    private void readDataTypesRequest(TnsPacketReader reader) throws IOException {
        TnsPacket packet = reader.readPacket();
        while (packet.payload().length == 0) {
            packet = reader.readFreshPacket();
        }
        TtcReader r = new TtcReader(packet.payload());
        int msgType = r.readUint8();
        if (msgType != MSG_TYPE_DATA_TYPES) {
            // Real bug this fixes -- see TnsPacketReader's own javadoc: a real, native-OCI
            // non-dblink native-OCI client can send its O5LOGON login call BEFORE its own data-types
            // negotiation request (both pipelined ahead of the normal protocol order). What we
            // just read isn't ours -- defer it for O5LogonHandler (the only later stage there is)
            // and read a genuinely fresh packet from the socket for our own message, instead of
            // either discarding this one or mistaking it for a malformed data-types request.
            reader.deferForLater(packet);
            packet = reader.readFreshPacket();
            while (packet.payload().length == 0) {
                packet = reader.readFreshPacket();
            }
            r = new TtcReader(packet.payload());
            msgType = r.readUint8();
            if (msgType != MSG_TYPE_DATA_TYPES) {
                throw new IOException("expected DATA_TYPES message, got type " + msgType);
            }
        }
        dataTypesRequestPayload = packet.payload();
    }

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

    // Length (payload bytes, including the leading msgType byte) of the compact data-types
    // request a real Oracle distributed-database-link connection sends -- confirmed live via
    // byte-for-byte capture against a real Oracle 23c instance (CREATE DATABASE LINK ... ; SELECT
    // ... FROM t@link), distinct from the longer request every legacy (non-native-OCI) client send. A real Oracle
    // server answers *this* request shape with a tiny ack that mirrors 15 bytes verbatim from the
    // client's own request (dropping only the request's final 2 bytes), not with the full
    // data-types table below -- sending the full table here (what this code used to do
    // unconditionally) desyncs the dblink client's protocol state machine, which reacts by
    // aborting the connection with TNS BREAK/RESET MARKER packets that this side then has no valid
    // response to. every legacy (non-native-OCI) client were tested working against the full-table response and are
    // deliberately left on that path -- this only carves out the one request shape confirmed to
    // need the short form.
    private static final int COMPACT_DATA_TYPES_REQUEST_LENGTH = 92;

    private void sendDataTypesResponse(OutputStream out, boolean largeSdu) throws IOException {
        byte[] payload;
        if (legacyThinProtocolClient) {
            payload = new byte[] { (byte) MSG_TYPE_DATA_TYPES, 0, 0 };
        } else if (dataTypesRequestPayload != null
                && dataTypesRequestPayload.length == COMPACT_DATA_TYPES_REQUEST_LENGTH) {
            payload = new byte[16];
            payload[0] = (byte) MSG_TYPE_DATA_TYPES;
            System.arraycopy(dataTypesRequestPayload, dataTypesRequestPayload.length - 17, payload, 1, 15);
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

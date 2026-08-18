package com.polygres.wire.orawire.frontend.auth;

import com.polygres.wire.auth.CredentialStore;
import com.polygres.wire.orawire.ttc.ResponseWriter;
import com.polygres.wire.orawire.ttc.TtcConstants;
import com.polygres.wire.orawire.ttc.TtcReader;
import com.polygres.wire.orawire.ttc.TtcWriter;
import com.polygres.wire.orawire.wireformat.TnsPacket;
import com.polygres.wire.orawire.wireformat.TnsPacketReader;
import com.polygres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;

/**
 * Server side of the O5LOGON 12c/PBKDF2 handshake, per
 * reference/o5logon_auth_spec.md §3. Two full request/response round trips
 * on the same connection: AUTH_PHASE_ONE (issue challenge) then
 * AUTH_PHASE_TWO (verify response), dispatched by function code (118/115)
 * read from each incoming DATA packet — not by any client-side "resend"
 * flag, which has no wire representation (spec §1.2).
 *
 * PROTOCOL/DATA_TYPES capability negotiation (see ProtocolNegotiation) now
 * replays a real server's own byte-exact capture, so ttc_field_version is
 * whatever that real session negotiated (>= 14 as of this capture) — see
 * ResponseWriter.writeO5LogonSuccessEnd's javadoc for the version-gated
 * trailing fields that follows from. Confirmed end-to-end against a real
 * ojdbc11 client completing full O5LOGON authentication successfully.
 */
public final class O5LogonHandler {

    private static final SecureRandom RANDOM = new SecureRandom();
    // Raw pre-encryption length. NOT 32: PKCS7 always appends a full extra
    // padding block when the plaintext is already block-aligned, so a raw
    // 32-byte value round-trips through encrypt+decrypt as 48 bytes — which
    // is exactly the length the real client uses to structurally detect the
    // legacy 11g scheme (auth.pyx:118, `len(session_key_part_a) == 48`),
    // confirmed by a live capture where this caused a real client to
    // silently take the 11g branch against our (12c-only) server. 16 pads
    // to 32, which is unambiguous and still satisfies the 12c path's
    // `[:32]` slice (spec §3.1.6).
    private static final int SESSION_KEY_HALF_LENGTH = 16;
    private static final int VFR_DATA_LENGTH = 16; // spec §5 item 1: length not established; project choice
    private static final int CSK_SALT_LENGTH = 16; // spec §5 item 2: length not established; project choice

    private final CredentialStore credentials = new CredentialStore();

    public AuthResult authenticate(TnsPacketReader reader, OutputStream out) throws IOException {
        boolean largeSdu = reader.isLargeSdu();
        // Same trigger AnoNegotiation/ProtocolNegotiation's extended path use — real OCI clients
        // (sqlplus, rwloadsim; protocol version >=320) marshal O5LOGON's function-call body in a
        // structurally different, fixed-width-field shape than ojdbc11/python-oracledb's
        // variable-length ub4/ub8-prefixed one. See readRichAuthHeader's javadoc.
        boolean richAuth = reader.isAnoEligible();
        TnsPacket phaseOnePacket = readNonEmptyPacket(reader);
        FunctionCall call1 = expectFunction(phaseOnePacket, AuthConstants.FUNC_AUTH_PHASE_ONE);
        String username = richAuth
                ? readUsernameAndSkipPairsRich(call1.reader())
                : readUsernameAndSkipPairs(call1.reader());

        byte[] password = credentials.lookupPassword(username);
        if (password == null) {
            sendRejection(out, largeSdu);
            return new AuthResult(username, false);
        }

        byte[] verifierData = randomBytes(VFR_DATA_LENGTH);
        byte[] pbkdf2Salt = concat(verifierData, "AUTH_PBKDF2_SPEEDY_KEY".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        byte[] passwordKey = OracleCrypto.pbkdf2HmacSha512(password, pbkdf2Salt, 64, AuthConstants.PBKDF2_VGEN_COUNT);
        byte[] passwordHash = sha512(concat(passwordKey, verifierData), 32);

        byte[] sessionKeyPartARaw = randomBytes(SESSION_KEY_HALF_LENGTH);
        byte[] authSesskey = OracleCrypto.encryptCbcPkcs7(passwordHash, sessionKeyPartARaw);
        // The client will independently derive this same value by decrypting
        // our authSesskey ciphertext — using it here too (rather than the raw
        // pre-padding bytes) guarantees byte-for-byte agreement, including the
        // real PKCS7 padding bytes the raw array doesn't have.
        byte[] sessionKeyPartA = OracleCrypto.decryptCbcNoUnpad(passwordHash, authSesskey);

        byte[] cskSalt = randomBytes(CSK_SALT_LENGTH);

        if (richAuth) {
            sendPhaseOneResponseRich(out, verifierData, authSesskey, cskSalt, largeSdu);
        } else {
            sendPhaseOneResponse(out, verifierData, authSesskey, cskSalt, call1.sequenceNumber(), largeSdu);
        }

        TnsPacket phaseTwoPacket = readNonEmptyPacket(reader);
        FunctionCall call2 = expectFunction(phaseTwoPacket, AuthConstants.FUNC_AUTH_PHASE_TWO);
        Map<String, String> pairs = richAuth
                ? readPhaseTwoPairsRich(call2.reader())
                : readPhaseTwoPairs(call2.reader());

        boolean success;
        try {
            success = verifyPhaseTwo(pairs, passwordHash, sessionKeyPartA, cskSalt, password);
        } catch (RuntimeException e) {
            success = false;
        }

        if (success) {
            byte[] comboKey = deriveComboKey(pairs, passwordHash, sessionKeyPartA, cskSalt);
            if (richAuth) {
                sendPhaseTwoSuccessRich(out, comboKey, largeSdu);
            } else {
                sendPhaseTwoSuccess(out, comboKey, call2.sequenceNumber(), largeSdu);
            }
        } else {
            sendRejection(out, largeSdu);
        }
        return new AuthResult(username, success);
    }

    private boolean verifyPhaseTwo(Map<String, String> pairs, byte[] passwordHash, byte[] sessionKeyPartA,
            byte[] cskSalt, byte[] expectedPassword) {
        byte[] comboKey = deriveComboKey(pairs, passwordHash, sessionKeyPartA, cskSalt);

        byte[] authPasswordCipher = HexFormat.of().parseHex(pairs.get("AUTH_PASSWORD"));
        byte[] decrypted = OracleCrypto.stripPkcs7(OracleCrypto.decryptCbcNoUnpad(comboKey, authPasswordCipher));
        // spec §2.3: 16-byte random salt || plaintext password
        byte[] claimedPassword = Arrays.copyOfRange(decrypted, 16, decrypted.length);
        return Arrays.equals(claimedPassword, expectedPassword);
    }

    private byte[] deriveComboKey(Map<String, String> pairs, byte[] passwordHash, byte[] sessionKeyPartA,
            byte[] cskSalt) {
        byte[] authSesskeyClientCipher = HexFormat.of().parseHex(pairs.get("AUTH_SESSKEY"));
        byte[] sessionKeyPartB = OracleCrypto.decryptCbcNoUnpad(passwordHash, authSesskeyClientCipher);
        // spec §3.2.2: temp_key = part_b[:32] + part_a[:32], hex-uppercased, fed as PBKDF2 password
        byte[] tempKey = concat(
                Arrays.copyOf(sessionKeyPartB, 32),
                Arrays.copyOf(sessionKeyPartA, 32));
        byte[] tempKeyHex = HexFormat.of().withUpperCase().formatHex(tempKey)
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return OracleCrypto.pbkdf2HmacSha512(tempKeyHex, cskSalt, 32, AuthConstants.PBKDF2_SDER_COUNT);
    }

    private String readUsernameAndSkipPairs(TtcReader r) {
        // Extra leading byte (value 0 in every capture seen so far) present ONLY once this
        // server started advertising a real, richer PROTOCOL/DATA_TYPES capability set (see
        // ProtocolNegotiation) — before that fix, the client's AUTH_PHASE_ONE request started
        // directly with hasUser and this server worked fine without skipping anything here. A
        // live capture after the capability fix showed a request whose fields (userLen=5 for
        // "ORAPG", numPairs=5 matching AUTH_TERMINAL/AUTH_PROGRAM_NM/AUTH_MACHINE/AUTH_PID/
        // AUTH_SID) only line up correctly if exactly one extra byte is consumed first — this
        // server's own richer capability advertisement evidently makes the client include one
        // more leading field in its own request, symmetric with how much more we now offer it.
        // IDENTIFIED: this is token_num (UB8), per Message._write_function_code in
        // python-oracledb's own source (base.pyx) — every function-call header carries it once
        // ttc_field_version >= 23.1_ext1, which this server's real PROTOCOL capability array
        // now clears (see ProtocolNegotiation and RequestLoop.handleData's matching fix).
        // read with readUb8() (proper variable-length decode), not a fixed single-byte skip —
        // this only worked before because token_num was 0 (a single 0x00 byte) in every
        // capture seen so far.
        r.readUb8();
        int hasUser = r.readUint8();
        // NOT "redundant with an inner length prefix" — found live against the real Java thin
        // driver: decompiling T4CTTIoauthenticate.marshal()/T4CMAREngine.marshalCHR (ojdbc11.jar)
        // shows the username is written as plain raw bytes with no length prefix of its own; this
        // ub4 is its one and only length. Trusting the old "redundant" assumption and re-reading a
        // length prefix from the username bytes themselves misread arbitrary username-content
        // bytes as a length, corrupting every field after it.
        long userLen = r.readUb4();
        r.readUb4(); // auth_mode
        r.readUint8(); // authivl pointer
        long numPairs = r.readUb4();
        r.readUint8(); // authovl pointer
        r.readUint8(); // authovln pointer
        String username = null;
        if (hasUser != 0) {
            byte[] userBytes = r.readRawOrLengthPrefixedBytes((int) userLen);
            username = new String(userBytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        AuthKv.skipPairs(r, (int) numPairs);
        return username;
    }

    /**
     * Real OCI clients' (sqlplus, rwloadsim; protocol version &gt;=320, same tier as
     * AnoNegotiation/ProtocolNegotiation's extended path) O5LOGON function-call body — confirmed
     * live via MITM capture (real sqlplus &lt;-&gt; real Oracle Database 23ai) to be structurally
     * different from readUsernameAndSkipPairs' shape: fixed-width fields throughout rather than
     * the thin-client's self-describing variable-length ub4/ub8 count-prefix encoding (several
     * observed "length"-looking bytes here — 0xfe, 0xff — would be nonsensical ub4/ub8 byte
     * counts, ruling that encoding out for this shape).
     *
     * <p>Byte-for-byte identical header layout was confirmed in both AUTH_PHASE_ONE and
     * AUTH_PHASE_TWO captures (same offsets for hasUser, the numPairs field, and the username),
     * so both {@link #readUsernameAndSkipPairsRich} and {@link #readPhaseTwoPairsRich} share this
     * method. Several fields between hasUser and numPairs, and between numPairs and the username,
     * are skipped rather than interpreted — their semantics weren't resolved from a single
     * capture (most look like fixed 8-byte native-OCI pointer/handle sentinels, always
     * {@code 0xFEFFFFFFFFFFFFFF} in every capture seen so far) and nothing downstream needs them,
     * consistent with this package's established "byte-exact replay/skip over guessing" precedent
     * for the type-34/extended-DATA_TYPES gaps found earlier in this same investigation.
     */
    private record RichAuthHeader(int hasUser, int numPairs) {
    }

    private RichAuthHeader readRichAuthHeader(TtcReader r) {
        int hasUser = r.readUint8();
        r.skip(25); // fixed sentinel/pointer fields — semantics not resolved, see class javadoc
        int numPairs = (int) readLe32(r);
        r.skip(16); // more fixed sentinel fields between numPairs and the username length byte
        return new RichAuthHeader(hasUser, numPairs);
    }

    private String readRichUsername(TtcReader r, int hasUser) {
        if (hasUser == 0) {
            return null;
        }
        int userLen = r.readUint8(); // direct single-byte length, no outer wrapper — unlike the
                                      // per-pair key/value fields below, which do have one
        return new String(r.readRawBytes(userLen), java.nio.charset.StandardCharsets.UTF_8);
    }

    /**
     * Per-pair shape confirmed live: {@code [4-byte LE outerLen][1-byte innerLen][key bytes]
     * [4-byte LE valueOuterLen, 0 if absent][1-byte innerLen + value bytes if valueOuterLen != 0]
     * [4-byte LE flags]}. outerLen was observed to consistently equal 3x the following field's
     * actual byte length in every capture (e.g. a 13-byte key's outerLen was 39) — plausibly an
     * AL32UTF8 worst-case buffer-size hint some OCI marshaling layer writes, but that's inference,
     * not a confirmed semantic, so it's read and discarded here rather than validated against.
     */
    private Map<String, String> readRichPairs(TtcReader r, int numPairs) {
        Map<String, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < numPairs; i++) {
            readLe32(r); // key outerLen — discarded, see javadoc
            int keyLen = r.readUint8();
            String key = new String(r.readRawBytes(keyLen), java.nio.charset.StandardCharsets.UTF_8);
            long valueOuterLen = readLe32(r);
            String value = null;
            if (valueOuterLen != 0) {
                int valueLen = r.readUint8();
                value = new String(r.readRawBytes(valueLen), java.nio.charset.StandardCharsets.UTF_8);
            }
            readLe32(r); // flags — discarded, meaning not established (same as the legacy path)
            map.put(key, value);
        }
        return map;
    }

    private static long readLe32(TtcReader r) {
        long b0 = r.readUint8();
        long b1 = r.readUint8();
        long b2 = r.readUint8();
        long b3 = r.readUint8();
        return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24);
    }

    private String readUsernameAndSkipPairsRich(TtcReader r) {
        RichAuthHeader header = readRichAuthHeader(r);
        String username = readRichUsername(r, header.hasUser());
        readRichPairs(r, header.numPairs()); // phase-one's pairs (AUTH_TERMINAL etc.) — unused server-side
        return username;
    }

    private Map<String, String> readPhaseTwoPairsRich(TtcReader r) {
        RichAuthHeader header = readRichAuthHeader(r);
        readRichUsername(r, header.hasUser()); // present on phase two as well as phase one, unused here
        return readRichPairs(r, header.numPairs());
    }

    private Map<String, String> readPhaseTwoPairs(TtcReader r) {
        r.readUb8(); // token_num — see readUsernameAndSkipPairs's javadoc
        int hasUser = r.readUint8();
        long userLen = r.readUb4(); // see readUsernameAndSkipPairs — this is the only length, no inner prefix
        r.readUb4();   // auth_mode
        r.readUint8(); // authivl pointer
        long numPairs = r.readUb4();
        r.readUint8(); // authovl pointer
        r.readUint8(); // authovln pointer
        if (hasUser != 0) {
            r.readRawOrLengthPrefixedBytes((int) userLen); // username, present on phase two as well as phase one
        }
        return AuthKv.readPairs(r, (int) numPairs);
    }

    /**
     * The real Java thin driver (ojdbc11, found live — python-oracledb doesn't do this) can send a
     * zero-payload DATA packet ahead of either O5LOGON round trip's real function-call message,
     * same pattern already found and fixed for the PROTOCOL/DATA_TYPES exchange
     * (see {@link com.polygres.wire.orawire.frontend.ProtocolNegotiation}). Skip empties rather than
     * treating one as a malformed message.
     */
    private static TnsPacket readNonEmptyPacket(TnsPacketReader reader) throws IOException {
        TnsPacket packet = reader.readPacket();
        while (packet.payload().length == 0) {
            packet = reader.readPacket();
        }
        return packet;
    }

    private record FunctionCall(TtcReader reader, int sequenceNumber) {
    }

    /**
     * Also returns the client's own per-call sequence number (previously read and discarded) —
     * needed to build a real success terminator: found live via decompiling {@code
     * T4CTTIoer11.unmarshalAttributes()}/{@code T4CTTIfun.receive()} that the client's OER-unit
     * parser records this call's sequence number as {@code callNumber} and later code paths
     * compare it back; a terminator literally byte-copied from a *different* real session embeds
     * that other session's callNumber, not this one's.
     */
    private FunctionCall expectFunction(TnsPacket packet, int expectedFunctionCode) throws IOException {
        if (packet.type() != TnsPacketType.DATA) {
            throw new IOException("expected DATA packet during auth, got " + packet.type());
        }
        TtcReader r = new TtcReader(packet.payload());
        int messageType = r.readUint8();
        if (messageType != TtcConstants.MSG_TYPE_FUNCTION) {
            throw new IOException("expected function-call message during auth, got type " + messageType);
        }
        int functionCode = r.readUint8();
        int sequenceNumber = r.readUint8();
        if (functionCode != expectedFunctionCode) {
            throw new IOException("expected auth function code " + expectedFunctionCode + ", got " + functionCode);
        }
        return new FunctionCall(r, sequenceNumber);
    }

    /**
     * Real OCI clients' (sqlplus, rwloadsim; same {@code richAuth} tier as
     * readUsernameAndSkipPairsRich) PARAMETER-message shape for the O5LOGON phase-one response —
     * confirmed live via MITM capture (real sqlplus &lt;-&gt; real Oracle Database 23ai) to differ
     * from sendPhaseOneResponse's shape in two ways: (1) the pair count is a direct single byte,
     * not {@link com.polygres.wire.orawire.ttc.TtcWriter#writeUb2}'s count-prefixed form; (2) each
     * pair's key/value/flags use fixed-width fields empirically confirmed against six real pairs:
     * key = [2-byte BE length][4-byte BE length, same value repeated][key bytes]; value = [1-byte
     * BE length][4-byte BE length, same value repeated][value bytes] (omitted entirely, no
     * trailing 4-byte piece either, when the value is absent — not observed in this capture, since
     * all 6 real pairs had non-empty values, so that case is inferred from the legacy AuthKv
     * encoding's analogous null-shortcut, not independently confirmed); flags = 3-byte
     * <b>little-endian</b> integer (confirmed via AUTH_VFR_DATA's real flags bytes {@code 15 48
     * 00}, which decode as LE {@link AuthConstants#VERIFIER_TYPE_12C} (0x4815) — every other
     * pair's flags were 0, ambiguous as to endianness on their own).
     *
     * <p>The trailing terminator (after all 6 pairs) is replayed byte-exact from the same capture
     * — its field semantics weren't resolved (same "replay rather than guess" reasoning already
     * used for {@code PROTOCOL_RESPONSE_EXTENDED_B64} and {@code DATA_TYPES_RESPONSE_EXTENDED_B64}
     * — this project's established precedent for gaps found this deep into a single investigation
     * session), including whatever per-call sequence-number field the legacy path's
     * {@code O5LOGON_TERMINATOR_CALLNUMBER_OFFSET} patches — unpatched here, since this capture's
     * own client accepted it as-is and there was no independent way to confirm a patch is even
     * needed at this tier without risking the same kind of regression the legacy path's callNumber
     * investigation already went through once.
     */
    private static final String PHASE_ONE_TERMINATOR_RICH_B64 =
        "AAQBAAAAdgUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            + "AAAAAAAAAAAd";

    private void sendPhaseOneResponseRich(OutputStream out, byte[] verifierData, byte[] authSesskey, byte[] cskSalt,
            boolean largeSdu) throws IOException {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        buf.write(TtcConstants.MSG_TYPE_PARAMETER);
        buf.write(6); // pair count — direct single byte at this tier, not ub2
        writeRichPair(buf, "AUTH_SESSKEY", hex(authSesskey), 0);
        writeRichPair(buf, "AUTH_VFR_DATA", hex(verifierData), AuthConstants.VERIFIER_TYPE_12C);
        writeRichPair(buf, "AUTH_PBKDF2_CSK_SALT", hex(cskSalt), 0);
        writeRichPair(buf, "AUTH_PBKDF2_VGEN_COUNT", String.valueOf(AuthConstants.PBKDF2_VGEN_COUNT), 0);
        writeRichPair(buf, "AUTH_PBKDF2_SDER_COUNT", String.valueOf(AuthConstants.PBKDF2_SDER_COUNT), 0);
        // Same real-32-hex-char-value requirement as sendPhaseOneResponse's own
        // AUTH_GLOBALLY_UNIQUE_DBID — see that method's javadoc; the trailing NUL byte here is
        // part of the KEY text itself (confirmed live: the real key is 26 bytes, one more than
        // "AUTH_GLOBALLY_UNIQUE_DBID".length()), not the space character the legacy path uses —
        // a real, independently-confirmed difference from the legacy shape, not a typo.
        //
        // CORRECTED (was a wrong assumption): this is NOT session-random data — confirmed live by
        // capturing it independently across three separate real sessions against the same backing
        // database and finding the byte-exact same value every time ({@link
        // #RICH_TIER_DATABASE_GUID_HEX}). It's the PDB's own real, fixed GUID. The original
        // "replaying one real database's UUID into every deployment would be actively wrong"
        // reasoning was about not hardcoding *this specific* value long-term, not about whether
        // the field needs to be *stable* — sending fresh random bytes on every single connection
        // is likely worse: if the real OCI client validates internal consistency involving this
        // value (not yet confirmed which check, if any), random garbage would fail it every time,
        // where at least a real, well-formed, stable value has a chance of passing. Kept as this
        // environment's actual captured value for now; a real per-deployment fix would generate
        // one random GUID at server startup and hold it for the process lifetime rather than
        // per-connection — not yet done, since it isn't confirmed this field is what's blocking
        // the client at all (see ARCHITECTURE.md §5.5g's running investigation).
        writeRichPair(buf, "AUTH_GLOBALLY_UNIQUE_DBID\0", RICH_TIER_DATABASE_GUID_HEX, 0);
        byte[] terminator = java.util.Base64.getDecoder().decode(PHASE_ONE_TERMINATOR_RICH_B64);
        // Same class of per-session-varying field found in the phase-two terminator (see
        // PHASE_TWO_RICH_CALLNUMBER_OFFSET's javadoc) — confirmed by diffing two independently
        // captured real phase-one responses: bytes at this offset within the terminator (0x44 0x06
        // vs 0x04 0x0A in the two captures) were the ONLY difference outside the two crypto-value
        // regions already patched above. Same "don't replay one session's value" randomization.
        byte[] terminatorVaryingBytes = randomBytes(PHASE_ONE_TERMINATOR_VARYING_LENGTH);
        System.arraycopy(terminatorVaryingBytes, 0, terminator, PHASE_ONE_TERMINATOR_VARYING_OFFSET,
                terminatorVaryingBytes.length);
        buf.write(terminator);
        sendData(out, buf.toByteArray(), largeSdu);
    }

    private static final int PHASE_ONE_TERMINATOR_VARYING_OFFSET = 6;
    private static final int PHASE_ONE_TERMINATOR_VARYING_LENGTH = 2;

    /** See the "CORRECTED" comment on this field's call site — captured live, byte-identical
     * across three independent real sessions against the same backing database. */
    private static final String RICH_TIER_DATABASE_GUID_HEX = "7633D8148E2E259AE5679C2AA50E96A5";

    private static void writeRichPair(java.io.ByteArrayOutputStream buf, String key, String value, long flags) {
        // Confirmed live: KEY uses a 2-byte-then-4-byte outer/inner length pair; VALUE uses a
        // narrower 1-byte-then-4-byte pair — genuinely different widths, not a copy/paste
        // opportunity (an earlier version of this code used the key's wider encoding for the
        // value too, which desynced the real client's parser one byte per pair).
        writeRichLengthPrefixedString(buf, key, 2);
        writeRichLengthPrefixedString(buf, value, 1);
        writeLe(buf, flags, 3);
    }

    /** key/value field shape confirmed live — see sendPhaseOneResponseRich's javadoc. */
    private static void writeRichLengthPrefixedString(java.io.ByteArrayOutputStream buf, String s, int outerWidth) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int len = bytes.length;
        writeBe(buf, len, outerWidth);
        writeBe(buf, len, 4);
        buf.write(bytes, 0, bytes.length);
    }

    /** Length-prefix fields (writeRichLengthPrefixedString) are big-endian — confirmed live. */
    private static void writeBe(java.io.ByteArrayOutputStream buf, long value, int width) {
        for (int i = width - 1; i >= 0; i--) {
            buf.write((int) ((value >> (8 * i)) & 0xFF));
        }
    }

    /** The flags/verifier-type field (writeRichPair) is little-endian — confirmed live via
     * AUTH_VFR_DATA's real flags bytes {@code 15 48 00} decoding as LE 0x4815 (VERIFIER_TYPE_12C). */
    private static void writeLe(java.io.ByteArrayOutputStream buf, long value, int width) {
        for (int i = 0; i < width; i++) {
            buf.write((int) ((value >> (8 * i)) & 0xFF));
        }
    }

    private void sendPhaseOneResponse(OutputStream out, byte[] verifierData, byte[] authSesskey, byte[] cskSalt,
            int sequenceNumber, boolean largeSdu) throws IOException {
        // Pair count, order, and the AUTH_GLOBALLY_UNIQUE_DBID pair below are not the original
        // 5-pair guess from o5logon_auth_spec.md — found live by proxying a real ojdbc11 client
        // straight through to a real Oracle instance and capturing its genuine phase-one response
        // byte-for-byte: the real server sends 6 pairs, in this exact order, with
        // AUTH_GLOBALLY_UNIQUE_DBID present but empty. Matching that shape exactly rather than the
        // spec's guess is what gets a real Java driver client past this response without throwing
        // ORA-17401 protocol violation.
        TtcWriter w = new TtcWriter();
        w.writeUint8(TtcConstants.MSG_TYPE_PARAMETER);
        w.writeUb2(6);
        AuthKv.writePair(w, "AUTH_SESSKEY", hex(authSesskey), 0);
        writePairWithVerifierType(w, "AUTH_VFR_DATA", hex(verifierData), AuthConstants.VERIFIER_TYPE_12C);
        AuthKv.writePair(w, "AUTH_PBKDF2_CSK_SALT", hex(cskSalt), 0);
        AuthKv.writePair(w, "AUTH_PBKDF2_VGEN_COUNT", String.valueOf(AuthConstants.PBKDF2_VGEN_COUNT), 0);
        AuthKv.writePair(w, "AUTH_PBKDF2_SDER_COUNT", String.valueOf(AuthConstants.PBKDF2_SDER_COUNT), 0);
        // NOT the plain 25-char string — found live, exact-byte-bounded re-diff of a real captured
        // response: the real server encodes this key's length as 26, one more than
        // "AUTH_GLOBALLY_UNIQUE_DBID".length(), with a literal trailing NUL byte as part of the key
        // itself (apparently a C-string artifact leaking server-side, not a client requirement we
        // get to choose) — matched exactly here rather than guessed. Two real bugs fixed here
        // against a fresh live capture of the `orawire` repo's own server (finally reproducible
        // end to end — see docs/option-b-replatform-plan.md) completing a real O5LOGON exchange
        // with the actual ojdbc11 driver: (1) the "trailing NUL byte" the comment above already
        // described was actually written as a space (0x20) — `"...DBID "` is a Java string literal
        // with a trailing space character, not a NUL; (2) far more consequentially, this key's
        // *value* is NOT empty on a real server — it's a real 32-hex-char value, not `null`. The
        // capture showed PolyWire's response was exactly 34 bytes shorter than the real one
        // (334 vs 368), and 34 bytes is exactly the size of a present `01 20`-length-prefixed
        // 32-byte hex value vs. an absent one — this was the entire discrepancy, not one of
        // several. The exact value shouldn't matter to the client (it's an opaque DB identifier,
        // not something crypto-checked), so a random 16-byte value hex-encoded the same way
        // verifierData/cskSalt already are is used rather than a fixed placeholder.
        AuthKv.writeString(w, "AUTH_GLOBALLY_UNIQUE_DBID\0");
        AuthKv.writeString(w, hex(randomBytes(16)));
        w.writeUb4(0);
        // ACTUALLY FIXED NOW (an earlier version of this comment described this exact fix but the
        // code below it still passed the call's own sequenceNumber — a stale comment/code split
        // from an earlier revert that was never corrected). Root-caused via decompiling
        // T4CTTIfun.receive()'s code=4 (OER/terminator) case: it compares this terminator's
        // callNumber field against T4CTTIfun's own internal sequenceNumber counter (a
        // driver-side per-connection RPC counter, NOT the wire "sequenceNumber" byte the client
        // put in its own function-call header) and routes to handleOutOfSequenceError() instead
        // of processError() on any mismatch — which silently defers processing rather than
        // throwing immediately, leaving the driver blocked on a fresh socket read waiting for a
        // response it thinks hasn't arrived yet (confirmed live: real ojdbc11 client hangs
        // forever in NIOPacket.readHeader's blocking read after phase-one, per a jstack of the
        // hung client thread). A fresh live capture against a real Oracle Database 23 Free
        // instance confirms this field is 0 for phase-one's terminator regardless of the
        // client's own wire sequenceNumber (1 in that capture, same as ours) — hardcoded to 0
        // rather than echoing the call's sequenceNumber.
        ResponseWriter.writeO5LogonSuccessEnd(w, 0, 0);
        sendData(out, w.toByteArray(), largeSdu);
    }

    /**
     * Real, complete phase-two success response, replayed verbatim from a live packet
     * capture of a real Oracle Database 23 Free instance (same session/technique as
     * ProtocolNegotiation's PROTOCOL_RESPONSE_B64/DATA_TYPES_RESPONSE_B64). Supersedes an
     * earlier 4-pair hand-built version (AUTH_SVR_RESPONSE/AUTH_SESSION_ID/AUTH_SERIAL_NUM/
     * AUTH_VERSION_NO only) that structurally worked well enough to get phase one accepted
     * and authentication to succeed server-side (confirmed live: the connection reached
     * RequestLoop, past all of O5LOGON) but was rejected client-side with the same class of
     * ORA-17401 ("[ 0, ]") seen throughout this investigation — the real response carries
     * 50 KV pairs (session/instance identity, NLS settings, timezone, capability echoes,
     * etc.) plus a large additional binary block this server never sent any equivalent of.
     * Reconstructing that block's semantics field-by-field wasn't tractable in the time
     * available; byte-exact replay sidesteps needing to.
     *
     * Only AUTH_SVR_RESPONSE's value is patched at runtime, at its fixed byte offset — it's
     * the one field that's genuinely per-session (an AES-CBC/PKCS7 encryption under this
     * call's own comboKey), unlike the rest of this template which the client doesn't appear
     * to validate against session-specific expectations.
     */
    private static final int PHASE_TWO_TEMPLATE_SVR_RESPONSE_OFFSET = 1635;
    private static final int PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH = 96; // hex-encoded 48-byte ciphertext, as ASCII
    // Same callNumber field ResponseWriter.O5LOGON_TERMINATOR_CALLNUMBER_OFFSET patches for
    // phase-one's terminator exists here too, at the same relative offset (25) within this
    // template's own trailing "04 01 01 02 04 e1 ..." terminator tail. Tried live, this session:
    // patching it to this call's real sequenceNumber (as phase-one's analogous fix does)
    // regressed python-oracledb (hung post-auth) without fixing ojdbc11 — reverted, see
    // sendPhaseTwoSuccess. Kept as a documented, ruled-out candidate rather than deleted.
    private static final int PHASE_TWO_TEMPLATE_CALLNUMBER_OFFSET = 2240;
    private static final String PHASE_TWO_RESPONSE_B64 =
        "CAEyARMTQVVUSF9WRVJTSU9OX1NUUklORwEiIi0gRGV2ZWxvcCwgTGVhcm4sIGFuZCBSdW4gZm9yIEZyZWUAARAQQVVUSF9WRVJTSU9OX1NRTAECAjI2AAETE0FVVEhfWEFDVElPTl9UUkFJVFMBAQEzAAEPD0FVVEhfVkVSU0lPTl9OTwEJCTM4NzU4ODA5NgABExNB" +
        "VVRIX1ZFUlNJT05fU1RBVFVTAQEBMAABFRVBVVRIX0NBUEFCSUxJVFlfVEFCTEUAAAEPD0FVVEhfTEFTVF9MT0dJTgEaGkZGNjQwMDAwMDAwMDAwMDAwMDAwMDAwMDAwAAELC0FVVEhfREJOQU1FAQgIRlJFRVBEQjEAARERQVVUSF9EQl9NT1VOVF9JRAABCgoxNTEy" +
        "NTU2MDkyAAELC0FVVEhfREJfSUQAAQoKMjk1Mjc3NDM5NAABDAxBVVRIX1VTRVJfSUQBAwMxMzgAAQ8PQVVUSF9TRVNTSU9OX0lEAQMDMTc5AAEPD0FVVEhfU0VSSUFMX05VTQEFBTUzODk0AAEQEEFVVEhfSU5TVEFOQ0VfTk8BAQExAAEQEEFVVEhfRkFJTE9WRVJf" +
        "SUQBAQExAAEPD0FVVEhfU0VSVkVSX1BJRAEFBTY4NDc4AAETE0FVVEhfU0NfU0VSVkVSX0hPU1QBDAxkNzZmZmRlYjViMGEAARUVQVVUSF9TQ19EQlVOSVFVRV9OQU1FAQQERlJFRQABFRVBVVRIX1NDX0lOU1RBTkNFX05BTUUBBARGUkVFAAETE0FVVEhfU0NfSU5T" +
        "VEFOQ0VfSUQBAQExAAEbG0FVVEhfU0NfSU5TVEFOQ0VfU1RBUlRfVElNRQEkJDIwMjYtMDgtMDUgMTY6NDQ6NDIuMDAwMDAwMDAwIC0wNzowMAABERFBVVRIX1NDX0RCX0RPTUFJTgAAARQUQVVUSF9TQ19TRVJWSUNFX05BTUUBCAhmcmVlcGRiMQABGxtBVVRIX09O" +
        "U19STEJfU1VCU0NSX1BBVFRFUk4BNDQlImV2ZW50VHlwZT1kYXRhYmFzZS9ldmVudC9zZXJ2aWNlbWV0cmljcy9mcmVlcGRiMSIAAAEaGkFVVEhfT05TX0hBX1NVQlNDUl9QQVRURVJOAUlJKCJldmVudFR5cGU9ZGF0YWJhc2UvZXZlbnQvc2VydmljZSIpIHwgKCJl" +
        "dmVudFR5cGU9ZGF0YWJhc2UvZXZlbnQvaG9zdCIpAAABGhpBVVRIX1NDX1JFQUxfREJVTklRVUVfTkFNRQEEBEZSRUUAARERQVVUSF9JTlNUQU5DRU5BTUUBBARGUkVFAAEPD0FVVEhfTkxTX0xYTEFOAAEICEFNRVJJQ0FOAAEWFkFVVEhfTkxTX0xYQ1RFUlJJVE9S" +
        "WQABBwdBTUVSSUNBAAEVFUFVVEhfTkxTX0xYQ0NVUlJFTkNZAAEBASQAARQUQVVUSF9OTFNfTFhDSVNPQ1VSUgABBwdBTUVSSUNBAAEVFUFVVEhfTkxTX0xYQ05VTUVSSUNTAAECAi4sAAETE0FVVEhfTkxTX0xYQ0RBVEVGTQABCQlERC1NT04tUlIAARUVQVVUSF9O" +
        "TFNfTFhDREFURUxBTkcAAQgIQU1FUklDQU4AARERQVVUSF9OTFNfTFhDU09SVAABBgZCSU5BUlkAARUVQVVUSF9OTFNfTFhDQ0FMRU5EQVIAAQkJR1JFR09SSUFOAAEVFUFVVEhfTkxTX0xYQ1VOSU9OQ1VSAAEBASQAARMTQVVUSF9OTFNfTFhDVElNRUZNAAEODkhI" +
        "Lk1JLlNTWEZGIEFNAAETE0FVVEhfTkxTX0xYQ1NUTVBGTQABGBhERC1NT04tUlIgSEguTUkuU1NYRkYgQU0AARMTQVVUSF9OTFNfTFhDVFRaTkZNAAESEkhILk1JLlNTWEZGIEFNIFRaUgABExNBVVRIX05MU19MWENTVFpORk0AARwcREQtTU9OLVJSIEhILk1JLlNT" +
        "WEZGIEFNIFRaUgABGBhBVVRIX05MU19MWExFTlNFTUFOVElDUwABBARCWVRFAAEZGUFVVEhfTkxTX0xYTkNIQVJDT05WRVhDUAABBQVGQUxTRQABEBBBVVRIX05MU19MWENPTVAAAQYGQklOQVJZAAEREUFVVEhfU1ZSX1JFU1BPTlNFAWBgNjFEOTVGOTlFOTk4NjVC" +
        "RDExODU4RjhDNzcyNENDM0NCNUJGNkM3Q0M1MTJGMzQwODM5RjhEMTU4MEFENjlDREY5NzUzQjYwMDk4MzUwOEFDRDJCOUJDRUZCRTM1Q0UyAAEVFUFVVEhfTUFYX09QRU5fQ1VSU09SUwEDAzMwMAABDQ1BVVRIX1BEQl9VSUQAAQoKMjk1Mjc3NDM5NAABFBRBVVRI" +
        "X01BWF9JREVOX0xFTkdUSAEDAzEyOAABCgpBVVRIX0ZMQUdTAQEBMQABEBBBVVRIX1NFUlZFUl9UWVBFAQEBMQAXBQEBEAEVFgABCAhBTUVSSUNBTgEQAAEHB0FNRVJJQ0EBCQABAQEkAAABBwdBTUVSSUNBAQEAAQICLiwBAgABCAhBTDMyVVRGOAEKAAEJCUdSRUdP" +
        "UklBTgEMAAEJCURELU1PTi1SUgEHAAEICEFNRVJJQ0FOAQgAAQYGQklOQVJZAQsAAQ4OSEguTUkuU1NYRkYgQU0BOQABGBhERC1NT04tUlIgSEguTUkuU1NYRkYgQU0BOgABEhJISC5NSS5TU1hGRiBBTSBUWlIBOwABHBxERC1NT04tUlIgSEguTUkuU1NYRkYgQU0g" +
        "VFpSATwAAQEBJAE0AAEGBkJJTkFSWQEyAAEEBEJZVEUBPQABBQVGQUxTRQE+AAELC4AAgZyuPDyAAAAAAaMAARQUAAAAAQAAAIoAAAACAAAAAwAAAHABqgEUFCJDT05ORUNUIiwiUkVTT1VSQ0UiAAHHAAQBAQIE4QAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAAA" +
        "AA==";

    /**
     * Real OCI clients' (sqlplus, rwloadsim; {@code richAuth} tier) phase-two success response —
     * replayed byte-exact from the SAME live MITM capture session (real sqlplus &lt;-&gt; real
     * Oracle Database 23ai) as this class's other rich-tier constants and
     * {@link com.polygres.wire.orawire.frontend.AnoNegotiation}'s and
     * {@link com.polygres.wire.orawire.frontend.ProtocolNegotiation}'s. This was NOT true of an earlier
     * version of this constant
     * — it was captured from a *different* real session than everything else, discovered live to
     * be the actual cause of a post-login `MARKER`/hang that survived several rounds of
     * response-content fixes: this exact bug class (cross-session capability mismatch) was already
     * hit and fixed once before in this codebase, for {@code PROTOCOL_RESPONSE_B64}/
     * {@code DATA_TYPES_RESPONSE_B64} — see that pair's own javadoc. Diffing the two candidate
     * sessions confirmed every OTHER rich-tier constant already came from one consistent session;
     * only this one needed re-capturing. Follows the exact same "byte-exact template, patch only
     * the genuinely per-session field" strategy already used by {@link #sendPhaseTwoSuccess} for
     * the legacy shape — attempting to fully generalize this tier's KV-pair grammar (as was done
     * for {@link #sendPhaseOneResponseRich}) turned out not to hold uniformly here:
     * {@code AUTH_CAPABILITY_TABLE}'s key used a narrower prefix width than every other key in the
     * same response, and this key's own <b>value</b> — the one field that must be patched
     * per-session — is not one contiguous run of hex characters at all.
     *
     * <p>Confirmed live: {@code AUTH_SVR_RESPONSE}'s 96-hex-char value is split by 10 fixed,
     * unexplained bytes ({@code 00 00 02 8b 06 00 00 00 20 00} in the capture) into a 9-character
     * first chunk and an 87-character second chunk — not length-related to anything else in this
     * response (their sum, 96, matches {@link #PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH} exactly,
     * confirming the split is real and not a mis-parse). Rather than guess why, the split point
     * and the 10 filler bytes between the two chunks are treated as fixed template content — like
     * everything else in this response — and only the two hex sub-strings either side of them are
     * patched with this call's own computed ciphertext hex, split the same 9/87 way.
     */
    private static final int PHASE_TWO_RICH_SVR_RESPONSE_CHUNK1_OFFSET = 1948;
    private static final int PHASE_TWO_RICH_SVR_RESPONSE_CHUNK1_LENGTH = 9;
    private static final int PHASE_TWO_RICH_SVR_RESPONSE_CHUNK2_OFFSET = 1967;
    private static final int PHASE_TWO_RICH_SVR_RESPONSE_CHUNK2_LENGTH = 87;
    private static final int PHASE_TWO_RICH_CALLNUMBER_OFFSET = 2533;
    private static final int PHASE_TWO_RICH_CALLNUMBER_LENGTH = 2;
    private static final String PHASE_TWO_RESPONSE_EXTENDED_B64 =
        "CDQAEwAAABNBVVRIX1ZFUlNJT05fU1RSSU5HIgAAACItIERldmVsb3AsIExlYXJuLCBhbmQgUnVuIGZvciBGcmVlAAAAABAAAAAQQVVUSF9WRVJT" +
        "SU9OX1NRTAIAAAACMjYAAAAAEwAAABNBVVRIX1hBQ1RJT05fVFJBSVRTAQAAAAEzAAAAAA8AAAAPQVVUSF9WRVJTSU9OX05PCQAAAAkzODc1ODgw" +
        "OTYAAAAAEwAAABNBVVRIX1ZFUlNJT05fU1RBVFVTAQAAAAEwAAAAABUAAAAVQVVUSF9DQVBBQklMSVRZX1RBQkxFAAAAAAAAAAAPAAAAD0FVVEhf" +
        "TEFTVF9MT0dJThoAAAAaNzg3RTA4MDkwNjFGMEEwMDAwMDAwMDAwMDAAAAAACwAAAAtBVVRIX0RCTkFNRQgAAAAIRlJFRVBEQjEAAAAAEQAAABFB" +
        "VVRIX0RCX01PVU5UX0lEAAoAAAAKMTUxMjU1NjA5MgAAAAALAAAAC0FVVEhfREJfSUQACgAAAAoyOTUyNzc0Mzk0AAAAAAwAAAAMQVVUSF9VU0VS" +
        "X0lEAQAAAAE5AAAAAA8AAAAPQVVUSF9TRVNTSU9OX0lEAwAAAAMyMDkAAAAADwAAAA9BVVRIX1NFUklBTF9OVU0FAAAABTQ3NzA4AAAAABAAAAAQ" +
        "QVVUSF9JTlNUQU5DRV9OTwEAAAABMQAAAAAQAAAAEEFVVEhfRkFJTE9WRVJfSUQBAAAAATEAAAAADwAAAA9BVVRIX1NFUlZFUl9QSUQGAAAABjE0" +
        "MDg5OAAAAAATAAAAE0FVVEhfU0NfU0VSVkVSX0hPU1QMAAAADGQ3NmZmZGViNWIwYQAAAAAVAAAAFUFVVEhfU0NfREJVTklRVUVfTkFNRQQAAAAE" +
        "RlJFRQAAAAAVAAAAFUFVVEhfU0NfSU5TVEFOQ0VfTkFNRQQAAAAERlJFRQAAAAATAAAAE0FVVEhfU0NfSU5TVEFOQ0VfSUQBAAAAATEAAAAAGwAA" +
        "ABtBVVRIX1NDX0lOU1RBTkNFX1NUQVJUX1RJTUUkAAAAJDIwMjYtMDgtMDUgMTY6NDQ6NDIuMDAwMDAwMDAwIC0wNzowMAAAAAARAAAAEUFVVEhf" +
        "U0NfREJfRE9NQUlOAAAAAAAAAAAUAAAAFEFVVEhfU0NfU0VSVklDRV9OQU1FCAAAAAhmcmVlcGRiMQAAAAAbAAAAG0FVVEhfT05TX1JMQl9TVUJT" +
        "Q1JfUEFUVEVSTjQAAAA0JSJldmVudFR5cGU9ZGF0YWJhc2UvZXZlbnQvc2VydmljZW1ldHJpY3MvZnJlZXBkYjEiAAAAAAAaAAAAGkFVVEhfT05T" +
        "X0hBX1NVQlNDUl9QQVRURVJOSQAAAEkoImV2ZW50VHlwZT1kYXRhYmFzZS9ldmVudC9zZXJ2aWNlIikgfCAoImV2ZW50VHlwZT1kYXRhYmFzZS9l" +
        "dmVudC9ob3N0IikAAAAAABoAAAAaQVVUSF9TQ19SRUFMX0RCVU5JUVVFX05BTUUEAAAABEZSRUUAAAAAEQAAABFBVVRIX0lOU1RBTkNFTkFNRQQA" +
        "AAAERlJFRQAAAAAPAAAAD0FVVEhfTkxTX0xYTEFOAAgAAAAIQU1FUklDQU4AAAAAFgAAABZBVVRIX05MU19MWENURVJSSVRPUlkABwAAAAdBTUVS" +
        "SUNBAAAAABUAAAAVQVVUSF9OTFNfTFhDQ1VSUkVOQ1kAAQAAAAEkAAAAABQAAAAUQVVUSF9OTFNfTFhDSVNPQ1VSUgAHAAAAB0FNRVJJQ0EAAAAA" +
        "FQAAABVBVVRIX05MU19MWENOVU1FUklDUwACAAAAAi4sAAAAABMAAAATQVVUSF9OTFNfTFhDREFURUZNAAkAAAAJREQtTU9OLVJSAAAAABUAAAAV" +
        "QVVUSF9OTFNfTFhDREFURUxBTkcACAAAAAhBTUVSSUNBTgAAAAARAAAAEUFVVEhfTkxTX0xYQ1NPUlQABgAAAAZCSU5BUlkAAAAAFQAAABVBVVRI" +
        "X05MU19MWENDQUxFTkRBUgAJAAAACUdSRUdPUklBTgAAAAAVAAAAFUFVVEhfTkxTX0xYQ1VOSU9OQ1VSAAEAAAABJAAAAAATAAAAE0FVVEhfTkxT" +
        "X0xYQ1RJTUVGTQAOAAAADkhILk1JLlNTWEZGIEFNAAAAABMAAAATQVVUSF9OTFNfTFhDU1RNUEZNABgAAAAYREQtTU9OLVJSIEhILk1JLlNTWEZG" +
        "IEFNAAAAABMAAAATQVVUSF9OTFNfTFhDVFRaTkZNABIAAAASSEguTUkuU1NYRkYgQU0gVFpSAAAAABMAAAATQVVUSF9OTFNfTFhDU1RaTkZNABwA" +
        "AAAcREQtTU9OLVJSIEhILk1JLlNTWEZGIEFNIFRaUgAAAAAYAAAAGEFVVEhfTkxTX0xYTEVOU0VNQU5USUNTAAQAAAAEQllURQAAAAAZAAAAGUFV" +
        "VEhfTkxTX0xYTkNIQVJDT05WRVhDUAAFAAAABUZBTFNFAAAAABAAAAAQQVVUSF9OTFNfTFhDT01QAAYAAAAGQklOQVJZAAAAABEAAAARQVVUSF9T" +
        "VlJfUkVTUE9OU0VgAAAAYDY4Nzg3RUIzRQAAAosGAAAAIABGQzhDRTMyMjY0NTBGNUIxNjY1NUZGNzQzQTY0NzUxNjk0RTZENDQ2NjhBQTEyMzYx" +
        "OTY5QTc0MURDNEVFQzRFNjY3RUI2MDZGMjhBQ0ZGREM3QTEzMzYAAAAAFQAAABVBVVRIX01BWF9PUEVOX0NVUlNPUlMDAAAAAzMwMAAAAAANAAAA" +
        "DUFVVEhfUERCX1VJRAAKAAAACjI5NTI3NzQzOTQAAAAAFAAAABRBVVRIX01BWF9JREVOX0xFTkdUSAMAAAADMTI4AAAAAAoAAAAKQVVUSF9GTEFH" +
        "UwEAAAABMQAAAAAQAAAAEEFVVEhfU0VSVkVSX1RZUEUBAAAAATEAAAAAGAAAABhBVVRIX1NFUlZFUl9DQVBBQklMSVRJRVMBAAAAATEAAAAAEAAA" +
        "ABBBVVRIX1JFU0VUX1NUQVRFAQAAAAEwAAAAABcFAQAQBgAAABYAAAAACwAAAAuAAAAANTw8gAAAAKMAAAAAAGQAAABkAAAAAQAAAAkAAAAEAAAA" +
        "CgAAAEMAAAALAAAARAAAAAwAAAAOAAAADwAAABUAAAAjAAAAJAAAADIAAAAzAAAAPwAAAEAAAABBAAAAagAAAGsAAAByAAAAegAAAH0AAAB/AAAA" +
        "IaoAHQAAAB0iREJBIiwiQVFfQURNSU5JU1RSQVRPUl9ST0xFIgAAAADHAAQAAAAESElHSAAAAADMAAAAAAAEAAAABAAAAADKAAAAAAAEAAAABARw" +
        "2GzLAAAAAAAEAQAAAGIEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
        "AAAAHQ==";

    private void sendPhaseTwoSuccessRich(OutputStream out, byte[] comboKey, boolean largeSdu) throws IOException {
        byte[] plaintext = new byte[32];
        RANDOM.nextBytes(plaintext);
        System.arraycopy("SERVER_TO_CLIENT".getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, plaintext, 16, 16);
        byte[] authSvrResponse = OracleCrypto.encryptCbcPkcs7(comboKey, plaintext);
        String authSvrResponseHex = hex(authSvrResponse);
        if (authSvrResponseHex.length() != PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH) {
            throw new IllegalStateException("unexpected AUTH_SVR_RESPONSE hex length: " + authSvrResponseHex.length());
        }
        byte[] payload = java.util.Base64.getDecoder().decode(PHASE_TWO_RESPONSE_EXTENDED_B64);
        byte[] chunk1 = authSvrResponseHex.substring(0, PHASE_TWO_RICH_SVR_RESPONSE_CHUNK1_LENGTH)
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        byte[] chunk2 = authSvrResponseHex.substring(PHASE_TWO_RICH_SVR_RESPONSE_CHUNK1_LENGTH)
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        System.arraycopy(chunk1, 0, payload, PHASE_TWO_RICH_SVR_RESPONSE_CHUNK1_OFFSET, chunk1.length);
        System.arraycopy(chunk2, 0, payload, PHASE_TWO_RICH_SVR_RESPONSE_CHUNK2_OFFSET, chunk2.length);
        // Two more real-session-varying bytes, found by diffing two independently captured real
        // phase-two responses (0xBA 0x02 vs 0xE4 0x03 at this offset — everything else outside
        // AUTH_SVR_RESPONSE was byte-identical between the two sessions, isolating this as the
        // only other genuinely per-session field). Same relative position/pattern as the legacy
        // path's own PHASE_TWO_TEMPLATE_CALLNUMBER_OFFSET. Not tied to anything client-observable
        // (both real sessions had the same phase-two request sequenceNumber byte, ruling that out
        // directly) — randomized here on the same "don't replay one real session's specific value
        // into every connection" principle already applied to AUTH_GLOBALLY_UNIQUE_DBID.
        // Randomizing it alone did NOT resolve the post-login hang documented in
        // ARCHITECTURE.md's O5LOGON rich-tier section — kept anyway since it's evidence-based
        // (a real, confirmed per-session field), not because it's proven to matter.
        byte[] callNumberBytes = randomBytes(PHASE_TWO_RICH_CALLNUMBER_LENGTH);
        System.arraycopy(callNumberBytes, 0, payload, PHASE_TWO_RICH_CALLNUMBER_OFFSET, callNumberBytes.length);
        sendDataFragmented(out, payload, largeSdu);
    }

    private void sendPhaseTwoSuccess(OutputStream out, byte[] comboKey, int sequenceNumber, boolean largeSdu)
            throws IOException {
        byte[] plaintext = new byte[32];
        RANDOM.nextBytes(plaintext); // bytes[0:16]: unconstrained by the client check (spec §1.7/§3.2.5)
        System.arraycopy("SERVER_TO_CLIENT".getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, plaintext, 16, 16);
        byte[] authSvrResponse = OracleCrypto.encryptCbcPkcs7(comboKey, plaintext);
        String authSvrResponseHex = hex(authSvrResponse);
        if (authSvrResponseHex.length() != PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH) {
            throw new IllegalStateException("unexpected AUTH_SVR_RESPONSE hex length: " + authSvrResponseHex.length());
        }

        byte[] payload = java.util.Base64.getDecoder().decode(PHASE_TWO_RESPONSE_B64);
        System.arraycopy(authSvrResponseHex.getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0,
                payload, PHASE_TWO_TEMPLATE_SVR_RESPONSE_OFFSET, PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH);
        // NOT patched (tried live, this session): patching this byte to this call's real
        // sequenceNumber regressed python-oracledb — it hung post-auth instead of completing —
        // while not fixing ojdbc11 either. Reverted; left frozen at the donor capture's value (2)
        // like every other untouched field in this template. See PHASE_TWO_TEMPLATE_CALLNUMBER_OFFSET's
        // javadoc for why this byte was suspected, and ARCHITECTURE.md §13 for the full history.
        sendData(out, payload, largeSdu);
    }

    /** No cited wire format for rejection (spec §5 item 9); uses the TTC error unit with a nonzero error_num. */
    private void sendRejection(OutputStream out, boolean largeSdu) throws IOException {
        TtcWriter w = new TtcWriter();
        ResponseWriter.writeErrorEnd(w, 1017, "invalid username/password", 0);
        sendData(out, w.toByteArray(), largeSdu);
    }

    // AUTH_VFR_DATA is the one key whose trailing ub4 the client reads as verifier_type, not flags (spec §1.3/§3.1.8).
    private void writePairWithVerifierType(TtcWriter w, String key, String value, long verifierType) {
        AuthKv.writeString(w, key);
        AuthKv.writeString(w, value);
        w.writeUb4(verifierType);
    }


    /**
     * Splits a logical message across multiple physical TNS DATA packets, matching real Oracle
     * server behavior confirmed live via MITM capture (real sqlplus &lt;-&gt; real Oracle
     * Database 23ai): a 2618-byte phase-two success response was sent as a 1967-byte packet
     * followed by a 651-byte packet, {@code TNS_DATA_FLAGS_END_OF_RESPONSE} clear on the first
     * and set on the second — see {@link TnsPacket#encode(boolean, boolean)}. This codebase's
     * other DATA responses (PROTOCOL/DATA_TYPES, even the 3070-byte extended DATA_TYPES response)
     * are sent as one oversized packet and were tolerated live by the same real client, so
     * fragmentation isn't applied universally — only here, where an unfragmented send was found
     * live to make the client abort (TNS MARKER) rather than proceed. {@code CHUNK_LENGTH} is the
     * exact real first-fragment *total packet* size observed (1967) minus the 10-byte DATA
     * packet header, i.e. the payload capacity per fragment.
     */
    private static final int RICH_FRAGMENT_CHUNK_LENGTH = 1967 - 10;

    private void sendDataFragmented(OutputStream out, byte[] payload, boolean largeSdu) throws IOException {
        int offset = 0;
        while (offset < payload.length) {
            int len = Math.min(RICH_FRAGMENT_CHUNK_LENGTH, payload.length - offset);
            byte[] chunk = java.util.Arrays.copyOfRange(payload, offset, offset + len);
            offset += len;
            boolean last = offset >= payload.length;
            TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, chunk);
            out.write(packet.encode(largeSdu, last));
            // Flush each fragment separately rather than once at the end — matches a real
            // server's own distinct writes more closely than letting the OS potentially coalesce
            // both fragments into one TCP segment. Tried live with an added artificial delay
            // between fragments too (ruling out a TCP-segmentation/timing theory for the
            // post-login hang documented in ARCHITECTURE.md §5.5g) — no effect either way; kept
            // as a harmless correctness improvement, not because it fixed anything.
            out.flush();
        }
    }

    private void sendData(OutputStream out, byte[] payload, boolean largeSdu) throws IOException {
        // flags=0 — SUPERSEDES an earlier flags=0x20 guess (found via byte diff against a
        // different, indirect orawire capture). A live packet capture of a real Oracle
        // Database 23 Free instance's own O5LOGON-bearing DATA packet (proxied through a
        // byte-logging TCP relay, real ojdbc11 client) showed a plain 0x00 flags byte at
        // this exact header position, not 0x20.
        TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, payload);
        out.write(packet.encode(largeSdu));
        out.flush();
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static byte[] sha512(byte[] data, int truncateTo) {
        try {
            byte[] digest = java.security.MessageDigest.getInstance("SHA-512").digest(data);
            return Arrays.copyOf(digest, truncateTo);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().withUpperCase().formatHex(bytes);
    }

    public record AuthResult(String username, boolean success) {
    }
}

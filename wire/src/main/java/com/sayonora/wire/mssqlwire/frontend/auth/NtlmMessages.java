package com.sayonora.wire.mssqlwire.frontend.auth;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * NTLMv2 (MS-NLMP) server side for mssqlwire's Windows/SSPI login path -- the post-LOGIN7
 * continuation exchange that {@code MssqlWireSessionHandler} drives once {@code
 * Login7Handler.Credentials#integratedSecurity()} is set: LOGIN7 carries the client's Type-1
 * (Negotiate) message inline in its own {@code ibSSPI}/{@code cbSSPI} field; the server then sends
 * a raw Type-2 (Challenge) message in a standalone {@code TdsPacketType.SSPI} (0x11) packet (no TDS
 * token wrapper -- confirmed live, a real client rejects a wrapped one); the client replies with
 * its own {@code TdsPacketType.SSPI} packet carrying the raw Type-3 (Authenticate) message; the
 * server verifies it and only then sends the usual LOGINACK/error tokens.
 *
 * <p>Only NTLMv2 is implemented (not the older, materially weaker NTLMv1 or LM response schemes) --
 * every current client (confirmed live: mssql-jdbc's {@code authenticationScheme=NTLM}) negotiates
 * NTLMv2 by default, and there is no {@link CredentialStore}-backed way to verify LM/NTLMv1
 * responses that isn't strictly weaker for no real client-compatibility benefit. A client that
 * somehow only offers NTLMv1 is refused with a real login failure rather than silently accepted
 * under a weaker scheme.
 */
public final class NtlmMessages {

    private static final byte[] SIGNATURE = "NTLMSSP\0".getBytes(StandardCharsets.US_ASCII);
    private static final SecureRandom RANDOM = new SecureRandom();

    private NtlmMessages() {
    }

    /** True if {@code blob} looks like a real NTLM Type-1 (Negotiate) message -- just enough of a
     * sanity check to refuse cleanly rather than build a Type-2 challenge for garbage. The actual
     * negotiated-flags content of Type-1 doesn't change how this server responds (it always offers
     * NTLMv2), so nothing else from it is parsed. */
    public static boolean isType1Negotiate(byte[] blob) {
        return blob.length >= 12 && startsWithSignature(blob) && readU32LE(blob, 8) == 1;
    }

    /**
     * Builds a real Type-2 (Challenge) message: signature, type=2, empty target-name (len 0,
     * offset right after the fixed header), negotiate flags (NTLMSSP_NEGOTIATE_NTLM |
     * NEGOTIATE_TARGET_INFO | NEGOTIATE_UNICODE | REQUEST_TARGET | NEGOTIATE_TARGET_TYPE_SERVER --
     * confirmed live against mssql-jdbc: without NEGOTIATE_TARGET_INFO set, the client sends an
     * NTLMv2 response with an EMPTY target-info block, and this server's own verification (which
     * echoes target-info back into its NTProofStr computation) then disagrees with what the client
     * actually signed), the 8-byte server challenge, 8 reserved zero bytes, then the target-info
     * AV_PAIR list NTLMv2 requires the client to fold into its response, terminated by AV_EOL.
     * Real servers put nb-domain-name/nb-computer-name in target-info; a single-tenant server with
     * no real Windows domain has no meaningful values for those, so this uses a fixed, honest
     * placeholder ("WARP") for both rather than fabricating a fake AD domain name -- NTLMv2's
     * security property (binding the response to this exact exchange) only depends on target-info
     * being present and later echoed back unmodified, not on its actual content meaning anything.
     */
    public static byte[] buildType2Challenge(byte[] serverChallenge) {
        if (serverChallenge.length != 8) {
            throw new IllegalArgumentException("NTLM server challenge must be 8 bytes");
        }
        byte[] targetInfo = buildTargetInfo();
        // Real Windows servers (and, confirmed live, mssql-jdbc's own client-side parser) always
        // include the optional 8-byte Version block when NTLMSSP_NEGOTIATE_TARGET_INFO is set --
        // omitting it (spec-legal on its own) made mssql-jdbc's parser read past the end of the
        // message (BufferUnderflowException), so it's included here as real servers do: major=10,
        // minor=0, build=0, 3 reserved zero bytes, NTLMRevisionCurrent=15 (0x0F).
        byte[] version = {10, 0, 0, 0, 0, 0, 0, 0x0F};
        int payloadOffset = 48 + version.length; // 48-byte fixed header + 8-byte Version block
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(SIGNATURE);
        writeU32LE(out, 2); // message type
        writeU16LE(out, 0); // target name len
        writeU16LE(out, 0); // target name max len
        writeU32LE(out, payloadOffset); // target name offset (empty payload, but must still point somewhere real)
        writeU32LE(out, 0x02898215); // NTLMSSP_NEGOTIATE_UNICODE | REQUEST_TARGET | NTLM | NEGOTIATE_ALWAYS_SIGN | TARGET_TYPE_SERVER | NEGOTIATE_TARGET_INFO | NEGOTIATE_128 | NEGOTIATE_KEY_EXCH | NEGOTIATE_56 | NEGOTIATE_VERSION
        out.writeBytes(serverChallenge);
        writeU32LE(out, 0); // reserved (8 bytes)
        writeU32LE(out, 0);
        writeU16LE(out, targetInfo.length); // target info len
        writeU16LE(out, targetInfo.length); // target info max len
        writeU32LE(out, payloadOffset); // target info offset -- empty target name means it starts at the same offset
        out.writeBytes(version);
        out.writeBytes(targetInfo);
        return out.toByteArray();
    }

    private static byte[] buildTargetInfo() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeAvPair(out, 2, "WARP"); // MsvAvNbDomainName
        writeAvPair(out, 1, "WARP"); // MsvAvNbComputerName
        writeTimestampAvPair(out); // MsvAvTimestamp (id 7) -- see its own javadoc
        writeU16LE(out, 0); // MsvAvEOL
        writeU16LE(out, 0);
        return out.toByteArray();
    }

    // FILETIME epoch (1601-01-01T00:00:00Z) to Unix epoch (1970-01-01T00:00:00Z), in 100ns ticks.
    private static final long FILETIME_EPOCH_OFFSET_100NS = 116_444_736_000_000_000L;

    /** MsvAvTimestamp -- an 8-byte little-endian Windows FILETIME (100ns ticks since
     * 1601-01-01). Confirmed live: a real client (mssql-jdbc) still completes the handshake
     * without it (logging "Missing timestamp" and falling back to its own local time for computing
     * its NTLMv2 response's temp blob), but a real Windows AD server always sends one, and a
     * stricter/older client may not tolerate its absence -- included for real spec conformance
     * rather than relying on every client's fallback behavior. */
    private static void writeTimestampAvPair(ByteArrayOutputStream out) {
        long filetime = System.currentTimeMillis() * 10_000L + FILETIME_EPOCH_OFFSET_100NS;
        writeU16LE(out, 7);
        writeU16LE(out, 8);
        for (int i = 0; i < 8; i++) {
            out.write((int) ((filetime >>> (8 * i)) & 0xFF));
        }
    }

    private static void writeAvPair(ByteArrayOutputStream out, int avId, String value) {
        byte[] v = value.getBytes(StandardCharsets.UTF_16LE);
        writeU16LE(out, avId);
        writeU16LE(out, v.length);
        out.writeBytes(v);
    }

    public record Type3Message(String domain, String userName, String workstation, byte[] ntChallengeResponse) {
    }

    /** Parses a real Type-3 (Authenticate) message far enough to verify it: domain/user/workstation
     * (needed to recompute the NTLMv2 key the same way the client did) and the raw
     * NtChallengeResponse field (NTProofStr + the client's own echoed "temp" blob, see {@link
     * #verifyNtlmV2Response}). The LM response, session key, and negotiate-flags fields are read
     * past (their offset/length pairs are in the same fixed block) but not otherwise used --
     * verifying the *NT* response alone is what NTLMv2 auth actually depends on. */
    public static Type3Message parseType3(byte[] blob) {
        if (blob.length < 12 || !startsWithSignature(blob) || readU32LE(blob, 8) != 3) {
            throw new IllegalArgumentException("not a real NTLM Type-3 message");
        }
        // Fixed offset/length fields, in order, each an (len:u16, maxLen:u16, offset:u32) triple:
        // LmChallengeResponse, NtChallengeResponse, DomainName, UserName, Workstation,
        // EncryptedRandomSessionKey (may be absent on an older/shorter message -- not read here).
        int p = 12;
        p += 8; // LmChallengeResponse fields -- not used, see javadoc
        int ntRespLen = readU16LE(blob, p); int ntRespOff = readU32LE(blob, p + 4); p += 8;
        int domainLen = readU16LE(blob, p); int domainOff = readU32LE(blob, p + 4); p += 8;
        int userLen = readU16LE(blob, p); int userOff = readU32LE(blob, p + 4); p += 8;
        int wsLen = readU16LE(blob, p); int wsOff = readU32LE(blob, p + 4); p += 8;

        String domain = readUtf16(blob, domainOff, domainLen);
        String userName = readUtf16(blob, userOff, userLen);
        String workstation = readUtf16(blob, wsOff, wsLen);
        byte[] ntChallengeResponse = java.util.Arrays.copyOfRange(blob, ntRespOff, ntRespOff + ntRespLen);
        return new Type3Message(domain, userName, workstation, ntChallengeResponse);
    }

    /**
     * Verifies a real NTLMv2 NtChallengeResponse (MS-NLMP 3.3.2) against the plaintext password
     * {@link com.sayonora.wire.auth.CredentialStore} holds for {@code userName}. {@code
     * ntChallengeResponse} is NTProofStr (16 bytes) followed by the client's own "temp" blob
     * (timestamp/client-challenge/echoed target-info); this recomputes NTProofStr the same way the
     * client did -- {@code HMAC-MD5(NTOWFv2(password, user, domain), serverChallenge + temp)}, with
     * {@code NTOWFv2 = HMAC-MD5(MD4(UTF16LE(password)), UTF16LE(UPPER(user) + domain))} -- and
     * compares in constant time. {@code serverChallenge} must be the exact 8 bytes this server sent
     * in its own Type-2 message for this same login attempt (see {@link #buildType2Challenge}).
     */
    public static boolean verifyNtlmV2Response(byte[] ntChallengeResponse, byte[] serverChallenge,
            String password, String userName, String domain) {
        if (ntChallengeResponse.length < 16) {
            return false;
        }
        byte[] ntProofStr = java.util.Arrays.copyOfRange(ntChallengeResponse, 0, 16);
        byte[] temp = java.util.Arrays.copyOfRange(ntChallengeResponse, 16, ntChallengeResponse.length);

        byte[] ntowfV2 = ntowfV2(password, userName, domain);
        byte[] serverChallengeAndTemp = concat(serverChallenge, temp);
        byte[] computed = hmacMd5(ntowfV2, serverChallengeAndTemp);
        return java.security.MessageDigest.isEqual(computed, ntProofStr);
    }

    private static byte[] ntowfV2(String password, String userName, String domain) {
        byte[] ntHash = Md4.digest(password.getBytes(StandardCharsets.UTF_16LE));
        String identity = userName.toUpperCase(Locale.ROOT) + (domain == null ? "" : domain);
        return hmacMd5(ntHash, identity.getBytes(StandardCharsets.UTF_16LE));
    }

    private static byte[] hmacMd5(byte[] key, byte[] data) {
        try {
            Mac mac = Mac.getInstance("HmacMD5");
            mac.init(new SecretKeySpec(key, "HmacMD5"));
            return mac.doFinal(data);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacMD5 unavailable", e);
        }
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    public static byte[] randomServerChallenge() {
        byte[] c = new byte[8];
        RANDOM.nextBytes(c);
        return c;
    }

    private static boolean startsWithSignature(byte[] blob) {
        for (int i = 0; i < SIGNATURE.length; i++) {
            if (blob[i] != SIGNATURE[i]) {
                return false;
            }
        }
        return true;
    }

    private static String readUtf16(byte[] data, int offset, int len) {
        return len == 0 ? "" : new String(data, offset, len, StandardCharsets.UTF_16LE);
    }

    private static int readU16LE(byte[] data, int pos) {
        return (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8);
    }

    private static int readU32LE(byte[] data, int pos) {
        return (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8)
                | ((data[pos + 2] & 0xFF) << 16) | ((data[pos + 3] & 0xFF) << 24);
    }

    private static void writeU16LE(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
    }

    private static void writeU32LE(ByteArrayOutputStream out, int v) {
        out.write(v & 0xFF);
        out.write((v >>> 8) & 0xFF);
        out.write((v >>> 16) & 0xFF);
        out.write((v >>> 24) & 0xFF);
    }
}

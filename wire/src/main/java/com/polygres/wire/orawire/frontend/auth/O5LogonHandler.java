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

public final class O5LogonHandler {

    private static final SecureRandom RANDOM = new SecureRandom();
    
    private static final int SESSION_KEY_HALF_LENGTH = 16;
    private static final int VFR_DATA_LENGTH = 16;
    private static final int CSK_SALT_LENGTH = 16;

    private final CredentialStore credentials = new CredentialStore();

    public AuthResult authenticate(TnsPacketReader reader, OutputStream out) throws IOException {
        boolean largeSdu = reader.isLargeSdu();
        
        boolean richAuth = reader.isAnoEligible();
        TnsPacket phaseOnePacket = readNonEmptyPacket(reader);
        FunctionCall call1 = expectFunction(phaseOnePacket, AuthConstants.FUNC_AUTH_PHASE_ONE);
        String username = richAuth
                ? readUsernameAndSkipPairsRich(call1.reader())
                : readUsernameAndSkipPairs(call1.reader());

        byte[] password = credentials.lookupPassword(username);
        if (password == null) {
            sendRejection(out, largeSdu);
            return new AuthResult(username, false, credentials.isMultiUser());
        }

        byte[] verifierData = randomBytes(VFR_DATA_LENGTH);
        byte[] pbkdf2Salt = concat(verifierData, "AUTH_PBKDF2_SPEEDY_KEY".getBytes(java.nio.charset.StandardCharsets.US_ASCII));
        byte[] passwordKey = OracleCrypto.pbkdf2HmacSha512(password, pbkdf2Salt, 64, AuthConstants.PBKDF2_VGEN_COUNT);
        byte[] passwordHash = sha512(concat(passwordKey, verifierData), 32);

        byte[] sessionKeyPartARaw = randomBytes(SESSION_KEY_HALF_LENGTH);
        byte[] authSesskey = OracleCrypto.encryptCbcPkcs7(passwordHash, sessionKeyPartARaw);
        
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
        return new AuthResult(username, success, credentials.isMultiUser());
    }

    private boolean verifyPhaseTwo(Map<String, String> pairs, byte[] passwordHash, byte[] sessionKeyPartA,
            byte[] cskSalt, byte[] expectedPassword) {
        byte[] comboKey = deriveComboKey(pairs, passwordHash, sessionKeyPartA, cskSalt);

        byte[] authPasswordCipher = HexFormat.of().parseHex(pairs.get("AUTH_PASSWORD"));
        byte[] decrypted = OracleCrypto.stripPkcs7(OracleCrypto.decryptCbcNoUnpad(comboKey, authPasswordCipher));
        
        byte[] claimedPassword = Arrays.copyOfRange(decrypted, 16, decrypted.length);
        return Arrays.equals(claimedPassword, expectedPassword);
    }

    private byte[] deriveComboKey(Map<String, String> pairs, byte[] passwordHash, byte[] sessionKeyPartA,
            byte[] cskSalt) {
        byte[] authSesskeyClientCipher = HexFormat.of().parseHex(pairs.get("AUTH_SESSKEY"));
        byte[] sessionKeyPartB = OracleCrypto.decryptCbcNoUnpad(passwordHash, authSesskeyClientCipher);
        
        byte[] tempKey = concat(
                Arrays.copyOf(sessionKeyPartB, 32),
                Arrays.copyOf(sessionKeyPartA, 32));
        byte[] tempKeyHex = HexFormat.of().withUpperCase().formatHex(tempKey)
                .getBytes(java.nio.charset.StandardCharsets.US_ASCII);
        return OracleCrypto.pbkdf2HmacSha512(tempKeyHex, cskSalt, 32, AuthConstants.PBKDF2_SDER_COUNT);
    }

    private String readUsernameAndSkipPairs(TtcReader r) {
        
        r.readUb8();
        int hasUser = r.readUint8();
        long userLen = r.readUb4();
        r.readUb4();
        r.readUint8();
        long numPairs = r.readUb4();
        r.readUint8();
        r.readUint8();
        String username = null;
        if (hasUser != 0) {
            byte[] userBytes = r.readRawOrLengthPrefixedBytes((int) userLen);
            username = new String(userBytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        AuthKv.skipPairs(r, (int) numPairs);
        return username;
    }

    private record RichAuthHeader(int hasUser, int numPairs) {
    }

    private RichAuthHeader readRichAuthHeader(TtcReader r) {
        int hasUser = r.readUint8();
        r.skip(25);
        int numPairs = (int) readLe32(r);
        r.skip(16);
        return new RichAuthHeader(hasUser, numPairs);
    }

    private String readRichUsername(TtcReader r, int hasUser) {
        if (hasUser == 0) {
            return null;
        }
        int userLen = r.readUint8();
        return new String(r.readRawBytes(userLen), java.nio.charset.StandardCharsets.UTF_8);
    }

    private Map<String, String> readRichPairs(TtcReader r, int numPairs) {
        Map<String, String> map = new java.util.LinkedHashMap<>();
        for (int i = 0; i < numPairs; i++) {
            readLe32(r);
            int keyLen = r.readUint8();
            String key = new String(r.readRawBytes(keyLen), java.nio.charset.StandardCharsets.UTF_8);
            long valueOuterLen = readLe32(r);
            String value = null;
            if (valueOuterLen != 0) {
                int valueLen = r.readUint8();
                value = new String(r.readRawBytes(valueLen), java.nio.charset.StandardCharsets.UTF_8);
            }
            readLe32(r);
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
        readRichPairs(r, header.numPairs());
        return username;
    }

    private Map<String, String> readPhaseTwoPairsRich(TtcReader r) {
        RichAuthHeader header = readRichAuthHeader(r);
        readRichUsername(r, header.hasUser());
        return readRichPairs(r, header.numPairs());
    }

    private Map<String, String> readPhaseTwoPairs(TtcReader r) {
        r.readUb8();
        int hasUser = r.readUint8();
        long userLen = r.readUb4();
        r.readUb4();
        r.readUint8();
        long numPairs = r.readUb4();
        r.readUint8();
        r.readUint8();
        if (hasUser != 0) {
            r.readRawOrLengthPrefixedBytes((int) userLen);
        }
        return AuthKv.readPairs(r, (int) numPairs);
    }

    private static TnsPacket readNonEmptyPacket(TnsPacketReader reader) throws IOException {
        TnsPacket packet = reader.readPacket();
        while (packet.payload().length == 0) {
            packet = reader.readPacket();
        }
        return packet;
    }

    private record FunctionCall(TtcReader reader, int sequenceNumber) {
    }

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

    private static final String PHASE_ONE_TERMINATOR_RICH_B64 =
        "AAQBAAAAdgUAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            + "AAAAAAAAAAAd";

    private void sendPhaseOneResponseRich(OutputStream out, byte[] verifierData, byte[] authSesskey, byte[] cskSalt,
            boolean largeSdu) throws IOException {
        java.io.ByteArrayOutputStream buf = new java.io.ByteArrayOutputStream();
        buf.write(TtcConstants.MSG_TYPE_PARAMETER);
        buf.write(6);
        writeRichPair(buf, "AUTH_SESSKEY", hex(authSesskey), 0);
        writeRichPair(buf, "AUTH_VFR_DATA", hex(verifierData), AuthConstants.VERIFIER_TYPE_12C);
        writeRichPair(buf, "AUTH_PBKDF2_CSK_SALT", hex(cskSalt), 0);
        writeRichPair(buf, "AUTH_PBKDF2_VGEN_COUNT", String.valueOf(AuthConstants.PBKDF2_VGEN_COUNT), 0);
        writeRichPair(buf, "AUTH_PBKDF2_SDER_COUNT", String.valueOf(AuthConstants.PBKDF2_SDER_COUNT), 0);
        
        writeRichPair(buf, "AUTH_GLOBALLY_UNIQUE_DBID\0", RICH_TIER_DATABASE_GUID_HEX, 0);
        byte[] terminator = java.util.Base64.getDecoder().decode(PHASE_ONE_TERMINATOR_RICH_B64);
        
        byte[] terminatorVaryingBytes = randomBytes(PHASE_ONE_TERMINATOR_VARYING_LENGTH);
        System.arraycopy(terminatorVaryingBytes, 0, terminator, PHASE_ONE_TERMINATOR_VARYING_OFFSET,
                terminatorVaryingBytes.length);
        buf.write(terminator);
        sendData(out, buf.toByteArray(), largeSdu);
    }

    private static final int PHASE_ONE_TERMINATOR_VARYING_OFFSET = 6;
    private static final int PHASE_ONE_TERMINATOR_VARYING_LENGTH = 2;

    private static final String RICH_TIER_DATABASE_GUID_HEX = "7633D8148E2E259AE5679C2AA50E96A5";

    private static void writeRichPair(java.io.ByteArrayOutputStream buf, String key, String value, long flags) {
        
        writeRichLengthPrefixedString(buf, key, 2);
        writeRichLengthPrefixedString(buf, value, 1);
        writeLe(buf, flags, 3);
    }

    private static void writeRichLengthPrefixedString(java.io.ByteArrayOutputStream buf, String s, int outerWidth) {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int len = bytes.length;
        writeBe(buf, len, outerWidth);
        writeBe(buf, len, 4);
        buf.write(bytes, 0, bytes.length);
    }

    private static void writeBe(java.io.ByteArrayOutputStream buf, long value, int width) {
        for (int i = width - 1; i >= 0; i--) {
            buf.write((int) ((value >> (8 * i)) & 0xFF));
        }
    }

    private static void writeLe(java.io.ByteArrayOutputStream buf, long value, int width) {
        for (int i = 0; i < width; i++) {
            buf.write((int) ((value >> (8 * i)) & 0xFF));
        }
    }

    private void sendPhaseOneResponse(OutputStream out, byte[] verifierData, byte[] authSesskey, byte[] cskSalt,
            int sequenceNumber, boolean largeSdu) throws IOException {
        
        TtcWriter w = new TtcWriter();
        w.writeUint8(TtcConstants.MSG_TYPE_PARAMETER);
        w.writeUb2(6);
        AuthKv.writePair(w, "AUTH_SESSKEY", hex(authSesskey), 0);
        writePairWithVerifierType(w, "AUTH_VFR_DATA", hex(verifierData), AuthConstants.VERIFIER_TYPE_12C);
        AuthKv.writePair(w, "AUTH_PBKDF2_CSK_SALT", hex(cskSalt), 0);
        AuthKv.writePair(w, "AUTH_PBKDF2_VGEN_COUNT", String.valueOf(AuthConstants.PBKDF2_VGEN_COUNT), 0);
        AuthKv.writePair(w, "AUTH_PBKDF2_SDER_COUNT", String.valueOf(AuthConstants.PBKDF2_SDER_COUNT), 0);
        
        AuthKv.writeString(w, "AUTH_GLOBALLY_UNIQUE_DBID\0");
        AuthKv.writeString(w, hex(randomBytes(16)));
        w.writeUb4(0);
        
        ResponseWriter.writeO5LogonSuccessEnd(w, 0, 0);
        sendData(out, w.toByteArray(), largeSdu);
    }

    private static final int PHASE_TWO_TEMPLATE_SVR_RESPONSE_OFFSET = 1635;
    private static final int PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH = 96;
    
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
        
        byte[] callNumberBytes = randomBytes(PHASE_TWO_RICH_CALLNUMBER_LENGTH);
        System.arraycopy(callNumberBytes, 0, payload, PHASE_TWO_RICH_CALLNUMBER_OFFSET, callNumberBytes.length);
        sendDataFragmented(out, payload, largeSdu);
    }

    private void sendPhaseTwoSuccess(OutputStream out, byte[] comboKey, int sequenceNumber, boolean largeSdu)
            throws IOException {
        byte[] plaintext = new byte[32];
        RANDOM.nextBytes(plaintext);
        System.arraycopy("SERVER_TO_CLIENT".getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, plaintext, 16, 16);
        byte[] authSvrResponse = OracleCrypto.encryptCbcPkcs7(comboKey, plaintext);
        String authSvrResponseHex = hex(authSvrResponse);
        if (authSvrResponseHex.length() != PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH) {
            throw new IllegalStateException("unexpected AUTH_SVR_RESPONSE hex length: " + authSvrResponseHex.length());
        }

        byte[] payload = java.util.Base64.getDecoder().decode(PHASE_TWO_RESPONSE_B64);
        System.arraycopy(authSvrResponseHex.getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0,
                payload, PHASE_TWO_TEMPLATE_SVR_RESPONSE_OFFSET, PHASE_TWO_TEMPLATE_SVR_RESPONSE_LENGTH);
        
        sendData(out, payload, largeSdu);
    }

    private void sendRejection(OutputStream out, boolean largeSdu) throws IOException {
        TtcWriter w = new TtcWriter();
        ResponseWriter.writeErrorEnd(w, 1017, "invalid username/password", 0);
        sendData(out, w.toByteArray(), largeSdu);
    }

    private void writePairWithVerifierType(TtcWriter w, String key, String value, long verifierType) {
        AuthKv.writeString(w, key);
        AuthKv.writeString(w, value);
        w.writeUb4(verifierType);
    }

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
            out.flush();
        }
    }

    private void sendData(OutputStream out, byte[] payload, boolean largeSdu) throws IOException {
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

    /** {@code realIdentity} is true exactly when {@code POLYWIRE_AUTH_CREDENTIALS} is configured
     * (see {@link CredentialStore}), i.e. {@code username} is a real, distinguishable per-caller
     * identity worth propagating into {@link com.polygres.wire.core.AccessContext} -- as opposed
     * to the single shared-credential default, where every caller presents the same username and
     * carrying it into RLS/audit would be misleading. */
    public record AuthResult(String username, boolean success, boolean realIdentity) {
    }
}

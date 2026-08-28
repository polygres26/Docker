package com.nexagres.wire.orawire.frontend.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nexagres.wire.orawire.ttc.TtcConstants;
import com.nexagres.wire.orawire.ttc.TtcReader;
import com.nexagres.wire.orawire.ttc.TtcWriter;
import com.nexagres.wire.orawire.wireformat.TnsPacket;
import com.nexagres.wire.orawire.wireformat.TnsPacketReader;
import com.nexagres.wire.orawire.wireformat.TnsPacketType;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Full round-trip test: a hand-written client (implementing the same math
 * as python-oracledb's AuthMessage._generate_verifier, per
 * reference/o5logon_auth_spec.md §2.1/2.3) against our real
 * {@link O5LogonHandler} over an in-memory pipe. This is the strongest
 * verification available without a real Oracle client, since it exercises
 * both the wire framing and the crypto derivation end to end.
 */
class O5LogonHandlerTest {

    @Test
    void successfulLoginDerivesMatchingComboKeyAndVerifiesServerResponse() throws Exception {
        PipedOutputStream clientToServer = new PipedOutputStream();
        PipedInputStream serverReadsFromClient = new PipedInputStream(clientToServer, 1 << 16);
        PipedOutputStream serverToClient = new PipedOutputStream();
        PipedInputStream clientReadsFromServer = new PipedInputStream(serverToClient, 1 << 16);

        CompletableFuture<O5LogonHandler.AuthResult> serverResult = CompletableFuture.supplyAsync(() -> {
            try {
                return new O5LogonHandler().authenticate(new TnsPacketReader(serverReadsFromClient), serverToClient);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        byte[] password = "orapg".getBytes(StandardCharsets.UTF_8); // matches CredentialStore's default
        FakeClient client = new FakeClient(clientToServer, clientReadsFromServer, "orapg", password);
        client.runHandshake();

        O5LogonHandler.AuthResult result = serverResult.get();
        assertTrue(result.success(), "server should accept the correct password");
        assertEquals("orapg", result.username());
        assertTrue(client.serverResponseVerified, "client should be able to verify AUTH_SVR_RESPONSE with its own combo_key");
    }

    @Test
    void wrongPasswordIsRejected() throws Exception {
        PipedOutputStream clientToServer = new PipedOutputStream();
        PipedInputStream serverReadsFromClient = new PipedInputStream(clientToServer, 1 << 16);
        PipedOutputStream serverToClient = new PipedOutputStream();
        PipedInputStream clientReadsFromServer = new PipedInputStream(serverToClient, 1 << 16);

        CompletableFuture<O5LogonHandler.AuthResult> serverResult = CompletableFuture.supplyAsync(() -> {
            try {
                return new O5LogonHandler().authenticate(new TnsPacketReader(serverReadsFromClient), serverToClient);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        FakeClient client = new FakeClient(clientToServer, clientReadsFromServer, "orapg",
                "wrong-password".getBytes(StandardCharsets.UTF_8));
        client.runHandshake();

        O5LogonHandler.AuthResult result = serverResult.get();
        assertTrue(!result.success(), "server should reject the wrong password");
    }

    /** Client-side math transcribed from o5logon_auth_spec.md §2.1 (12c branch) and §2.3. */
    private static final class FakeClient {
        final PipedOutputStream out;
        final TnsPacketReader in;
        final String username;
        final byte[] password;
        boolean serverResponseVerified;

        FakeClient(PipedOutputStream out, PipedInputStream in, String username, byte[] password) {
            this.out = out;
            this.in = new TnsPacketReader(in);
            this.username = username;
            this.password = password;
        }

        void runHandshake() throws Exception {
            sendPhaseOneRequest();
            TnsPacket phaseOneResponse = in.readPacket();
            Map<String, String> resp1 = readParameterResponse(phaseOneResponse);

            byte[] verifierData = HexFormat.of().parseHex(resp1.get("AUTH_VFR_DATA"));
            int vgenCount = Integer.parseInt(resp1.get("AUTH_PBKDF2_VGEN_COUNT"));
            int sderCount = Integer.parseInt(resp1.get("AUTH_PBKDF2_SDER_COUNT"));
            byte[] cskSalt = HexFormat.of().parseHex(resp1.get("AUTH_PBKDF2_CSK_SALT"));
            byte[] serverAuthSesskey = HexFormat.of().parseHex(resp1.get("AUTH_SESSKEY"));

            byte[] pbkdf2Salt = concat(verifierData, "AUTH_PBKDF2_SPEEDY_KEY".getBytes(StandardCharsets.US_ASCII));
            byte[] passwordKey = OracleCrypto.pbkdf2HmacSha512(password, pbkdf2Salt, 64, vgenCount);
            byte[] passwordHash = Arrays.copyOf(sha512(concat(passwordKey, verifierData)), 32);

            byte[] sessionKeyPartA = OracleCrypto.decryptCbcNoUnpad(passwordHash, serverAuthSesskey);
            byte[] sessionKeyPartB = new byte[32];
            new java.security.SecureRandom().nextBytes(sessionKeyPartB);
            byte[] clientAuthSesskey = OracleCrypto.encryptCbcPkcs7(passwordHash, sessionKeyPartB);

            byte[] tempKey = concat(Arrays.copyOf(sessionKeyPartB, 32), Arrays.copyOf(sessionKeyPartA, 32));
            byte[] tempKeyHex = HexFormat.of().withUpperCase().formatHex(tempKey).getBytes(StandardCharsets.US_ASCII);
            byte[] comboKey = OracleCrypto.pbkdf2HmacSha512(tempKeyHex, cskSalt, 32, sderCount);

            byte[] saltedPassword = concat(randomBytes(16), password);
            byte[] authPassword = OracleCrypto.encryptCbcPkcs7(comboKey, saltedPassword);

            sendPhaseTwoRequest(HexFormat.of().withUpperCase().formatHex(clientAuthSesskey),
                    HexFormat.of().withUpperCase().formatHex(authPassword));

            TnsPacket phaseTwoResponse = in.readPacket();
            TtcReader r = new TtcReader(phaseTwoResponse.payload());
            int msgType = r.readUint8();
            if (msgType == TtcConstants.MSG_TYPE_PARAMETER) {
                int numPairs = r.readUb2();
                for (int i = 0; i < numPairs; i++) {
                    String key = readTwoLenString(r);
                    String value = readTwoLenString(r);
                    r.readUb4();
                    if ("AUTH_SVR_RESPONSE".equals(key)) {
                        byte[] cipher = HexFormat.of().parseHex(value);
                        byte[] plain = OracleCrypto.stripPkcs7(OracleCrypto.decryptCbcNoUnpad(comboKey, cipher));
                        byte[] marker = Arrays.copyOfRange(plain, 16, 32);
                        serverResponseVerified = Arrays.equals(marker, "SERVER_TO_CLIENT".getBytes(StandardCharsets.US_ASCII));
                    }
                }
            }
            // else: MSG_TYPE_ERROR (rejection) — serverResponseVerified stays false
        }

        private void sendPhaseOneRequest() throws IOException {
            TtcWriter w = new TtcWriter();
            w.writeUint8(TtcConstants.MSG_TYPE_FUNCTION);
            w.writeUint8(AuthConstants.FUNC_AUTH_PHASE_ONE);
            w.writeUint8(1); // seq num
            writeHeader(w, AuthConstants.AUTH_MODE_LOGON, 5);
            writePairTwoLen(w, "AUTH_TERMINAL", "unknown", 0);
            writePairTwoLen(w, "AUTH_PROGRAM_NM", "test", 0);
            writePairTwoLen(w, "AUTH_MACHINE", "localhost", 0);
            writePairTwoLen(w, "AUTH_PID", "1", 0);
            writePairTwoLen(w, "AUTH_SID", "tester", 0);
            send(w);
        }

        private void sendPhaseTwoRequest(String sessKeyHex, String passwordHex) throws IOException {
            TtcWriter w = new TtcWriter();
            w.writeUint8(TtcConstants.MSG_TYPE_FUNCTION);
            w.writeUint8(AuthConstants.FUNC_AUTH_PHASE_TWO);
            w.writeUint8(2); // seq num
            writeHeader(w, AuthConstants.AUTH_MODE_LOGON | AuthConstants.AUTH_MODE_WITH_PASSWORD, 2);
            writePairTwoLen(w, "AUTH_SESSKEY", sessKeyHex, 1);
            writePairTwoLen(w, "AUTH_PASSWORD", passwordHex, 0);
            send(w);
        }

        private void writeHeader(TtcWriter w, long authMode, int numPairs) {
            // token_num (UB8), value 0 — matches every real capture seen so far. Present ahead of
            // hasUser once ttc_field_version >= 23.1_ext1 is negotiated (see
            // O5LogonHandler.readUsernameAndSkipPairs's javadoc for the live-capture history: this
            // server's own real PROTOCOL/DATA_TYPES capability advertisement now clears that
            // threshold, so a real client's AUTH_PHASE_ONE/AUTH_PHASE_TWO requests both carry it).
            w.writeUb8(0);
            w.writeUint8(1); // has_user
            byte[] userBytes = username.getBytes(StandardCharsets.UTF_8);
            w.writeUb4(userBytes.length);
            w.writeUb4(authMode);
            w.writeUint8(1);
            w.writeUb4(numPairs);
            w.writeUint8(1);
            w.writeUint8(1);
            w.writeBytesWithLength(userBytes);
        }

        private void writePairTwoLen(TtcWriter w, String key, String value, long flags) {
            writeTwoLenString(w, key);
            writeTwoLenString(w, value);
            w.writeUb4(flags);
        }

        private void writeTwoLenString(TtcWriter w, String s) {
            byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
            w.writeUb4(bytes.length);
            w.writeBytesWithLength(bytes);
        }

        private String readTwoLenString(TtcReader r) {
            long outerLen = r.readUb4();
            if (outerLen == 0) {
                return null;
            }
            byte[] inner = r.readBytesWithLength();
            return new String(inner, StandardCharsets.UTF_8);
        }

        private Map<String, String> readParameterResponse(TnsPacket packet) {
            TtcReader r = new TtcReader(packet.payload());
            int msgType = r.readUint8();
            assertEquals(TtcConstants.MSG_TYPE_PARAMETER, msgType);
            int numPairs = r.readUb2();
            Map<String, String> map = new java.util.LinkedHashMap<>();
            for (int i = 0; i < numPairs; i++) {
                String key = readTwoLenString(r);
                String value = readTwoLenString(r);
                r.readUb4();
                map.put(key, value);
            }
            return map;
        }

        private void send(TtcWriter w) throws IOException {
            TnsPacket packet = new TnsPacket(TnsPacketType.DATA, 0, w.toByteArray());
            out.write(packet.encode(false));
            out.flush();
        }

        private static byte[] randomBytes(int n) {
            byte[] b = new byte[n];
            new java.security.SecureRandom().nextBytes(b);
            return b;
        }

        private static byte[] concat(byte[] a, byte[] b) {
            byte[] out = new byte[a.length + b.length];
            System.arraycopy(a, 0, out, 0, a.length);
            System.arraycopy(b, 0, out, a.length, b.length);
            return out;
        }

        private static byte[] sha512(byte[] data) {
            try {
                return java.security.MessageDigest.getInstance("SHA-512").digest(data);
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}

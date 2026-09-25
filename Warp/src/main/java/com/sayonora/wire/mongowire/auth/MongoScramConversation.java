package com.sayonora.wire.mongowire.auth;

import com.sayonora.wire.auth.CredentialStore;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Server side of a single RFC 5802 SCRAM-SHA-256 conversation, as used by every modern MongoDB
 * driver's default auth mechanism (mongo-java-driver prefers SCRAM-SHA-256 over SCRAM-SHA-1
 * whenever both are supported). One instance lives for the lifetime of one login attempt on one
 * connection -- see {@code MongoCommandDispatcher}'s {@code pendingScram} field, which is the only
 * thing keeping it alive between the {@code saslStart} and {@code saslContinue} round trips (a
 * real client always does exactly one of each, back to back, on the same connection).
 *
 * <p>There is no persisted per-user SCRAM verifier (salt/iterations/StoredKey) anywhere in this
 * codebase -- unlike a real MongoDB server, which stores those once at {@code createUser} time,
 * Warp only has {@link CredentialStore}'s plaintext password (the same shared secret orawire's
 * O5LOGON already depends on being able to see server-side, for the same structural reason: there
 * is no other source of truth to hash from). So a fresh salt is generated per login attempt
 * instead of being looked up -- this is spec-legal (RFC 5802 never requires a stable salt across
 * logins, only within one conversation) and sidesteps needing new persistent storage for a single
 * shared/multi-user credential list that already lives in an env var.
 */
public final class MongoScramConversation {

    private static final int ITERATION_COUNT = 15000;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final Base64.Encoder B64 = Base64.getEncoder();
    private static final Base64.Decoder B64D = Base64.getDecoder();

    private final String username;
    private final byte[] saltedPassword;
    private final String clientFirstMessageBare;
    private final String serverFirstMessage;
    private final String rnonce;

    private MongoScramConversation(String username, byte[] saltedPassword, String clientFirstMessageBare,
            String serverFirstMessage, String rnonce) {
        this.username = username;
        this.saltedPassword = saltedPassword;
        this.clientFirstMessageBare = clientFirstMessageBare;
        this.serverFirstMessage = serverFirstMessage;
        this.rnonce = rnonce;
    }

    /**
     * Handles {@code saslStart}. Returns {@code null} (rather than throwing) when the username is
     * unknown or has no configured password -- the caller replies with a generic auth-failed
     * error either way, exactly like a real server never reveals "no such user" vs. "wrong
     * password" during SCRAM (that distinction is only made by a real client-first-message parse
     * failure, which IS a distinct, legitimate protocol error).
     */
    public static MongoScramConversation start(String clientFirstMessage, CredentialStore credentials) {
        Map<String, String> attrs = parseAttrs(stripGs2Header(clientFirstMessage));
        String username = unescapeUsername(attrs.get("n"));
        String clientNonce = attrs.get("r");
        if (username == null || clientNonce == null) {
            throw new IllegalArgumentException("malformed SCRAM client-first-message");
        }
        byte[] password = credentials.lookupPassword(username);
        if (password == null) {
            return null;
        }
        byte[] salt = new byte[16];
        RANDOM.nextBytes(salt);
        byte[] saltedPassword = hi(password, salt, ITERATION_COUNT);
        String rnonce = clientNonce + Base64.getEncoder().withoutPadding()
                .encodeToString(randomBytes(18));
        String clientFirstMessageBare = "n=" + attrs.get("rawN") + ",r=" + clientNonce;
        String serverFirstMessage = "r=" + rnonce + ",s=" + B64.encodeToString(salt) + ",i=" + ITERATION_COUNT;
        return new MongoScramConversation(username, saltedPassword, clientFirstMessageBare, serverFirstMessage, rnonce);
    }

    public String serverFirstMessage() {
        return serverFirstMessage;
    }

    public String username() {
        return username;
    }

    /**
     * Handles {@code saslContinue}. Returns the server-final-message ({@code "v=<signature>"}) on
     * success, or {@code null} if the client's proof doesn't verify (wrong password, or a
     * conversation that was never matched to a real user in {@link #start}).
     */
    public String verifyAndFinish(String clientFinalMessage) {
        Map<String, String> attrs = parseAttrs(clientFinalMessage);
        String channelBinding = attrs.get("c");
        String r = attrs.get("r");
        String p = attrs.get("p");
        if (channelBinding == null || r == null || p == null || !r.equals(rnonce)) {
            return null;
        }
        String clientFinalMessageWithoutProof = "c=" + channelBinding + ",r=" + r;
        String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

        byte[] clientKey = hmac(saltedPassword, "Client Key");
        byte[] storedKey = sha256(clientKey);
        byte[] clientSignature = hmac(storedKey, authMessage);
        byte[] clientProof = B64D.decode(p);
        byte[] recoveredClientKey = xor(clientProof, clientSignature);
        if (!MessageDigest.isEqual(sha256(recoveredClientKey), storedKey)) {
            return null;
        }

        byte[] serverKey = hmac(saltedPassword, "Server Key");
        byte[] serverSignature = hmac(serverKey, authMessage);
        return "v=" + B64.encodeToString(serverSignature);
    }

    private static byte[] randomBytes(int n) {
        byte[] b = new byte[n];
        RANDOM.nextBytes(b);
        return b;
    }

    private static String stripGs2Header(String clientFirstMessage) {
        // "n,,n=user,r=nonce" -- gs2-header is everything up to and including the second comma
        // (no channel binding, no authzid; that's the only GS2 header every current MongoDB
        // driver ever sends -- channel binding is not offered by mongo-java-driver).
        int firstComma = clientFirstMessage.indexOf(',');
        int secondComma = clientFirstMessage.indexOf(',', firstComma + 1);
        if (firstComma < 0 || secondComma < 0) {
            throw new IllegalArgumentException("malformed SCRAM client-first-message: no gs2-header");
        }
        return clientFirstMessage.substring(secondComma + 1);
    }

    private static Map<String, String> parseAttrs(String message) {
        Map<String, String> attrs = new HashMap<>();
        for (String part : message.split(",")) {
            int eq = part.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            attrs.put(part.substring(0, eq), part.substring(eq + 1));
        }
        if (attrs.containsKey("n")) {
            attrs.put("rawN", attrs.get("n"));
        }
        return attrs;
    }

    /** RFC 5802's {@code saslprep}-lite username unescaping: {@code =2C} -> {@code ,}, {@code =3D}
     * -> {@code =}. No other saslprep normalization is applied -- every real credential in {@link
     * CredentialStore} is a plain ASCII username, so full Unicode normalization has no observable
     * effect here. */
    private static String unescapeUsername(String n) {
        if (n == null) {
            return null;
        }
        return n.replace("=2C", ",").replace("=3D", "=");
    }

    private static byte[] hi(byte[] password, byte[] salt, int iterations) {
        try {
            javax.crypto.SecretKeyFactory f = javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            char[] pwChars = new String(password, StandardCharsets.UTF_8).toCharArray();
            PBEKeySpec spec = new PBEKeySpec(pwChars, salt, iterations, 256);
            return f.generateSecret(spec).getEncoded();
        } catch (Exception e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA256 unavailable", e);
        }
    }

    private static byte[] hmac(byte[] key, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            throw new IllegalStateException("HmacSHA256 unavailable", e);
        }
    }

    private static byte[] sha256(byte[] data) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static byte[] xor(byte[] a, byte[] b) {
        byte[] out = new byte[a.length];
        for (int i = 0; i < a.length; i++) {
            out[i] = (byte) (a[i] ^ b[i]);
        }
        return out;
    }
}

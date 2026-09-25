package com.sayonora.wire.license;

import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;

/**
 * Offline tool for minting a {@code WARP_LICENSE_KEY} -- run this on a laptop that holds the
 * Enterprise signing private key, never inside a running Warp process (which only ever sees
 * the public key, in {@link License#current()}'s verification path). Not wired into {@code
 * Main}'s startup in any way; this is a standalone {@code main()} for whoever issues licenses.
 *
 * <p>Usage:
 * <pre>
 * java -cp sayonora-wire.jar com.sayonora.wire.license.LicenseKeyGenTool \
 *     --private-key &lt;base64 PKCS8 Ed25519 private key&gt; \
 *     --tier ENTERPRISE \
 *     --licensed-to "Acme Corp" \
 *     [--expires 2027-01-01T00:00:00Z]   # omit for a perpetual license
 * </pre>
 * Prints the {@code WARP_LICENSE_KEY} value to hand to the customer -- nothing else, so it's
 * safe to pipe directly into a secrets manager or a customer email.
 */
public final class LicenseKeyGenTool {

    public static void main(String[] args) throws Exception {
        String privateKeyB64 = null;
        String tier = "ENTERPRISE";
        String licensedTo = null;
        String expires = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--private-key" -> privateKeyB64 = args[++i];
                case "--tier" -> tier = args[++i];
                case "--licensed-to" -> licensedTo = args[++i];
                case "--expires" -> expires = args[++i];
                default -> throw new IllegalArgumentException("unknown argument: " + args[i]);
            }
        }
        if (privateKeyB64 == null || licensedTo == null) {
            System.err.println("usage: LicenseKeyGenTool --private-key <base64> --tier ENTERPRISE "
                    + "--licensed-to \"Customer Name\" [--expires 2027-01-01T00:00:00Z]");
            System.exit(1);
            return;
        }
        // Re-validates the tier name against the real enum up front -- a typo here should fail
        // loudly at generation time, not silently produce a key License.verify() later rejects.
        LicenseTier.valueOf(tier);

        StringBuilder json = new StringBuilder("{\"tier\":\"").append(tier).append("\",\"licensedTo\":\"")
                .append(licensedTo.replace("\"", "\\\"")).append("\"");
        if (expires != null) {
            json.append(",\"expiresAt\":\"").append(expires).append("\"");
        }
        json.append("}");
        byte[] payloadBytes = json.toString().getBytes(StandardCharsets.UTF_8);

        PrivateKey privateKey = KeyFactory.getInstance("Ed25519")
                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(privateKeyB64)));
        Signature signer = Signature.getInstance("Ed25519");
        signer.initSign(privateKey);
        signer.update(payloadBytes);
        byte[] signatureBytes = signer.sign();

        String key = Base64.getUrlEncoder().withoutPadding().encodeToString(payloadBytes)
                + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signatureBytes);
        System.out.println(key);
    }

    private LicenseKeyGenTool() {
    }
}

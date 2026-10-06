package com.sayonora.warp.config;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.sayonora.warp.secrets.FieldCipher;
import com.sayonora.warp.server.ServerOptions;
import com.sayonora.warp.tls.acme.AcmeDb;
import com.sayonora.warp.xa.XaRecoveryLog;
import java.sql.SQLException;
import java.util.Map;

/**
 * Where Warp's stored secrets stand and moving them to the active encryption key. Rotation is: start Warp with the new key as
 * {@code SAYONORA_ENCRYPTION_KEY} and the old one in {@code SAYONORA_ENCRYPTION_KEY_PREVIOUS}, call {@code POST /api/security/encryption/rotate},
 * check {@code GET /api/security/encryption} reports nothing under the old key, then drop the old key. Covers the three secret fields of the
 * latest {@code warp_config} version, the MCP upstream tokens inside it, the XA recovery log's backend passwords and the ACME state. Older
 * {@code warp_config} versions are immutable history and keep the key they were written with, so the old key is still needed to read them
 * (for example to roll back to one).
 */
public final class SecretsRotation {

    private SecretsRotation() {
    }

    public static JsonObject status(ConfigStore store, XaRecoveryLog xa, ServerOptions options) throws SQLException {
        JsonObject out = new JsonObject();
        out.addProperty("encryptionEnabled", FieldCipher.enabled());
        out.addProperty("requireKey", FieldCipher.requireKey());
        out.addProperty("activeKeyId", FieldCipher.activeKeyId());
        JsonArray previous = new JsonArray();
        FieldCipher.previousKeyIds().forEach(previous::add);
        out.add("previousKeyIds", previous);
        boolean needs = false;
        JsonObject config = new JsonObject();
        for (Map.Entry<String, Object> e : store.secretProtection().entrySet()) {
            if (e.getValue() instanceof Map<?, ?> counts) {
                JsonObject c = new JsonObject();
                counts.forEach((k, v) -> c.addProperty(String.valueOf(k), (Integer) v));
                config.add(e.getKey(), c);
                needs |= notCurrent(counts.keySet());
            } else {
                config.addProperty(e.getKey(), String.valueOf(e.getValue()));
                needs |= notCurrent(java.util.List.of(String.valueOf(e.getValue())));
            }
        }
        out.add("latestConfig", config);
        if (xa != null) {
            Map<String, Integer> counts = xa.passwordProtection();
            out.add("xaLogPasswords", toJson(counts));
            needs |= notCurrent(counts.keySet());
        }
        Map<String, Integer> acme = AcmeDb.protection(options);
        out.add("acmeState", toJson(acme));
        needs |= notCurrent(acme.keySet());
        out.addProperty("needsRotation", needs && FieldCipher.enabled());
        return out;
    }

    /** Re-encrypts everything under the active key and returns what changed plus the status afterwards. */
    public static JsonObject rotate(ConfigStore store, XaRecoveryLog xa, ServerOptions options) throws SQLException {
        if (!FieldCipher.enabled()) {
            throw new IllegalStateException("SAYONORA_ENCRYPTION_KEY is not set: there is no key to encrypt with");
        }
        JsonObject out = new JsonObject();
        var version = store.reencryptLatest();
        out.addProperty("newConfigVersion", version.isPresent() ? version.getAsLong() : null);
        out.addProperty("xaLogRowsReencrypted", xa == null ? 0 : xa.reencryptPasswords());
        out.addProperty("acmeRowsReencrypted", AcmeDb.reencryptAll(options));
        out.add("status", status(store, xa, options));
        return out;
    }

    private static boolean notCurrent(java.util.Collection<?> labels) {
        for (Object l : labels) {
            String s = String.valueOf(l);
            if (!s.equals("empty") && !s.equals(FieldCipher.activeKeyId())) {
                return true;
            }
        }
        return false;
    }

    private static JsonObject toJson(Map<String, Integer> counts) {
        JsonObject o = new JsonObject();
        counts.forEach(o::addProperty);
        return o;
    }
}

package com.sayonora.warp.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS KMS on Postgres (the {@code awsparams} store), JSON 1.1. An emulator, not an HSM: key material is AES-256-GCM sealed
 * under a master key held by the Warp process ({@code WARP_KMS_MASTER_KEY}; the service fails closed with
 * {@code KMSInternalException} when it is unset unless {@code WARP_KMS_INSECURE_DEV_KEY=true}, which uses a fixed public
 * key and is for development only). See {@link KmsCrypto} for the envelope and the algorithm list.
 *
 * <p>Keys (with their grants and import tokens) live on the host owning hash(key id); aliases on the first host. Key policies,
 * grants and rotation flags are stored and returned but never evaluated: any caller may use any key.
 * Supported key specs: SYMMETRIC_DEFAULT, RSA_2048/3072/4096, ECC_NIST_P256/P384/P521, ECC_SECG_P256K1 (BouncyCastle),
 * ECC_NIST_EDWARDS25519, HMAC_224/256/384/512, ML_DSA_44/65/87 (needs Java 24+) and SM2 (signing only; real KMS offers it in the
 * China regions, the emulator in every region).
 */
public final class KmsService extends AwsService {

    private static final Logger log = LoggerFactory.getLogger(KmsService.class);
    private static final Pattern ALIAS = Pattern.compile("^alias/[a-zA-Z0-9:/_-]+$");
    private static final Set<String> SYMMETRIC = Set.of("SYMMETRIC_DEFAULT");
    private static final Set<String> HMAC = Set.of("HMAC_224", "HMAC_256", "HMAC_384", "HMAC_512");
    private static final Set<String> ALL_SPECS = Set.of("SYMMETRIC_DEFAULT", "RSA_2048", "RSA_3072", "RSA_4096", "ECC_NIST_P256",
            "ECC_NIST_P384", "ECC_NIST_P521", "ECC_SECG_P256K1", "ECC_NIST_EDWARDS25519", "HMAC_224", "HMAC_256", "HMAC_384",
            "HMAC_512", "ML_DSA_44", "ML_DSA_65", "ML_DSA_87", "SM2");

    private final AwsRuntime rt;
    private final AwsShards sh;
    private volatile byte[] master;
    private final ConcurrentHashMap<String, Cached> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kmswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    private record Cached(Key key, long expires) {
    }

    /** A key row. {@code material} is the sealed secret (symmetric/HMAC key bytes or PKCS#8 private key). */
    record Key(String id, String arn, String spec, String usage, String origin, String state, String description, String policy,
            Map<String, String> tags, Timestamp created, Timestamp deletionDate, boolean rotationOn, int rotationDays,
            JsonArray rotations, String keyManager, boolean multiRegion, byte[] material, byte[] publicKey, String expirationModel,
            Timestamp validTo) {

        boolean enabled() {
            return state.equals("Enabled");
        }

        boolean asymmetric() {
            return spec.startsWith("RSA_") || spec.startsWith("ECC_") || spec.startsWith("ML_DSA") || spec.equals("SM2");
        }
    }

    public KmsService(AwsRuntime rt) {
        this.rt = rt;
        this.sh = new AwsShards(rt.registry, StoreType.AWSPARAMS);
    }

    @Override
    public String id() {
        return "kms";
    }

    @Override
    public String metricsProtocol() {
        return "kmswire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("kms");
    }

    @Override
    public Set<String> targetPrefixes() {
        return Set.of("TrentService.");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return switch (op) {
            case "Encrypt", "Decrypt", "ReEncrypt", "Sign", "Verify", "GenerateRandom", "GenerateDataKey",
                    "GenerateDataKeyWithoutPlaintext", "GenerateDataKeyPair", "GenerateDataKeyPairWithoutPlaintext", "GenerateMac",
                    "VerifyMac", "GetPublicKey", "DescribeKey", "ListKeys", "ListAliases", "GetKeyPolicy", "ListKeyPolicies",
                    "ListResourceTags", "ListGrants", "ListRetirableGrants", "GetKeyRotationStatus", "ListKeyRotations" ->
                    SqlMetricsCollector.StatementKind.READ;
            default -> SqlMetricsCollector.StatementKind.WRITE;
        };
    }

    @Override
    public String backendLabel(String op, JsonObject req) {
        try {
            String k = req == null ? null : Args.str(req, "KeyId");
            if (k == null || k.startsWith("alias/") || !sh.available()) {
                return "default";
            }
            return sh.owner(keyIdOf(k));
        } catch (RuntimeException e) {
            return "default";
        }
    }

    @Override
    public void start() {
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (!sh.available()) {
                    return;
                }
                for (String h : sh.hosts()) {
                    List<String> gone = sh.conn(h, c -> {
                        List<String> ids = new ArrayList<>();
                        try (var st = c.createStatement(); ResultSet rs = st.executeQuery(
                                "DELETE FROM warp_awsparams_kms_keys WHERE state = 'PendingDeletion' AND deletion_date < now() RETURNING key_id")) {
                            while (rs.next()) {
                                ids.add(rs.getString(1));
                            }
                        }
                        return ids;
                    });
                    if (!gone.isEmpty()) {
                        sh.conn(sh.home(), c -> {
                            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_kms_aliases WHERE key_id = ANY (?)")) {
                                ps.setArray(1, c.createArrayOf("text", gone.toArray()));
                                ps.executeUpdate();
                            }
                            return null;
                        });
                        gone.forEach(cache::remove);
                    }
                }
            } catch (RuntimeException e) {
                log.debug("kmswire sweep failed: {}", e.getMessage());
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        sweeper.shutdownNow();
    }

    // ---------------------------------------------------------------------------------------------- errors / helpers

    private static AwsException notFound(String msg) {
        return new AwsException(400, "NotFoundException", msg);
    }

    private static AwsException invalidState(String msg) {
        return new AwsException(400, "KMSInvalidStateException", msg);
    }

    private static AwsException disabled(String arn) {
        return new AwsException(400, "DisabledException", arn + " is disabled.");
    }

    private static AwsException usage(String msg) {
        return new AwsException(400, "InvalidKeyUsageException", msg);
    }

    private static AwsException validation(String msg) {
        return new AwsException(400, "ValidationException", msg);
    }

    private static AwsException unsupported(String msg) {
        return new AwsException(400, "UnsupportedOperationException", msg);
    }

    private byte[] masterKey() {
        byte[] m = master;
        if (m != null) {
            return m;
        }
        synchronized (this) {
            if (master == null) {
                if (rt.config.kmsMasterKey != null) {
                    master = KmsCrypto.deriveMaster(rt.config.kmsMasterKey);
                } else if (rt.config.kmsInsecureDevKey) {
                    log.warn("kmswire: WARP_KMS_INSECURE_DEV_KEY=true -- KMS key material is sealed under a FIXED PUBLIC key; "
                            + "development only, never use for real secrets");
                    master = KmsCrypto.deriveMaster("warp-insecure-development-key-do-not-use");
                } else {
                    throw new AwsException(500, "KMSInternalException", "KMS is not configured: set WARP_KMS_MASTER_KEY to a secret "
                            + "string (or WARP_KMS_INSECURE_DEV_KEY=true for development). Warp refuses to create or use key material "
                            + "without a master key.");
                }
            }
            return master;
        }
    }

    /** Fails closed with the KMS configuration error when no master key is available. */
    void requireConfigured() {
        masterKey();
    }

    private static double epoch(Timestamp t) {
        return t == null ? 0 : t.getTime() / 1000.0;
    }

    /** The key id inside a key id / key ARN (aliases are resolved elsewhere). */
    private static String keyIdOf(String ref) {
        if (ref.startsWith("arn:")) {
            int i = ref.indexOf(":key/");
            if (i >= 0) {
                return ref.substring(i + 5);
            }
        }
        return ref;
    }

    private String keyArn(String id) {
        return rt.config.arn("kms", "key/" + id);
    }

    private String aliasArn(String name) {
        return rt.config.arn("kms", name);
    }

    static String defaultPolicy(String account) {
        return "{\"Version\":\"2012-10-17\",\"Id\":\"key-default-1\",\"Statement\":[{\"Sid\":\"Enable IAM User Permissions\","
                + "\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"arn:aws:iam::" + account + ":root\"},\"Action\":\"kms:*\","
                + "\"Resource\":\"*\"}]}";
    }

    private static final String KEY_COLS = "key_id, arn, spec, usage, origin, state, description, policy, tags::text, created_at, "
            + "deletion_date, rotation_on, rotation_days, rotations::text, key_manager, multi_region, material, public_key, "
            + "expiration_model, valid_to";

    private static Key readKey(ResultSet rs) throws SQLException {
        Map<String, String> tags = new TreeMap<>();
        JsonParser.parseString(rs.getString(9)).getAsJsonObject().entrySet().forEach(e -> tags.put(e.getKey(), e.getValue().getAsString()));
        return new Key(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6),
                rs.getString(7), rs.getString(8), tags, rs.getTimestamp(10), rs.getTimestamp(11), rs.getBoolean(12), rs.getInt(13),
                JsonParser.parseString(rs.getString(14)).getAsJsonArray(), rs.getString(15), rs.getBoolean(16), rs.getBytes(17),
                rs.getBytes(18), rs.getString(19), rs.getTimestamp(20));
    }

    private Key loadKey(String id, boolean forUpdate, Connection c) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + KEY_COLS + " FROM warp_awsparams_kms_keys WHERE key_id = ?"
                + (forUpdate ? " FOR UPDATE" : ""))) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readKey(rs) : null;
            }
        }
    }

    /** Resolves a key id, key ARN, alias name or alias ARN to the key row; throws NotFoundException. */
    Key resolve(String ref) {
        if (ref == null || ref.isEmpty()) {
            throw validation("1 validation error detected: Value null at 'keyId' failed to satisfy constraint: Member must not be null");
        }
        String id;
        String aliasName = null;
        if (ref.startsWith("alias/")) {
            aliasName = ref;
        } else if (ref.startsWith("arn:") && ref.contains(":alias/")) {
            aliasName = ref.substring(ref.indexOf(":alias/") + 1);
        }
        if (aliasName != null) {
            String an = aliasName;
            id = sh.conn(sh.home(), c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT key_id FROM warp_awsparams_kms_aliases WHERE name = ?")) {
                    ps.setString(1, an);
                    try (ResultSet rs = ps.executeQuery()) {
                        return rs.next() ? rs.getString(1) : null;
                    }
                }
            });
            if (id == null) {
                throw notFound("Alias " + aliasArn(aliasName) + " is not found.");
            }
        } else {
            id = keyIdOf(ref);
        }
        Cached hit = cache.get(id);
        if (hit != null && hit.expires > System.currentTimeMillis()) {
            return hit.key;
        }
        String kid = id;
        Key k = sh.conn(sh.owner(id), c -> loadKey(kid, false, c));
        if (k == null) {
            throw notFound("Key '" + (ref.startsWith("arn:") ? ref : keyArn(id)) + "' does not exist");
        }
        cache.put(id, new Cached(k, System.currentTimeMillis() + 2000));
        return k;
    }

    private void forget(String id) {
        cache.remove(id);
    }

    private Key usable(String ref) {
        Key k = resolve(ref);
        switch (k.state) {
            case "PendingDeletion" -> throw invalidState(k.arn + " is pending deletion.");
            case "PendingImport" -> throw invalidState(k.arn + " is pending import.");
            case "Disabled" -> throw disabled(k.arn);
            default -> {
            }
        }
        return k;
    }

    private byte[] secret(Key k) {
        if (k.material == null) {
            throw invalidState(k.arn + " has no key material.");
        }
        return KmsCrypto.unwrap(masterKey(), k.id, k.material);
    }

    private static Map<String, String> context(JsonObject req, String field) {
        Map<String, String> m = new TreeMap<>();
        JsonObject o = Args.obj(req, field);
        if (o != null) {
            o.entrySet().forEach(e -> m.put(e.getKey(), e.getValue().getAsString()));
        }
        return m;
    }

    // ---------------------------------------------------------------------------------------------- dispatch

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        return switch (op) {
            case "CreateKey" -> createKey(req);
            case "DescribeKey" -> describeKey(req);
            case "ListKeys" -> listKeys(req);
            case "EnableKey" -> setState(req, "Enabled");
            case "DisableKey" -> setState(req, "Disabled");
            case "ScheduleKeyDeletion" -> scheduleDeletion(req);
            case "CancelKeyDeletion" -> cancelDeletion(req);
            case "UpdateKeyDescription" -> updateDescription(req);
            case "CreateAlias" -> createAlias(req);
            case "DeleteAlias" -> deleteAlias(req);
            case "UpdateAlias" -> updateAlias(req);
            case "ListAliases" -> listAliases(req);
            case "Encrypt" -> encrypt(req);
            case "Decrypt" -> decrypt(req);
            case "ReEncrypt" -> reEncrypt(req);
            case "GenerateDataKey" -> generateDataKey(req, true);
            case "GenerateDataKeyWithoutPlaintext" -> generateDataKey(req, false);
            case "GenerateDataKeyPair" -> generateDataKeyPair(req, true);
            case "GenerateDataKeyPairWithoutPlaintext" -> generateDataKeyPair(req, false);
            case "GenerateRandom" -> generateRandom(req);
            case "Sign" -> sign(req);
            case "Verify" -> verify(req);
            case "GetPublicKey" -> getPublicKey(req);
            case "GenerateMac" -> generateMac(req);
            case "VerifyMac" -> verifyMac(req);
            case "GetKeyPolicy" -> getKeyPolicy(req);
            case "PutKeyPolicy" -> putKeyPolicy(req);
            case "ListKeyPolicies" -> listKeyPolicies(req);
            case "TagResource" -> tagResource(req);
            case "UntagResource" -> untagResource(req);
            case "ListResourceTags" -> listResourceTags(req);
            case "EnableKeyRotation" -> rotationFlag(req, true);
            case "DisableKeyRotation" -> rotationFlag(req, false);
            case "GetKeyRotationStatus" -> rotationStatus(req);
            case "RotateKeyOnDemand" -> rotateOnDemand(req);
            case "ListKeyRotations" -> listRotations(req);
            case "CreateGrant" -> createGrant(req);
            case "ListGrants" -> listGrants(req, false);
            case "ListRetirableGrants" -> listGrants(req, true);
            case "RevokeGrant" -> revokeGrant(req);
            case "RetireGrant" -> retireGrant(req);
            case "GetParametersForImport" -> getParametersForImport(req);
            case "ImportKeyMaterial" -> importKeyMaterial(req);
            case "DeleteImportedKeyMaterial" -> deleteImportedKeyMaterial(req);
            case "ConnectCustomKeyStore", "CreateCustomKeyStore", "DescribeCustomKeyStores", "UpdateCustomKeyStore",
                    "DeleteCustomKeyStore", "DisconnectCustomKeyStore", "ReplicateKey", "UpdatePrimaryRegion", "DeriveSharedSecret" ->
                    throw unsupported(op + " is not supported by Warp's KMS emulation");
            default -> throw new AwsException(400, "UnknownOperationException", "Unknown operation " + op);
        };
    }

    // ---------------------------------------------------------------------------------------------- keys

    private static String defaultUsage(String spec) {
        if (HMAC.contains(spec)) {
            return "GENERATE_VERIFY_MAC";
        }
        return spec.equals("SYMMETRIC_DEFAULT") ? "ENCRYPT_DECRYPT" : "SIGN_VERIFY";
    }

    private static boolean usageOk(String spec, String usage) {
        return switch (usage) {
            case "ENCRYPT_DECRYPT" -> spec.equals("SYMMETRIC_DEFAULT") || spec.startsWith("RSA_");
            case "SIGN_VERIFY" -> spec.startsWith("RSA_") || spec.startsWith("ECC_") || spec.startsWith("ML_DSA") || spec.equals("SM2");
            case "GENERATE_VERIFY_MAC" -> HMAC.contains(spec);
            case "KEY_AGREEMENT" -> spec.startsWith("ECC_NIST_P");
            default -> false;
        };
    }

    private JsonObject createKey(JsonObject req) {
        String spec = Args.str(req, "KeySpec");
        if (spec == null) {
            spec = Args.str(req, "CustomerMasterKeySpec");
        }
        if (spec == null) {
            spec = "SYMMETRIC_DEFAULT";
        }
        if (!ALL_SPECS.contains(spec)) {
            throw validation("1 validation error detected: Value '" + spec + "' at 'keySpec' failed to satisfy constraint: Member must "
                    + "satisfy enum value set");
        }
        String usage = Args.str(req, "KeyUsage") == null ? defaultUsage(spec) : Args.str(req, "KeyUsage");
        if (!usageOk(spec, usage)) {
            throw validation("KeyUsage " + usage + " is not compatible with KeySpec " + spec);
        }
        String origin = Args.str(req, "Origin") == null ? "AWS_KMS" : Args.str(req, "Origin");
        if (!origin.equals("AWS_KMS") && !origin.equals("EXTERNAL")) {
            throw unsupported("Origin " + origin + " is not supported by Warp's KMS emulation");
        }
        boolean external = origin.equals("EXTERNAL");
        if (external && !spec.equals("SYMMETRIC_DEFAULT")) {
            throw validation("Only SYMMETRIC_DEFAULT keys can have Origin EXTERNAL in this emulation");
        }
        boolean multi = Boolean.TRUE.equals(Args.bool(req, "MultiRegion"));
        Map<String, String> tags = new TreeMap<>();
        for (JsonObject t : Args.objects(req, "Tags")) {
            tags.put(Args.str(t, "TagKey"), Args.str(t, "TagValue"));
        }
        String policy = Args.str(req, "Policy");
        if (policy != null) {
            try {
                JsonParser.parseString(policy).getAsJsonObject();
            } catch (RuntimeException e) {
                throw new AwsException(400, "MalformedPolicyDocumentException", "The provided policy document is malformed.");
            }
        }
        return createKeyRow(spec, usage, external, multi, Args.str(req, "Description"), policy == null ? defaultPolicy(rt.config.accountId)
                : policy, tags, "CUSTOMER");
    }

    private JsonObject createKeyRow(String spec, String usage, boolean external, boolean multi, String description, String policy,
            Map<String, String> tags, String manager) {
        byte[] m = masterKey();
        String id = multi ? "mrk-" + UUID.randomUUID().toString().replace("-", "") : UUID.randomUUID().toString();
        byte[] material = null;
        byte[] pub = null;
        if (!external) {
            if (spec.equals("SYMMETRIC_DEFAULT")) {
                material = KmsCrypto.random(32);
            } else if (HMAC.contains(spec)) {
                material = KmsCrypto.random(Integer.parseInt(spec.substring(5)) / 8);
            } else {
                try {
                    KmsCrypto.Generated g = KmsCrypto.generatePair(spec);
                    material = g.privateKey();
                    pub = g.publicKey();
                } catch (IllegalStateException e) {
                    throw unsupported(e.getMessage());
                }
            }
            material = KmsCrypto.wrap(m, id, material);
        }
        JsonObject t = new JsonObject();
        tags.forEach(t::addProperty);
        String state = external ? "PendingImport" : "Enabled";
        byte[] mat = material;
        byte[] pk = pub;
        Key k = sh.tx(sh.owner(id), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_kms_keys (key_id, arn, spec, usage, origin, state, "
                    + "description, policy, tags, key_manager, multi_region, material, public_key, expiration_model) VALUES "
                    + "(?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)")) {
                ps.setString(1, id);
                ps.setString(2, keyArn(id));
                ps.setString(3, spec);
                ps.setString(4, usage);
                ps.setString(5, external ? "EXTERNAL" : "AWS_KMS");
                ps.setString(6, state);
                ps.setString(7, description == null ? "" : description);
                ps.setString(8, policy);
                ps.setString(9, t.toString());
                ps.setString(10, manager);
                ps.setBoolean(11, multi);
                ps.setBytes(12, mat);
                ps.setBytes(13, pk);
                ps.setString(14, external ? "KEY_MATERIAL_DOES_NOT_EXPIRE" : null);
                ps.executeUpdate();
            }
            return loadKey(id, false, c);
        });
        JsonObject out = new JsonObject();
        out.add("KeyMetadata", metadata(k));
        return out;
    }

    private static JsonArray arr(String... v) {
        JsonArray a = new JsonArray();
        for (String s : v) {
            a.add(s);
        }
        return a;
    }

    private JsonObject metadata(Key k) {
        JsonObject o = new JsonObject();
        o.addProperty("AWSAccountId", rt.config.accountId);
        o.addProperty("KeyId", k.id);
        o.addProperty("Arn", k.arn);
        o.addProperty("CreationDate", epoch(k.created));
        o.addProperty("Enabled", k.enabled());
        o.addProperty("Description", k.description);
        o.addProperty("KeyUsage", k.usage);
        o.addProperty("KeyState", k.state);
        if (k.deletionDate != null) {
            o.addProperty("DeletionDate", epoch(k.deletionDate));
        }
        if (k.validTo != null) {
            o.addProperty("ValidTo", epoch(k.validTo));
        }
        o.addProperty("Origin", k.origin);
        o.addProperty("KeyManager", k.keyManager);
        o.addProperty("CustomerMasterKeySpec", k.spec);
        o.addProperty("KeySpec", k.spec);
        if (k.expirationModel != null) {
            o.addProperty("ExpirationModel", k.expirationModel);
        }
        if (k.usage.equals("ENCRYPT_DECRYPT")) {
            o.add("EncryptionAlgorithms", k.spec.equals("SYMMETRIC_DEFAULT") ? arr("SYMMETRIC_DEFAULT")
                    : arr("RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"));
        }
        if (k.usage.equals("SIGN_VERIFY")) {
            o.add("SigningAlgorithms", arr(KmsCrypto.signingAlgorithms(k.spec)));
        }
        if (k.usage.equals("GENERATE_VERIFY_MAC")) {
            o.add("MacAlgorithms", arr("HMAC_SHA_" + k.spec.substring(5)));
        }
        o.addProperty("MultiRegion", k.multiRegion);
        if (k.state.equals("PendingDeletion") && k.deletionDate != null) {
            long days = Math.max(1, Math.round((k.deletionDate.getTime() - System.currentTimeMillis()) / 86400000.0));
            o.addProperty("PendingDeletionWindowInDays", days);
        }
        return o;
    }

    private JsonObject describeKey(JsonObject req) {
        JsonObject out = new JsonObject();
        out.add("KeyMetadata", metadata(resolve(Args.str(req, "KeyId"))));
        return out;
    }

    private JsonObject listKeys(JsonObject req) {
        Integer limit = Args.integer(req, "Limit");
        String marker = Args.str(req, "Marker");
        TreeMap<String, String> all = new TreeMap<>();
        for (List<String> l : sh.<List<String>>onAll(c -> {
            List<String> r = new ArrayList<>();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT key_id FROM warp_awsparams_kms_keys")) {
                while (rs.next()) {
                    r.add(rs.getString(1));
                }
            }
            return r;
        })) {
            l.forEach(id -> all.put(id, keyArn(id)));
        }
        int max = limit == null ? 100 : Math.max(1, Math.min(limit, 1000));
        JsonArray keys = new JsonArray();
        boolean more = false;
        String last = null;
        for (var e : all.entrySet()) {
            if (marker != null && e.getKey().compareTo(marker) <= 0) {
                continue;
            }
            if (keys.size() >= max) {
                more = true;
                break;
            }
            JsonObject k = new JsonObject();
            k.addProperty("KeyId", e.getKey());
            k.addProperty("KeyArn", e.getValue());
            keys.add(k);
            last = e.getKey();
        }
        JsonObject out = new JsonObject();
        out.add("Keys", keys);
        out.addProperty("Truncated", more);
        if (more) {
            out.addProperty("NextMarker", last);
        }
        return out;
    }

    private JsonObject setState(JsonObject req, String target) {
        Key k = resolve(Args.str(req, "KeyId"));
        sh.tx(sh.owner(k.id), c -> {
            Key cur = loadKey(k.id, true, c);
            if (cur.state.equals("PendingDeletion") || cur.state.equals("PendingImport")) {
                throw invalidState(cur.arn + " is " + (cur.state.equals("PendingDeletion") ? "pending deletion." : "pending import."));
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET state = ? WHERE key_id = ?")) {
                ps.setString(1, target);
                ps.setString(2, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        return new JsonObject();
    }

    private JsonObject scheduleDeletion(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        Integer days = Args.integer(req, "PendingWindowInDays");
        int window = days == null ? 30 : days;
        if (window < 7 || window > 30) {
            throw validation("1 validation error detected: Value '" + window + "' at 'pendingWindowInDays' failed to satisfy constraint: "
                    + "Member must have value greater than or equal to 7");
        }
        Timestamp when = Timestamp.from(Instant.now().plusSeconds(window * 86400L));
        sh.tx(sh.owner(k.id), c -> {
            Key cur = loadKey(k.id, true, c);
            if (cur.state.equals("PendingDeletion")) {
                throw invalidState(cur.arn + " is pending deletion.");
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET state = 'PendingDeletion', deletion_date = ? "
                    + "WHERE key_id = ?")) {
                ps.setTimestamp(1, when);
                ps.setString(2, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        JsonObject out = new JsonObject();
        out.addProperty("KeyId", k.arn);
        out.addProperty("DeletionDate", epoch(when));
        out.addProperty("KeyState", "PendingDeletion");
        out.addProperty("PendingWindowInDays", window);
        return out;
    }

    private JsonObject cancelDeletion(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        sh.tx(sh.owner(k.id), c -> {
            Key cur = loadKey(k.id, true, c);
            if (!cur.state.equals("PendingDeletion")) {
                throw invalidState(cur.arn + " is not pending deletion.");
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET state = 'Disabled', deletion_date = NULL WHERE key_id = ?")) {
                ps.setString(1, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        JsonObject out = new JsonObject();
        out.addProperty("KeyId", k.arn);
        return out;
    }

    private JsonObject updateDescription(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        String d = Args.str(req, "Description") == null ? "" : Args.str(req, "Description");
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET description = ? WHERE key_id = ?")) {
                ps.setString(1, d);
                ps.setString(2, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        return new JsonObject();
    }

    // ---------------------------------------------------------------------------------------------- aliases

    private String checkAliasName(String name) {
        if (name == null || !ALIAS.matcher(name).matches()) {
            throw new AwsException(400, "InvalidAliasNameException", "1 validation error detected: Value '" + name + "' at 'aliasName' "
                    + "failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9/_-]+$");
        }
        return name;
    }

    private JsonObject createAlias(JsonObject req) {
        String name = checkAliasName(Args.str(req, "AliasName"));
        if (name.startsWith("alias/aws/")) {
            throw new AwsException(400, "InvalidAliasNameException", "Aliases that begin with alias/aws are reserved.");
        }
        Key k = resolve(Args.str(req, "TargetKeyId"));
        if (k.state.equals("PendingDeletion")) {
            throw invalidState(k.arn + " is pending deletion.");
        }
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_kms_aliases (name, key_id) VALUES (?, ?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, name);
                ps.setString(2, k.id);
                if (ps.executeUpdate() == 0) {
                    throw new AwsException(400, "AlreadyExistsException", "An alias with the name " + aliasArn(name) + " already exists");
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject deleteAlias(JsonObject req) {
        String name = checkAliasName(Args.str(req, "AliasName"));
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_kms_aliases WHERE name = ?")) {
                ps.setString(1, name);
                if (ps.executeUpdate() == 0) {
                    throw notFound("Alias " + aliasArn(name) + " is not found.");
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject updateAlias(JsonObject req) {
        String name = checkAliasName(Args.str(req, "AliasName"));
        Key k = resolve(Args.str(req, "TargetKeyId"));
        sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_aliases SET key_id = ?, updated_at = now() WHERE name = ?")) {
                ps.setString(1, k.id);
                ps.setString(2, name);
                if (ps.executeUpdate() == 0) {
                    throw notFound("Alias " + aliasArn(name) + " is not found.");
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject listAliases(JsonObject req) {
        String keyFilter = Args.str(req, "KeyId") == null ? null : resolve(Args.str(req, "KeyId")).id;
        Integer limit = Args.integer(req, "Limit");
        String marker = Args.str(req, "Marker");
        int max = limit == null ? 100 : Math.max(1, Math.min(limit, 100));
        return sh.conn(sh.home(), c -> {
            JsonArray a = new JsonArray();
            boolean more = false;
            String last = null;
            try (PreparedStatement ps = c.prepareStatement("SELECT name, key_id, created_at, updated_at FROM warp_awsparams_kms_aliases "
                    + "WHERE (?::text IS NULL OR key_id = ?) AND (?::text IS NULL OR name > ?) ORDER BY name")) {
                ps.setString(1, keyFilter);
                ps.setString(2, keyFilter);
                ps.setString(3, marker);
                ps.setString(4, marker);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        if (a.size() >= max) {
                            more = true;
                            break;
                        }
                        JsonObject o = new JsonObject();
                        o.addProperty("AliasName", rs.getString(1));
                        o.addProperty("AliasArn", aliasArn(rs.getString(1)));
                        o.addProperty("TargetKeyId", rs.getString(2));
                        o.addProperty("CreationDate", epoch(rs.getTimestamp(3)));
                        o.addProperty("LastUpdatedDate", epoch(rs.getTimestamp(4)));
                        a.add(o);
                        last = rs.getString(1);
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("Aliases", a);
            out.addProperty("Truncated", more);
            if (more) {
                out.addProperty("NextMarker", last);
            }
            return out;
        });
    }

    /** Ensures an AWS-managed key behind {@code alias/aws/<service>} exists and returns its key id. */
    String awsManagedKey(String aliasName, String description) {
        masterKey();
        try {
            return resolve(aliasName).id;
        } catch (AwsException notThere) {
            if (!notThere.code.equals("NotFoundException")) {
                throw notThere;
            }
        }
        JsonObject created = createKeyRow("SYMMETRIC_DEFAULT", "ENCRYPT_DECRYPT", false, false, description,
                defaultPolicy(rt.config.accountId), new TreeMap<>(), "AWS");
        String id = created.getAsJsonObject("KeyMetadata").get("KeyId").getAsString();
        String winner = sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_kms_aliases (name, key_id) VALUES (?, ?) "
                    + "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING key_id")) {
                ps.setString(1, aliasName);
                ps.setString(2, id);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    return rs.getString(1);
                }
            }
        });
        if (!winner.equals(id)) {
            sh.conn(sh.owner(id), c -> {
                try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_kms_keys WHERE key_id = ?")) {
                    ps.setString(1, id);
                    ps.executeUpdate();
                }
                return null;
            });
        }
        return winner;
    }

    /** Symmetric encrypt for other services (Secrets Manager, SSM): returns the self-describing blob. */
    byte[] encryptFor(String keyRef, byte[] plaintext, Map<String, String> ctx) {
        Key k = usable(keyRef);
        if (!k.spec.equals("SYMMETRIC_DEFAULT") || !k.usage.equals("ENCRYPT_DECRYPT")) {
            throw usage(k.arn + " key usage is not ENCRYPT_DECRYPT with a symmetric key");
        }
        return KmsCrypto.encrypt(k.id, secret(k), plaintext, ctx);
    }

    byte[] decryptFor(byte[] blob, Map<String, String> ctx) {
        String id = KmsCrypto.keyIdOf(blob);
        if (id == null) {
            throw new AwsException(400, "InvalidCiphertextException", "Ciphertext is not valid");
        }
        Key k = usable(id);
        byte[] pt = KmsCrypto.decrypt(k.id, secret(k), blob, ctx);
        if (pt == null) {
            throw new AwsException(400, "InvalidCiphertextException", "The ciphertext or encryption context is invalid");
        }
        return pt;
    }

    /** Existence and usability check for a caller-supplied key reference (Secrets Manager's KmsKeyId). */
    boolean canUse(String keyRef) {
        try {
            usable(keyRef);
            return true;
        } catch (AwsException e) {
            return false;
        }
    }

    String arnFor(String keyRef) {
        return resolve(keyRef).arn;
    }

    // ---------------------------------------------------------------------------------------------- crypto operations

    private JsonObject encrypt(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        byte[] pt = Args.bytes(req, "Plaintext");
        if (pt == null || pt.length == 0) {
            throw validation("1 validation error detected: Value at 'plaintext' failed to satisfy constraint: Member must have length "
                    + "greater than or equal to 1");
        }
        if (pt.length > 4096) {
            throw validation("1 validation error detected: Value at 'plaintext' failed to satisfy constraint: Member must have length "
                    + "less than or equal to 4096");
        }
        if (!k.usage.equals("ENCRYPT_DECRYPT")) {
            throw usage(k.arn + " key usage is " + k.usage + ", which is not valid for Encrypt");
        }
        String alg = Args.str(req, "EncryptionAlgorithm");
        JsonObject out = new JsonObject();
        out.addProperty("KeyId", k.arn);
        if (k.spec.equals("SYMMETRIC_DEFAULT")) {
            if (alg != null && !alg.equals("SYMMETRIC_DEFAULT")) {
                throw usage("The encryption algorithm " + alg + " is incompatible with the key spec SYMMETRIC_DEFAULT");
            }
            byte[] blob = KmsCrypto.encrypt(k.id, secret(k), pt, context(req, "EncryptionContext"));
            out.addProperty("CiphertextBlob", Base64.getEncoder().encodeToString(blob));
            out.addProperty("EncryptionAlgorithm", "SYMMETRIC_DEFAULT");
        } else {
            if (alg == null || !(alg.equals("RSAES_OAEP_SHA_1") || alg.equals("RSAES_OAEP_SHA_256"))) {
                throw usage("You must specify an EncryptionAlgorithm (RSAES_OAEP_SHA_1 or RSAES_OAEP_SHA_256) for an RSA key");
            }
            byte[] blob;
            try {
                blob = KmsCrypto.rsaEncrypt(k.publicKey, alg, pt);
            } catch (IllegalArgumentException e) {
                throw validation(e.getMessage());
            }
            out.addProperty("CiphertextBlob", Base64.getEncoder().encodeToString(blob));
            out.addProperty("EncryptionAlgorithm", alg);
        }
        return out;
    }

    private JsonObject decrypt(JsonObject req) {
        byte[] blob = Args.bytes(req, "CiphertextBlob");
        if (blob == null || blob.length == 0) {
            throw validation("1 validation error detected: Value at 'ciphertextBlob' failed to satisfy constraint: Member must not be null");
        }
        String keyRef = Args.str(req, "KeyId");
        String alg = Args.str(req, "EncryptionAlgorithm");
        JsonObject out = new JsonObject();
        String embedded = KmsCrypto.keyIdOf(blob);
        Key requested = keyRef == null ? null : resolve(keyRef);
        if (requested != null && requested.asymmetric()) {
            Key k = usable(keyRef);
            if (!k.usage.equals("ENCRYPT_DECRYPT")) {
                throw usage(k.arn + " key usage is " + k.usage + ", which is not valid for Decrypt");
            }
            if (alg == null) {
                throw usage("You must specify an EncryptionAlgorithm for an RSA key");
            }
            byte[] pt = KmsCrypto.rsaDecrypt(secret(k), alg, blob);
            if (pt == null) {
                throw new AwsException(400, "InvalidCiphertextException", "The ciphertext is invalid");
            }
            out.addProperty("KeyId", k.arn);
            out.addProperty("Plaintext", Base64.getEncoder().encodeToString(pt));
            out.addProperty("EncryptionAlgorithm", alg);
            return out;
        }
        if (embedded == null) {
            throw new AwsException(400, "InvalidCiphertextException", "The ciphertext is not a valid KMS ciphertext blob");
        }
        Key k = usable(embedded);
        if (requested != null && !requested.id.equals(k.id)) {
            throw new AwsException(400, "IncorrectKeyException", "The key ID in the request does not identify a CMK that can perform this "
                    + "operation.");
        }
        byte[] pt = KmsCrypto.decrypt(k.id, secret(k), blob, context(req, "EncryptionContext"));
        if (pt == null) {
            throw new AwsException(400, "InvalidCiphertextException", "The ciphertext or encryption context is invalid");
        }
        out.addProperty("KeyId", k.arn);
        out.addProperty("Plaintext", Base64.getEncoder().encodeToString(pt));
        out.addProperty("EncryptionAlgorithm", "SYMMETRIC_DEFAULT");
        return out;
    }

    private JsonObject reEncrypt(JsonObject req) {
        byte[] blob = Args.bytes(req, "CiphertextBlob");
        String embedded = blob == null ? null : KmsCrypto.keyIdOf(blob);
        if (embedded == null) {
            throw new AwsException(400, "InvalidCiphertextException", "The ciphertext is not a valid KMS ciphertext blob");
        }
        Key src = usable(embedded);
        if (Args.str(req, "SourceKeyId") != null && !resolve(Args.str(req, "SourceKeyId")).id.equals(src.id)) {
            throw new AwsException(400, "IncorrectKeyException", "The key ID in the request does not identify a CMK that can perform this "
                    + "operation.");
        }
        byte[] pt = KmsCrypto.decrypt(src.id, secret(src), blob, context(req, "SourceEncryptionContext"));
        if (pt == null) {
            throw new AwsException(400, "InvalidCiphertextException", "The ciphertext or encryption context is invalid");
        }
        Key dst = usable(Args.str(req, "DestinationKeyId"));
        if (!dst.spec.equals("SYMMETRIC_DEFAULT") || !dst.usage.equals("ENCRYPT_DECRYPT")) {
            throw usage(dst.arn + " key usage is not valid for ReEncrypt");
        }
        byte[] out = KmsCrypto.encrypt(dst.id, secret(dst), pt, context(req, "DestinationEncryptionContext"));
        JsonObject o = new JsonObject();
        o.addProperty("CiphertextBlob", Base64.getEncoder().encodeToString(out));
        o.addProperty("SourceKeyId", src.arn);
        o.addProperty("KeyId", dst.arn);
        o.addProperty("SourceEncryptionAlgorithm", "SYMMETRIC_DEFAULT");
        o.addProperty("DestinationEncryptionAlgorithm", "SYMMETRIC_DEFAULT");
        return o;
    }

    private JsonObject generateDataKey(JsonObject req, boolean withPlaintext) {
        Key k = usable(Args.str(req, "KeyId"));
        if (!k.spec.equals("SYMMETRIC_DEFAULT") || !k.usage.equals("ENCRYPT_DECRYPT")) {
            throw usage(k.arn + " key usage is not valid for GenerateDataKey");
        }
        String spec = Args.str(req, "KeySpec");
        Integer n = Args.integer(req, "NumberOfBytes");
        int len;
        if (spec != null && n != null) {
            throw validation("KeySpec and NumberOfBytes are mutually exclusive");
        } else if (spec != null) {
            len = switch (spec) {
                case "AES_128" -> 16;
                case "AES_256" -> 32;
                default -> throw validation("Value '" + spec + "' at 'keySpec' failed to satisfy constraint: Member must satisfy enum value set: [AES_256, AES_128]");
            };
        } else if (n != null) {
            if (n < 1 || n > 1024) {
                throw validation("Value '" + n + "' at 'numberOfBytes' failed to satisfy constraint: Member must have value between 1 and 1024");
            }
            len = n;
        } else {
            throw validation("Please specify either number of bytes or key spec.");
        }
        byte[] pt = KmsCrypto.random(len);
        byte[] blob = KmsCrypto.encrypt(k.id, secret(k), pt, context(req, "EncryptionContext"));
        JsonObject o = new JsonObject();
        o.addProperty("CiphertextBlob", Base64.getEncoder().encodeToString(blob));
        if (withPlaintext) {
            o.addProperty("Plaintext", Base64.getEncoder().encodeToString(pt));
        }
        o.addProperty("KeyId", k.arn);
        return o;
    }

    private JsonObject generateDataKeyPair(JsonObject req, boolean withPlaintext) {
        Key k = usable(Args.str(req, "KeyId"));
        if (!k.spec.equals("SYMMETRIC_DEFAULT")) {
            throw usage(k.arn + " must be a symmetric key to wrap a data key pair");
        }
        String spec = Args.req(req, "KeyPairSpec");
        if (!spec.startsWith("RSA_") && !spec.startsWith("ECC_")) {
            throw validation("Invalid KeyPairSpec " + spec);
        }
        KmsCrypto.Generated g;
        try {
            g = KmsCrypto.generatePair(spec);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw unsupported(e.getMessage());
        }
        byte[] wrapped = KmsCrypto.encrypt(k.id, secret(k), g.privateKey(), context(req, "EncryptionContext"));
        JsonObject o = new JsonObject();
        o.addProperty("PrivateKeyCiphertextBlob", Base64.getEncoder().encodeToString(wrapped));
        if (withPlaintext) {
            o.addProperty("PrivateKeyPlaintext", Base64.getEncoder().encodeToString(g.privateKey()));
        }
        o.addProperty("PublicKey", Base64.getEncoder().encodeToString(g.publicKey()));
        o.addProperty("KeyId", k.arn);
        o.addProperty("KeyPairSpec", spec);
        return o;
    }

    private JsonObject generateRandom(JsonObject req) {
        Integer n = Args.integer(req, "NumberOfBytes");
        if (n == null || n < 1 || n > 1024) {
            throw validation("1 validation error detected: Value '" + n + "' at 'numberOfBytes' failed to satisfy constraint: Member must "
                    + "have value between 1 and 1024");
        }
        JsonObject o = new JsonObject();
        o.addProperty("Plaintext", Base64.getEncoder().encodeToString(KmsCrypto.random(n)));
        return o;
    }

    private void checkSigning(Key k, String alg, boolean digest, byte[] message) {
        if (!k.usage.equals("SIGN_VERIFY")) {
            throw usage(k.arn + " key usage is " + k.usage + ", which is not valid for signing");
        }
        if (alg == null) {
            throw validation("1 validation error detected: Value null at 'signingAlgorithm' failed to satisfy constraint: Member must not be null");
        }
        boolean ok = false;
        for (String a : KmsCrypto.signingAlgorithms(k.spec)) {
            ok |= a.equals(alg);
        }
        if (!ok) {
            throw usage("Algorithm " + alg + " is incompatible with key spec " + k.spec + ".");
        }
        if (alg.equals("SM2DSA") && digest) {
            throw validation("SM2DSA requires MessageType RAW");
        }
        if (alg.equals("ED25519_SHA_512") && digest) {
            throw validation("ED25519_SHA_512 requires MessageType RAW");
        }
        if (alg.equals("ED25519_PH_SHA_512") && !digest) {
            throw validation("ED25519_PH_SHA_512 requires MessageType DIGEST");
        }
        if (digest) {
            int want = KmsCrypto.expectedDigestLength(alg);
            if (alg.equals("ED25519_PH_SHA_512")) {
                want = 64;
            }
            if (want > 0 && message.length != want) {
                throw validation("Digest is invalid length for algorithm " + alg + ".");
            }
        }
        if (!digest && message.length > 4096) {
            throw validation("1 validation error detected: Value at 'message' failed to satisfy constraint: Member must have length less "
                    + "than or equal to 4096");
        }
    }

    private JsonObject sign(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        byte[] msg = Args.bytes(req, "Message");
        if (msg == null || msg.length == 0) {
            throw validation("1 validation error detected: Value at 'message' failed to satisfy constraint: Member must have length "
                    + "greater than or equal to 1");
        }
        String type = Args.str(req, "MessageType") == null ? "RAW" : Args.str(req, "MessageType");
        String alg = Args.str(req, "SigningAlgorithm");
        boolean digest = type.equals("DIGEST");
        checkSigning(k, alg, digest, msg);
        byte[] sig;
        try {
            sig = KmsCrypto.sign(k.spec, secret(k), alg, msg, digest);
        } catch (IllegalArgumentException e) {
            throw validation(e.getMessage());
        } catch (IllegalStateException e) {
            throw unsupported(e.getMessage());
        }
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        o.addProperty("Signature", Base64.getEncoder().encodeToString(sig));
        o.addProperty("SigningAlgorithm", alg);
        return o;
    }

    private JsonObject verify(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        byte[] msg = Args.bytes(req, "Message");
        byte[] sig = Args.bytes(req, "Signature");
        if (msg == null || sig == null) {
            throw validation("Message and Signature are required");
        }
        String type = Args.str(req, "MessageType") == null ? "RAW" : Args.str(req, "MessageType");
        String alg = Args.str(req, "SigningAlgorithm");
        boolean digest = type.equals("DIGEST");
        checkSigning(k, alg, digest, msg);
        boolean valid;
        try {
            valid = KmsCrypto.verify(k.spec, k.publicKey, alg, msg, digest, sig);
        } catch (IllegalArgumentException e) {
            throw validation(e.getMessage());
        } catch (IllegalStateException e) {
            throw unsupported(e.getMessage());
        }
        if (!valid) {
            throw new AwsException(400, "KMSInvalidSignatureException", "The signature is not valid");
        }
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        o.addProperty("SignatureValid", true);
        o.addProperty("SigningAlgorithm", alg);
        return o;
    }

    private JsonObject getPublicKey(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        if (!k.asymmetric() || k.publicKey == null) {
            throw unsupported(k.arn + " is not an asymmetric key");
        }
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        o.addProperty("PublicKey", Base64.getEncoder().encodeToString(k.publicKey));
        o.addProperty("CustomerMasterKeySpec", k.spec);
        o.addProperty("KeySpec", k.spec);
        o.addProperty("KeyUsage", k.usage);
        if (k.usage.equals("ENCRYPT_DECRYPT")) {
            o.add("EncryptionAlgorithms", arr("RSAES_OAEP_SHA_1", "RSAES_OAEP_SHA_256"));
        } else {
            o.add("SigningAlgorithms", arr(KmsCrypto.signingAlgorithms(k.spec)));
        }
        return o;
    }

    private JsonObject generateMac(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        if (!k.usage.equals("GENERATE_VERIFY_MAC")) {
            throw usage(k.arn + " key usage is " + k.usage + ", which is not valid for GenerateMac");
        }
        String alg = Args.str(req, "MacAlgorithm");
        byte[] msg = Args.bytes(req, "Message");
        if (alg == null || !alg.equals("HMAC_SHA_" + k.spec.substring(5))) {
            throw usage("Algorithm " + alg + " is incompatible with key spec " + k.spec + ".");
        }
        JsonObject o = new JsonObject();
        o.addProperty("Mac", Base64.getEncoder().encodeToString(KmsCrypto.mac(secret(k), alg, msg)));
        o.addProperty("MacAlgorithm", alg);
        o.addProperty("KeyId", k.arn);
        return o;
    }

    private JsonObject verifyMac(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        if (!k.usage.equals("GENERATE_VERIFY_MAC")) {
            throw usage(k.arn + " key usage is " + k.usage + ", which is not valid for VerifyMac");
        }
        String alg = Args.str(req, "MacAlgorithm");
        if (alg == null || !alg.equals("HMAC_SHA_" + k.spec.substring(5))) {
            throw usage("Algorithm " + alg + " is incompatible with key spec " + k.spec + ".");
        }
        byte[] expect = KmsCrypto.mac(secret(k), alg, Args.bytes(req, "Message"));
        if (!KmsCrypto.constantTimeEquals(expect, Args.bytes(req, "Mac"))) {
            throw new AwsException(400, "KMSInvalidMacException", "The MAC is not valid");
        }
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        o.addProperty("MacValid", true);
        o.addProperty("MacAlgorithm", alg);
        return o;
    }

    // ---------------------------------------------------------------------------------------------- policy / tags / rotation

    private JsonObject getKeyPolicy(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        String name = Args.str(req, "PolicyName");
        if (name != null && !name.equals("default")) {
            throw new AwsException(400, "InvalidArnException", "Only the policy named default exists");
        }
        JsonObject o = new JsonObject();
        o.addProperty("Policy", k.policy);
        o.addProperty("PolicyName", "default");
        return o;
    }

    private JsonObject putKeyPolicy(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        String policy = Args.req(req, "Policy");
        try {
            JsonParser.parseString(policy).getAsJsonObject();
        } catch (RuntimeException e) {
            throw new AwsException(400, "MalformedPolicyDocumentException", "The provided policy document is malformed.");
        }
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET policy = ? WHERE key_id = ?")) {
                ps.setString(1, policy);
                ps.setString(2, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        return new JsonObject();
    }

    private JsonObject listKeyPolicies(JsonObject req) {
        resolve(Args.str(req, "KeyId"));
        JsonObject o = new JsonObject();
        o.add("PolicyNames", arr("default"));
        o.addProperty("Truncated", false);
        return o;
    }

    private void mutateTags(String keyRef, java.util.function.Consumer<Map<String, String>> f) {
        Key k = resolve(keyRef);
        sh.tx(sh.owner(k.id), c -> {
            Key cur = loadKey(k.id, true, c);
            Map<String, String> t = new TreeMap<>(cur.tags);
            f.accept(t);
            if (t.size() > 50) {
                throw new AwsException(400, "TagException", "Tag quota exceeded");
            }
            JsonObject j = new JsonObject();
            t.forEach(j::addProperty);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET tags = ?::jsonb WHERE key_id = ?")) {
                ps.setString(1, j.toString());
                ps.setString(2, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
    }

    private JsonObject tagResource(JsonObject req) {
        Map<String, String> add = new LinkedHashMap<>();
        for (JsonObject t : Args.objects(req, "Tags")) {
            add.put(Args.str(t, "TagKey"), Args.str(t, "TagValue"));
        }
        mutateTags(Args.str(req, "KeyId"), m -> m.putAll(add));
        return new JsonObject();
    }

    private JsonObject untagResource(JsonObject req) {
        List<String> keys = Args.strings(req, "TagKeys");
        mutateTags(Args.str(req, "KeyId"), m -> keys.forEach(m::remove));
        return new JsonObject();
    }

    private JsonObject listResourceTags(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        JsonArray a = new JsonArray();
        k.tags.forEach((key, v) -> {
            JsonObject t = new JsonObject();
            t.addProperty("TagKey", key);
            t.addProperty("TagValue", v);
            a.add(t);
        });
        JsonObject o = new JsonObject();
        o.add("Tags", a);
        o.addProperty("Truncated", false);
        return o;
    }

    private JsonObject rotationFlag(JsonObject req, boolean on) {
        Key k = resolve(Args.str(req, "KeyId"));
        if (!k.spec.equals("SYMMETRIC_DEFAULT") || k.origin.equals("EXTERNAL")) {
            throw unsupported(k.arn + " origin is EXTERNAL or the key is not symmetric, so it cannot be rotated");
        }
        Integer days = Args.integer(req, "RotationPeriodInDays");
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET rotation_on = ?, rotation_days = ? WHERE key_id = ?")) {
                ps.setBoolean(1, on);
                ps.setInt(2, days == null ? k.rotationDays : days);
                ps.setString(3, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        return new JsonObject();
    }

    private JsonObject rotationStatus(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        JsonObject o = new JsonObject();
        o.addProperty("KeyRotationEnabled", k.rotationOn);
        o.addProperty("KeyId", k.id);
        if (k.rotationOn) {
            o.addProperty("RotationPeriodInDays", k.rotationDays);
            o.addProperty("NextRotationDate", epoch(Timestamp.from(k.created.toInstant().plusSeconds(k.rotationDays * 86400L))));
        }
        return o;
    }

    private JsonObject rotateOnDemand(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        if (!k.spec.equals("SYMMETRIC_DEFAULT") || k.origin.equals("EXTERNAL")) {
            throw unsupported(k.arn + " cannot be rotated");
        }
        sh.tx(sh.owner(k.id), c -> {
            Key cur = loadKey(k.id, true, c);
            JsonArray rot = cur.rotations.deepCopy();
            JsonObject r = new JsonObject();
            r.addProperty("KeyId", cur.id);
            r.addProperty("RotationDate", epoch(Timestamp.from(Instant.now())));
            r.addProperty("RotationType", "ON_DEMAND");
            rot.add(r);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET rotations = ?::jsonb WHERE key_id = ?")) {
                ps.setString(1, rot.toString());
                ps.setString(2, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        return o;
    }

    private JsonObject listRotations(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        JsonObject o = new JsonObject();
        o.add("Rotations", k.rotations);
        o.addProperty("Truncated", false);
        return o;
    }

    // ---------------------------------------------------------------------------------------------- grants

    private JsonObject createGrant(JsonObject req) {
        Key k = usable(Args.str(req, "KeyId"));
        String grantee = Args.req(req, "GranteePrincipal");
        List<String> ops = Args.strings(req, "Operations");
        if (ops.isEmpty()) {
            throw validation("1 validation error detected: Value '[]' at 'operations' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        String id = java.util.HexFormat.of().formatHex(KmsCrypto.random(32));
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(KmsCrypto.random(48));
        JsonArray opsJson = new JsonArray();
        ops.forEach(opsJson::add);
        JsonObject constraints = Args.obj(req, "Constraints");
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_kms_grants (grant_id, key_id, name, grantee, retiring, "
                    + "operations, constraints, token) VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, ?)")) {
                ps.setString(1, id);
                ps.setString(2, k.id);
                ps.setString(3, Args.str(req, "Name"));
                ps.setString(4, grantee);
                ps.setString(5, Args.str(req, "RetiringPrincipal"));
                ps.setString(6, opsJson.toString());
                ps.setString(7, constraints == null ? null : constraints.toString());
                ps.setString(8, token);
                ps.executeUpdate();
            }
            return null;
        });
        JsonObject o = new JsonObject();
        o.addProperty("GrantToken", token);
        o.addProperty("GrantId", id);
        return o;
    }

    private JsonObject grantRow(ResultSet rs) throws SQLException {
        JsonObject g = new JsonObject();
        g.addProperty("KeyId", keyArn(rs.getString("key_id")));
        g.addProperty("GrantId", rs.getString("grant_id"));
        if (rs.getString("name") != null) {
            g.addProperty("Name", rs.getString("name"));
        }
        g.addProperty("CreationDate", epoch(rs.getTimestamp("created_at")));
        g.addProperty("GranteePrincipal", rs.getString("grantee"));
        if (rs.getString("retiring") != null) {
            g.addProperty("RetiringPrincipal", rs.getString("retiring"));
        }
        g.addProperty("IssuingAccount", "arn:aws:iam::" + rt.config.accountId + ":root");
        g.add("Operations", JsonParser.parseString(rs.getString("ops")));
        if (rs.getString("cons") != null) {
            g.add("Constraints", JsonParser.parseString(rs.getString("cons")));
        }
        return g;
    }

    private static final String GRANT_COLS = "grant_id, key_id, name, grantee, retiring, operations::text AS ops, constraints::text AS cons, "
            + "token, created_at";

    private JsonObject listGrants(JsonObject req, boolean retirable) {
        List<JsonObject> grants = new ArrayList<>();
        String grantId = Args.str(req, "GrantId");
        String grantee = Args.str(req, "GranteePrincipal");
        if (retirable) {
            String retiring = Args.req(req, "RetiringPrincipal");
            for (List<JsonObject> l : sh.<List<JsonObject>>onAll(c -> {
                List<JsonObject> r = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("SELECT " + GRANT_COLS + " FROM warp_awsparams_kms_grants WHERE retiring = ? ORDER BY grant_id")) {
                    ps.setString(1, retiring);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            r.add(grantRow(rs));
                        }
                    }
                }
                return r;
            })) {
                grants.addAll(l);
            }
        } else {
            Key k = resolve(Args.str(req, "KeyId"));
            grants = sh.conn(sh.owner(k.id), c -> {
                List<JsonObject> r = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement("SELECT " + GRANT_COLS + " FROM warp_awsparams_kms_grants WHERE key_id = ? "
                        + "AND (?::text IS NULL OR grant_id = ?) AND (?::text IS NULL OR grantee = ?) ORDER BY grant_id")) {
                    ps.setString(1, k.id);
                    ps.setString(2, grantId);
                    ps.setString(3, grantId);
                    ps.setString(4, grantee);
                    ps.setString(5, grantee);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            r.add(grantRow(rs));
                        }
                    }
                }
                return r;
            });
        }
        JsonArray a = new JsonArray();
        grants.forEach(a::add);
        JsonObject o = new JsonObject();
        o.add("Grants", a);
        o.addProperty("Truncated", false);
        return o;
    }

    private JsonObject revokeGrant(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        String id = Args.req(req, "GrantId");
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_kms_grants WHERE key_id = ? AND grant_id = ?")) {
                ps.setString(1, k.id);
                ps.setString(2, id);
                if (ps.executeUpdate() == 0) {
                    throw notFound("Grant ID " + id + " is not found");
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject retireGrant(JsonObject req) {
        String token = Args.str(req, "GrantToken");
        String grantId = Args.str(req, "GrantId");
        String keyRef = Args.str(req, "KeyId");
        if (token == null && (grantId == null || keyRef == null)) {
            throw validation("GrantToken or both KeyId and GrantId are required");
        }
        int removed = 0;
        for (Integer n : sh.<Integer>onAll(c -> {
            try (PreparedStatement ps = token != null ? c.prepareStatement("DELETE FROM warp_awsparams_kms_grants WHERE token = ?")
                    : c.prepareStatement("DELETE FROM warp_awsparams_kms_grants WHERE key_id = ? AND grant_id = ?")) {
                if (token != null) {
                    ps.setString(1, token);
                } else {
                    ps.setString(1, keyIdOf(keyRef));
                    ps.setString(2, grantId);
                }
                return ps.executeUpdate();
            }
        })) {
            removed += n;
        }
        if (removed == 0) {
            throw notFound("Grant not found");
        }
        return new JsonObject();
    }

    // ---------------------------------------------------------------------------------------------- import

    private JsonObject getParametersForImport(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        if (!k.origin.equals("EXTERNAL")) {
            throw unsupported(k.arn + " origin is not EXTERNAL");
        }
        String spec = Args.str(req, "WrappingKeySpec");
        int bits = spec == null ? 2048 : spec.startsWith("RSA_") ? Integer.parseInt(spec.substring(4)) : 2048;
        KmsCrypto.Generated g;
        try {
            KeyPairGenerator kg = KeyPairGenerator.getInstance("RSA");
            kg.initialize(bits);
            KeyPair kp = kg.generateKeyPair();
            g = new KmsCrypto.Generated(kp.getPrivate().getEncoded(), kp.getPublic().getEncoded());
        } catch (Exception e) {
            throw AwsException.internal(e.getMessage());
        }
        String token = Base64.getEncoder().encodeToString(KmsCrypto.random(64));
        Timestamp until = Timestamp.from(Instant.now().plusSeconds(24 * 3600));
        byte[] sealed = KmsCrypto.wrap(masterKey(), "import:" + k.id, g.privateKey());
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_kms_import_tokens (token, key_id, private_key, valid_until) VALUES (?, ?, ?, ?)")) {
                ps.setString(1, token);
                ps.setString(2, k.id);
                ps.setBytes(3, sealed);
                ps.setTimestamp(4, until);
                ps.executeUpdate();
            }
            return null;
        });
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        o.addProperty("ImportToken", token);
        o.addProperty("PublicKey", Base64.getEncoder().encodeToString(g.publicKey()));
        o.addProperty("ParametersValidTo", epoch(until));
        return o;
    }

    private JsonObject importKeyMaterial(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        String token = Args.req(req, "ImportToken");
        byte[] wrapped = Args.bytes(req, "EncryptedKeyMaterial");
        String model = Args.str(req, "ExpirationModel") == null ? "KEY_MATERIAL_EXPIRES" : Args.str(req, "ExpirationModel");
        Double validTo = Args.dbl(req, "ValidTo");
        String alg = Args.str(req, "WrappingAlgorithm");
        sh.tx(sh.owner(k.id), c -> {
            byte[] sealedPriv;
            try (PreparedStatement ps = c.prepareStatement("SELECT private_key, valid_until FROM warp_awsparams_kms_import_tokens WHERE token = ? AND key_id = ?")) {
                ps.setString(1, token);
                ps.setString(2, k.id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new AwsException(400, "InvalidImportTokenException", "The import token is not valid for this key");
                    }
                    if (rs.getTimestamp(2).before(new Timestamp(System.currentTimeMillis()))) {
                        throw new AwsException(400, "ExpiredImportTokenException", "The import token has expired");
                    }
                    sealedPriv = rs.getBytes(1);
                }
            }
            byte[] priv = KmsCrypto.unwrap(masterKey(), "import:" + k.id, sealedPriv);
            byte[] material = KmsCrypto.rsaDecrypt(priv, alg == null ? "RSAES_OAEP_SHA_256" : alg, wrapped);
            if (material == null) {
                material = KmsCrypto.rsaDecrypt(priv, "RSAES_OAEP_SHA_1", wrapped);
            }
            if (material == null || material.length != 32) {
                throw new AwsException(400, "IncorrectKeyMaterialException", "The key material is not a 256-bit symmetric key");
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET material = ?, state = 'Enabled', "
                    + "expiration_model = ?, valid_to = ? WHERE key_id = ?")) {
                ps.setBytes(1, KmsCrypto.wrap(masterKey(), k.id, material));
                ps.setString(2, model);
                ps.setTimestamp(3, validTo == null ? null : new Timestamp((long) (validTo * 1000)));
                ps.setString(4, k.id);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_kms_import_tokens WHERE token = ?")) {
                ps.setString(1, token);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        return new JsonObject();
    }

    private JsonObject deleteImportedKeyMaterial(JsonObject req) {
        Key k = resolve(Args.str(req, "KeyId"));
        if (!k.origin.equals("EXTERNAL")) {
            throw unsupported(k.arn + " origin is not EXTERNAL");
        }
        sh.conn(sh.owner(k.id), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_kms_keys SET material = NULL, state = 'PendingImport' WHERE key_id = ?")) {
                ps.setString(1, k.id);
                ps.executeUpdate();
            }
            return null;
        });
        forget(k.id);
        JsonObject o = new JsonObject();
        o.addProperty("KeyId", k.arn);
        return o;
    }
}

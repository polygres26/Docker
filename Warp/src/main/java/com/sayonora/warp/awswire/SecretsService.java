package com.sayonora.warp.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.core.SqlMetricsCollector;
import com.sayonora.warp.core.StoreType;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AWS Secrets Manager on Postgres (the {@code awsparams} store), JSON 1.1. A secret and its versions live on the host owning
 * hash(secret name); ListSecrets and BatchGetSecretValue fan out and merge. Secret values are never stored in clear: each
 * value is sealed with the secret's KMS key (default the AWS-managed {@code alias/aws/secretsmanager}) through
 * {@link KmsService}, with the secret ARN as encryption context, so the service needs the KMS master key (see
 * {@link KmsService}). Versions and staging labels follow the real rules (AWSCURRENT moves to a new version and the previous
 * current becomes AWSPREVIOUS; a stage lives on one version). RotateSecret stores the rotation configuration but invokes no
 * rotation function (there is no Lambda to call).
 */
public final class SecretsService extends AwsService {

    private static final Logger log = LoggerFactory.getLogger(SecretsService.class);
    private static final Pattern NAME = Pattern.compile("^[A-Za-z0-9/_+=.@-]{1,512}$");
    private static final String ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final AwsRuntime rt;
    private final KmsService kms;
    private final AwsShards sh;
    private final ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "secretswire-sweeper");
        t.setDaemon(true);
        return t;
    });

    public SecretsService(AwsRuntime rt, KmsService kms) {
        this.rt = rt;
        this.kms = kms;
        this.sh = new AwsShards(rt.registry, StoreType.AWSPARAMS);
    }

    @Override
    public String id() {
        return "secretsmanager";
    }

    @Override
    public String metricsProtocol() {
        return "secretswire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("secretsmanager");
    }

    @Override
    public Set<String> targetPrefixes() {
        return Set.of("secretsmanager.");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return switch (op) {
            case "GetSecretValue", "BatchGetSecretValue", "DescribeSecret", "ListSecrets", "ListSecretVersionIds", "GetRandomPassword",
                    "GetResourcePolicy", "ValidateResourcePolicy" -> SqlMetricsCollector.StatementKind.READ;
            default -> SqlMetricsCollector.StatementKind.WRITE;
        };
    }

    @Override
    public String backendLabel(String op, JsonObject req) {
        try {
            String n = req == null ? null : Args.str(req, "Name") != null ? Args.str(req, "Name") : Args.str(req, "SecretId");
            return n == null || n.startsWith("arn:") || !sh.available() ? "default" : sh.owner(n);
        } catch (RuntimeException e) {
            return "default";
        }
    }

    @Override
    public void start() {
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                if (sh.available()) {
                    for (String h : sh.hosts()) {
                        sh.conn(h, c -> {
                            try (var st = c.createStatement()) {
                                st.executeUpdate("DELETE FROM warp_awsparams_secret_versions WHERE name IN (SELECT name FROM warp_awsparams_secrets "
                                        + "WHERE delete_after IS NOT NULL AND delete_after < now())");
                                st.executeUpdate("DELETE FROM warp_awsparams_secrets WHERE delete_after IS NOT NULL AND delete_after < now()");
                            }
                            return null;
                        });
                    }
                }
            } catch (RuntimeException e) {
                log.debug("secretswire sweep failed: {}", e.getMessage());
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        sweeper.shutdownNow();
    }

    // ---------------------------------------------------------------------------------------------- model

    private static final class Secret {
        String name, arn, description, kmsKeyId, policy, rotationLambda;
        Map<String, String> tags = new TreeMap<>();
        JsonObject rotationRules;
        boolean rotationEnabled;
        Timestamp lastRotated, lastChanged, lastAccessed, created, deletedAt, deleteAfter;
        JsonArray replicas = new JsonArray();
    }

    private record Version(String id, List<String> stages, byte[] str, byte[] bin, Timestamp created) {
    }

    private static final String SECRET_COLS = "name, arn, description, kms_key_id, tags::text, policy, rotation_lambda, "
            + "rotation_rules::text, rotation_enabled, last_rotated, last_changed, last_accessed, created_at, deleted_at, delete_after, replicas::text";

    private static Secret readSecret(ResultSet rs) throws SQLException {
        Secret s = new Secret();
        s.name = rs.getString(1);
        s.arn = rs.getString(2);
        s.description = rs.getString(3);
        s.kmsKeyId = rs.getString(4);
        JsonParser.parseString(rs.getString(5)).getAsJsonObject().entrySet().forEach(e -> s.tags.put(e.getKey(), e.getValue().getAsString()));
        s.policy = rs.getString(6);
        s.rotationLambda = rs.getString(7);
        s.rotationRules = rs.getString(8) == null ? null : JsonParser.parseString(rs.getString(8)).getAsJsonObject();
        s.rotationEnabled = rs.getBoolean(9);
        s.lastRotated = rs.getTimestamp(10);
        s.lastChanged = rs.getTimestamp(11);
        s.lastAccessed = rs.getTimestamp(12);
        s.created = rs.getTimestamp(13);
        s.deletedAt = rs.getTimestamp(14);
        s.deleteAfter = rs.getTimestamp(15);
        s.replicas = JsonParser.parseString(rs.getString(16)).getAsJsonArray();
        return s;
    }

    private static AwsException notFound() {
        return new AwsException(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret.");
    }

    private static AwsException invalidParam(String m) {
        return new AwsException(400, "InvalidParameterException", m);
    }

    private static AwsException invalidRequest(String m) {
        return new AwsException(400, "InvalidRequestException", m);
    }

    private static double epoch(Timestamp t) {
        return t == null ? 0 : t.getTime() / 1000.0;
    }

    private static String randomSuffix() {
        StringBuilder sb = new StringBuilder("-");
        for (int i = 0; i < 6; i++) {
            sb.append(ALNUM.charAt(RANDOM.nextInt(ALNUM.length())));
        }
        return sb.toString();
    }

    /** Candidate secret names for a SecretId (name, full ARN, or partial ARN without the random suffix). */
    private static List<String> candidates(String id) {
        List<String> out = new ArrayList<>();
        if (id.startsWith("arn:")) {
            int i = id.indexOf(":secret:");
            if (i < 0) {
                throw invalidParam("Invalid secret ARN: " + id);
            }
            String rest = id.substring(i + 8);
            out.add(rest);
            if (rest.length() > 7 && rest.charAt(rest.length() - 7) == '-') {
                out.add(rest.substring(0, rest.length() - 7));
            }
        } else {
            out.add(id);
        }
        return out;
    }

    private Secret find(Connection c, String name, String givenId) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + SECRET_COLS + " FROM warp_awsparams_secrets WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                Secret s = readSecret(rs);
                if (givenId.startsWith("arn:") && !givenId.equals(s.arn) && !givenId.equals(rt.config.arn("secretsmanager", "secret:" + s.name))) {
                    return null;
                }
                return s;
            }
        }
    }

    private interface SecretFn<T> {
        T apply(Connection c, Secret s, String host) throws SQLException;
    }

    /** Resolves the secret (possibly by partial ARN) on its owner host and runs {@code fn} there in one connection. */
    private <T> T withSecret(String id, boolean tx, boolean allowDeleted, SecretFn<T> fn) {
        AwsException last = notFound();
        for (String name : candidates(id)) {
            String host = sh.owner(name);
            try {
                AwsShards.SqlFn<T> body = c -> {
                    Secret s = find(c, name, id);
                    if (s == null) {
                        throw notFound();
                    }
                    if (s.deletedAt != null && !allowDeleted) {
                        throw invalidRequest("You can't perform this operation on the secret because it was marked for deletion.");
                    }
                    return fn.apply(c, s, host);
                };
                return tx ? sh.tx(host, body) : sh.conn(host, body);
            } catch (AwsException e) {
                if (!e.code.equals("ResourceNotFoundException") || e.getMessage() == null || !e.getMessage().equals(last.getMessage())) {
                    throw e;
                }
                last = e;
            }
        }
        throw last;
    }

    private String keyFor(Secret s) {
        return s.kmsKeyId != null ? s.kmsKeyId : kms.awsManagedKey("alias/aws/secretsmanager",
                "Default key that protects my Secrets Manager data when no other key is defined");
    }

    private byte[] seal(Secret s, byte[] data) {
        return kms.encryptFor(keyFor(s), data, Map.of("SecretARN", s.arn));
    }

    private byte[] unseal(Secret s, byte[] blob) {
        return kms.decryptFor(blob, Map.of("SecretARN", s.arn));
    }

    private List<Version> versions(Connection c, String name) throws SQLException {
        List<Version> out = new ArrayList<>();
        try (PreparedStatement ps = c.prepareStatement("SELECT version_id, stages::text, secret_string, secret_binary, created_at "
                + "FROM warp_awsparams_secret_versions WHERE name = ? ORDER BY created_at, version_id")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    List<String> st = new ArrayList<>();
                    JsonParser.parseString(rs.getString(2)).getAsJsonArray().forEach(e -> st.add(e.getAsString()));
                    out.add(new Version(rs.getString(1), st, rs.getBytes(3), rs.getBytes(4), rs.getTimestamp(5)));
                }
            }
        }
        return out;
    }

    private static void saveStages(Connection c, String name, String versionId, List<String> stages) throws SQLException {
        JsonArray a = new JsonArray();
        stages.forEach(a::add);
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secret_versions SET stages = ?::jsonb WHERE name = ? AND version_id = ?")) {
            ps.setString(1, a.toString());
            ps.setString(2, name);
            ps.setString(3, versionId);
            ps.executeUpdate();
        }
    }

    private static void touch(Connection c, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET last_changed = now() WHERE name = ?")) {
            ps.setString(1, name);
            ps.executeUpdate();
        }
    }

    /**
     * Adds a version carrying {@code stages} (values already sealed by {@link #seal}: no KMS call happens while a connection is
     * held), moving each stage off the version that held it (AWSCURRENT demotes to AWSPREVIOUS).
     */
    private String addVersion(Connection c, Secret s, String token, byte[] sealedStr, byte[] sealedBin, List<String> stages)
            throws SQLException {
        String vid = token != null ? token : UUID.randomUUID().toString();
        List<Version> existing = versions(c, s.name);
        for (Version v : existing) {
            if (v.id.equals(vid)) {
                return vid; // idempotent retry with the same ClientRequestToken (the value is not compared)
            }
        }
        boolean current = stages.contains("AWSCURRENT");
        for (Version v : existing) {
            List<String> st = new ArrayList<>(v.stages);
            boolean changed = st.removeAll(stages);
            if (current && v.stages.contains("AWSCURRENT")) {
                st.add("AWSPREVIOUS");
                changed = true;
            } else if (current && v.stages.contains("AWSPREVIOUS")) {
                st.remove("AWSPREVIOUS");
                changed = true;
            }
            if (changed) {
                saveStages(c, s.name, v.id, st);
            }
        }
        JsonArray a = new JsonArray();
        stages.forEach(a::add);
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_secret_versions (name, version_id, stages, secret_string, "
                + "secret_binary) VALUES (?, ?, ?::jsonb, ?, ?)")) {
            ps.setString(1, s.name);
            ps.setString(2, vid);
            ps.setString(3, a.toString());
            ps.setBytes(4, sealedStr);
            ps.setBytes(5, sealedBin);
            ps.executeUpdate();
        }
        touch(c, s.name);
        return vid;
    }

    private static List<String> stagesFor(JsonObject req) {
        List<String> st = Args.strings(req, "VersionStages");
        return st.isEmpty() ? new ArrayList<>(List.of("AWSCURRENT")) : st;
    }

    private static byte[][] valueOf(JsonObject req) {
        String str = Args.str(req, "SecretString");
        byte[] bin = Args.bytes(req, "SecretBinary");
        if (str != null && bin != null) {
            throw invalidParam("You can't specify both SecretString and SecretBinary.");
        }
        return new byte[][] {str == null ? null : str.getBytes(StandardCharsets.UTF_8), bin};
    }

    private static void checkToken(String t) {
        if (t != null && (t.length() < 32 || t.length() > 64)) {
            throw invalidParam("ClientRequestToken must be 32 to 64 characters long.");
        }
    }

    // ---------------------------------------------------------------------------------------------- dispatch

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        return switch (op) {
            case "CreateSecret" -> createSecret(req);
            case "GetSecretValue" -> getSecretValue(req);
            case "BatchGetSecretValue" -> batchGet(req);
            case "PutSecretValue" -> putSecretValue(req);
            case "UpdateSecret" -> updateSecret(req);
            case "DescribeSecret" -> describeSecret(req);
            case "ListSecrets" -> listSecrets(req);
            case "DeleteSecret" -> deleteSecret(req);
            case "RestoreSecret" -> restoreSecret(req);
            case "ListSecretVersionIds" -> listVersions(req);
            case "UpdateSecretVersionStage" -> updateStage(req);
            case "GetRandomPassword" -> randomPassword(req);
            case "TagResource" -> tag(req, true);
            case "UntagResource" -> tag(req, false);
            case "PutResourcePolicy" -> putPolicy(req);
            case "GetResourcePolicy" -> getPolicy(req);
            case "DeleteResourcePolicy" -> deletePolicy(req);
            case "ValidateResourcePolicy" -> validatePolicy(req);
            case "RotateSecret" -> rotateSecret(req);
            case "CancelRotateSecret" -> cancelRotate(req);
            case "ReplicateSecretToRegions" -> replicate(req);
            case "RemoveRegionsFromReplication" -> removeRegions(req);
            case "StopReplicationToReplica" -> nameOnly(req);
            default -> throw new AwsException(400, "InvalidRequestException", "Unknown operation " + op);
        };
    }

    // ---------------------------------------------------------------------------------------------- CRUD

    private JsonObject createSecret(JsonObject req) {
        String name = Args.req(req, "Name");
        if (!NAME.matcher(name).matches()) {
            throw invalidParam("Invalid name. Must be a valid name containing alphanumeric characters, or any of the following: -/_+=.@!");
        }
        checkToken(Args.str(req, "ClientRequestToken"));
        String kmsKey = Args.str(req, "KmsKeyId");
        if (kmsKey != null && !kms.canUse(kmsKey)) {
            throw invalidParam("You can't access the KMS key. Verify that the key exists and you have permission to use it.");
        }
        byte[][] value = valueOf(req);
        Map<String, String> tags = new TreeMap<>();
        for (JsonObject t : Args.objects(req, "Tags")) {
            tags.put(Args.str(t, "Key"), Args.str(t, "Value") == null ? "" : Args.str(t, "Value"));
        }
        String arn = rt.config.arn("secretsmanager", "secret:" + name + randomSuffix());
        String desc = Args.str(req, "Description");
        String token = Args.str(req, "ClientRequestToken");
        Secret pre = new Secret();
        pre.name = name;
        pre.arn = arn;
        pre.kmsKeyId = kmsKey;
        byte[] sealedStr = value[0] == null ? null : seal(pre, value[0]);
        byte[] sealedBin = value[1] == null ? null : seal(pre, value[1]);
        // the ARN of a KMS key the caller gave is stored as given (DescribeSecret echoes it)
        return sh.tx(sh.owner(name), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT deleted_at FROM warp_awsparams_secrets WHERE name = ? FOR UPDATE")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        if (rs.getTimestamp(1) != null) {
                            throw invalidRequest("You can't create this secret because a secret with this name is already scheduled for deletion.");
                        }
                        throw new AwsException(400, "ResourceExistsException", "The operation failed because the secret " + name + " already exists.");
                    }
                }
            }
            JsonObject t = new JsonObject();
            tags.forEach(t::addProperty);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_secrets (name, arn, description, kms_key_id, tags) "
                    + "VALUES (?, ?, ?, ?, ?::jsonb)")) {
                ps.setString(1, name);
                ps.setString(2, arn);
                ps.setString(3, desc == null ? "" : desc);
                ps.setString(4, kmsKey);
                ps.setString(5, t.toString());
                ps.executeUpdate();
            }
            Secret s = find(c, name, name);
            JsonObject out = new JsonObject();
            out.addProperty("ARN", arn);
            out.addProperty("Name", name);
            if (value[0] != null || value[1] != null) {
                out.addProperty("VersionId", addVersion(c, s, token, sealedStr, sealedBin, new ArrayList<>(List.of("AWSCURRENT"))));
            }
            return out;
        });
    }

    private JsonObject valueJson(Secret s, Version v) {
        JsonObject o = new JsonObject();
        o.addProperty("ARN", s.arn);
        o.addProperty("Name", s.name);
        o.addProperty("VersionId", v.id);
        if (v.str != null) {
            o.addProperty("SecretString", new String(unseal(s, v.str), StandardCharsets.UTF_8));
        }
        if (v.bin != null) {
            o.addProperty("SecretBinary", Base64.getEncoder().encodeToString(unseal(s, v.bin)));
        }
        JsonArray st = new JsonArray();
        v.stages.forEach(st::add);
        o.add("VersionStages", st);
        o.addProperty("CreatedDate", epoch(v.created));
        return o;
    }

    private Version pick(Connection c, Secret s, String versionId, String stage) throws SQLException {
        List<Version> vs = versions(c, s.name);
        if (versionId != null) {
            for (Version v : vs) {
                if (v.id.equals(versionId) && (stage == null || v.stages.contains(stage))) {
                    return v;
                }
            }
            throw new AwsException(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret value for VersionId: " + versionId);
        }
        String want = stage == null ? "AWSCURRENT" : stage;
        for (Version v : vs) {
            if (v.stages.contains(want)) {
                return v;
            }
        }
        throw new AwsException(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret value for staging label: " + want);
    }

    private JsonObject getSecretValue(JsonObject req) {
        String id = Args.req(req, "SecretId");
        String vid = Args.str(req, "VersionId");
        String stage = Args.str(req, "VersionStage");
        Object[] found = withSecret(id, false, false, (c, s, host) -> {
            Version v = pick(c, s, vid, stage);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET last_accessed = date_trunc('day', now()) "
                    + "WHERE name = ? AND last_accessed IS DISTINCT FROM date_trunc('day', now())")) {
                ps.setString(1, s.name);
                ps.executeUpdate();
            }
            return new Object[] {s, v};
        });
        return valueJson((Secret) found[0], (Version) found[1]); // KMS decrypt happens with no database connection held
    }

    private JsonObject batchGet(JsonObject req) {
        List<String> ids = Args.strings(req, "SecretIdList");
        List<JsonObject> filters = Args.objects(req, "Filters");
        if (ids.isEmpty() == filters.isEmpty()) {
            throw invalidParam("You must specify either SecretIdList or Filters, but not both.");
        }
        if (ids.isEmpty()) {
            for (JsonObject o : listSecrets(req).getAsJsonArray("SecretList").asList().stream().map(JsonElement::getAsJsonObject).toList()) {
                ids.add(Args.str(o, "Name"));
            }
        }
        JsonArray values = new JsonArray();
        JsonArray errors = new JsonArray();
        for (String id : ids) {
            try {
                JsonObject one = new JsonObject();
                one.addProperty("SecretId", id);
                values.add(getSecretValue(one));
            } catch (AwsException e) {
                JsonObject err = new JsonObject();
                err.addProperty("SecretId", id);
                err.addProperty("ErrorCode", e.code);
                err.addProperty("Message", e.getMessage());
                errors.add(err);
            }
        }
        JsonObject out = new JsonObject();
        out.add("SecretValues", values);
        out.add("Errors", errors);
        return out;
    }

    private JsonObject putSecretValue(JsonObject req) {
        String id = Args.req(req, "SecretId");
        byte[][] value = valueOf(req);
        if (value[0] == null && value[1] == null) {
            throw invalidParam("You must provide either SecretString or SecretBinary.");
        }
        String token = Args.str(req, "ClientRequestToken");
        checkToken(token);
        List<String> stages = stagesFor(req);
        Secret pre = withSecret(id, false, false, (c, s, host) -> s);
        byte[] sealedStr = value[0] == null ? null : seal(pre, value[0]);
        byte[] sealedBin = value[1] == null ? null : seal(pre, value[1]);
        return withSecret(id, true, false, (c, s, host) -> {
            String vid = addVersion(c, s, token, sealedStr, sealedBin, stages);
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.addProperty("Name", s.name);
            out.addProperty("VersionId", vid);
            JsonArray st = new JsonArray();
            stages.forEach(st::add);
            out.add("VersionStages", st);
            return out;
        });
    }

    private JsonObject updateSecret(JsonObject req) {
        String id = Args.req(req, "SecretId");
        byte[][] value = valueOf(req);
        String token = Args.str(req, "ClientRequestToken");
        checkToken(token);
        String kmsKey = Args.str(req, "KmsKeyId");
        if (kmsKey != null && !kms.canUse(kmsKey)) {
            throw invalidParam("You can't access the KMS key. Verify that the key exists and you have permission to use it.");
        }
        String desc = Args.str(req, "Description");
        byte[] sealedStr = null, sealedBin = null;
        if (value[0] != null || value[1] != null) {
            Secret pre = withSecret(id, false, false, (c, s, host) -> s);
            if (kmsKey != null) {
                pre.kmsKeyId = kmsKey;
            }
            sealedStr = value[0] == null ? null : seal(pre, value[0]);
            sealedBin = value[1] == null ? null : seal(pre, value[1]);
        }
        byte[] fStr = sealedStr, fBin = sealedBin;
        return withSecret(id, true, false, (c, s, host) -> {
            if (desc != null || kmsKey != null) {
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET description = COALESCE(?, description), "
                        + "kms_key_id = COALESCE(?, kms_key_id), last_changed = now() WHERE name = ?")) {
                    ps.setString(1, desc);
                    ps.setString(2, kmsKey);
                    ps.setString(3, s.name);
                    ps.executeUpdate();
                }
                if (kmsKey != null) {
                    s.kmsKeyId = kmsKey;
                }
            }
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.addProperty("Name", s.name);
            if (value[0] != null || value[1] != null) {
                out.addProperty("VersionId", addVersion(c, s, token, fStr, fBin, new ArrayList<>(List.of("AWSCURRENT"))));
            }
            return out;
        });
    }

    private JsonObject describeJson(Connection c, Secret s) throws SQLException {
        JsonObject o = new JsonObject();
        o.addProperty("ARN", s.arn);
        o.addProperty("Name", s.name);
        if (s.description != null && !s.description.isEmpty()) {
            o.addProperty("Description", s.description);
        }
        if (s.kmsKeyId != null) {
            o.addProperty("KmsKeyId", s.kmsKeyId);
        }
        o.addProperty("RotationEnabled", s.rotationEnabled);
        if (s.rotationLambda != null) {
            o.addProperty("RotationLambdaARN", s.rotationLambda);
        }
        if (s.rotationRules != null) {
            o.add("RotationRules", s.rotationRules);
        }
        if (s.lastRotated != null) {
            o.addProperty("LastRotatedDate", epoch(s.lastRotated));
        }
        o.addProperty("LastChangedDate", epoch(s.lastChanged));
        if (s.lastAccessed != null) {
            o.addProperty("LastAccessedDate", epoch(s.lastAccessed));
        }
        if (s.deletedAt != null) {
            o.addProperty("DeletedDate", epoch(s.deletedAt));
        }
        JsonArray tags = new JsonArray();
        s.tags.forEach((k, v) -> {
            JsonObject t = new JsonObject();
            t.addProperty("Key", k);
            t.addProperty("Value", v);
            tags.add(t);
        });
        if (tags.size() > 0) {
            o.add("Tags", tags);
        }
        if (c != null) {
            JsonObject map = new JsonObject();
            for (Version v : versions(c, s.name)) {
                if (!v.stages.isEmpty()) {
                    JsonArray st = new JsonArray();
                    v.stages.forEach(st::add);
                    map.add(v.id, st);
                }
            }
            o.add("VersionIdsToStages", map);
        }
        o.addProperty("CreatedDate", epoch(s.created));
        if (s.replicas.size() > 0) {
            o.add("ReplicationStatus", s.replicas);
        }
        return o;
    }

    private JsonObject describeSecret(JsonObject req) {
        return withSecret(Args.req(req, "SecretId"), false, true, (c, s, host) -> describeJson(c, s));
    }

    private static boolean prefixMatch(String value, List<String> patterns) {
        boolean positive = false, any = false;
        for (String p : patterns) {
            if (p.startsWith("!")) {
                if (value.toLowerCase().startsWith(p.substring(1).toLowerCase())) {
                    return false;
                }
            } else {
                positive = true;
                any |= value.toLowerCase().startsWith(p.toLowerCase());
            }
        }
        return !positive || any;
    }

    private boolean matches(Secret s, List<JsonObject> filters) {
        for (JsonObject f : filters) {
            String key = Args.str(f, "Key");
            List<String> vals = Args.strings(f, "Values");
            boolean ok = switch (key == null ? "" : key) {
                case "name" -> prefixMatch(s.name, vals);
                case "description" -> prefixMatch(s.description == null ? "" : s.description, vals);
                case "tag-key" -> s.tags.keySet().stream().anyMatch(k -> prefixMatch(k, vals));
                case "tag-value" -> s.tags.values().stream().anyMatch(v -> prefixMatch(v, vals));
                case "primary-region" -> vals.contains(rt.config.region);
                case "owning-service" -> false;
                case "all" -> vals.stream().anyMatch(v -> s.name.toLowerCase().contains(v.toLowerCase())
                        || (s.description != null && s.description.toLowerCase().contains(v.toLowerCase()))
                        || s.tags.entrySet().stream().anyMatch(e -> e.getKey().toLowerCase().contains(v.toLowerCase())
                                || e.getValue().toLowerCase().contains(v.toLowerCase())));
                default -> throw invalidParam("Invalid filter key: " + key);
            };
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private JsonObject listSecrets(JsonObject req) {
        List<JsonObject> filters = Args.objects(req, "Filters");
        boolean plannedDeletion = Boolean.TRUE.equals(Args.bool(req, "IncludePlannedDeletion"));
        Integer maxArg = Args.integer(req, "MaxResults");
        int max = maxArg == null ? 100 : Math.max(1, Math.min(maxArg, 100));
        String token = Args.str(req, "NextToken");
        String after = token == null ? null : new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        boolean desc = "desc".equals(Args.str(req, "SortOrder"));
        List<Secret> all = new ArrayList<>();
        for (List<Secret> l : sh.<List<Secret>>onAll(c -> {
            List<Secret> r = new ArrayList<>();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT " + SECRET_COLS + " FROM warp_awsparams_secrets")) {
                while (rs.next()) {
                    r.add(readSecret(rs));
                }
            }
            return r;
        })) {
            all.addAll(l);
        }
        all.sort(desc ? Comparator.comparing((Secret s) -> s.name).reversed() : Comparator.comparing(s -> s.name));
        JsonArray list = new JsonArray();
        boolean more = false;
        String last = null;
        boolean skipping = after != null;
        for (Secret s : all) {
            if (skipping) {
                if (desc ? s.name.compareTo(after) >= 0 : s.name.compareTo(after) <= 0) {
                    continue;
                }
                skipping = false;
            }
            if ((s.deletedAt != null && !plannedDeletion) || !matches(s, filters)) {
                continue;
            }
            if (list.size() >= max) {
                more = true;
                break;
            }
            try {
                list.add(describeJson(null, s));
            } catch (SQLException e) {
                throw AwsShards.storage(e);
            }
            last = s.name;
        }
        JsonObject out = new JsonObject();
        out.add("SecretList", list);
        if (more) {
            out.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(last.getBytes(StandardCharsets.UTF_8)));
        }
        return out;
    }

    private JsonObject deleteSecret(JsonObject req) {
        String id = Args.req(req, "SecretId");
        Boolean force = Args.bool(req, "ForceDeleteWithoutRecovery");
        Long window = Args.lng(req, "RecoveryWindowInDays");
        if (Boolean.TRUE.equals(force) && window != null) {
            throw invalidParam("You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.");
        }
        if (window != null && (window < 7 || window > 30)) {
            throw invalidParam("RecoveryWindowInDays value must be between 7 and 30 days (inclusive).");
        }
        return withSecret(id, true, true, (c, s, host) -> {
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.addProperty("Name", s.name);
            if (Boolean.TRUE.equals(force)) {
                for (String sql : new String[] {"DELETE FROM warp_awsparams_secret_versions WHERE name = ?", "DELETE FROM warp_awsparams_secrets WHERE name = ?"}) {
                    try (PreparedStatement ps = c.prepareStatement(sql)) {
                        ps.setString(1, s.name);
                        ps.executeUpdate();
                    }
                }
                out.addProperty("DeletionDate", epoch(new Timestamp(System.currentTimeMillis())));
            } else {
                long days = window == null ? 30 : window;
                Timestamp when = Timestamp.from(Instant.now().plusSeconds(days * 86400));
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET deleted_at = now(), delete_after = ? WHERE name = ?")) {
                    ps.setTimestamp(1, when);
                    ps.setString(2, s.name);
                    ps.executeUpdate();
                }
                out.addProperty("DeletionDate", epoch(when));
            }
            return out;
        });
    }

    private JsonObject restoreSecret(JsonObject req) {
        return withSecret(Args.req(req, "SecretId"), true, true, (c, s, host) -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET deleted_at = NULL, delete_after = NULL WHERE name = ?")) {
                ps.setString(1, s.name);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.addProperty("Name", s.name);
            return out;
        });
    }

    private JsonObject listVersions(JsonObject req) {
        boolean deprecated = Boolean.TRUE.equals(Args.bool(req, "IncludeDeprecated"));
        Integer maxArg = Args.integer(req, "MaxResults");
        int max = maxArg == null ? 100 : Math.max(1, Math.min(maxArg, 100));
        String token = Args.str(req, "NextToken");
        return withSecret(Args.req(req, "SecretId"), false, true, (c, s, host) -> {
            JsonArray a = new JsonArray();
            boolean skipping = token != null;
            boolean more = false;
            String last = null;
            String kmsArn = s.kmsKeyId;
            for (Version v : versions(c, s.name)) {
                if (skipping) {
                    if (v.id.equals(token)) {
                        skipping = false;
                    }
                    continue;
                }
                if (v.stages.isEmpty() && !deprecated) {
                    continue;
                }
                if (a.size() >= max) {
                    more = true;
                    break;
                }
                JsonObject o = new JsonObject();
                o.addProperty("VersionId", v.id);
                JsonArray st = new JsonArray();
                v.stages.forEach(st::add);
                o.add("VersionStages", st);
                o.addProperty("CreatedDate", epoch(v.created));
                if (kmsArn != null) {
                    JsonArray k = new JsonArray();
                    k.add(kmsArn);
                    o.add("KmsKeyIds", k);
                }
                a.add(o);
                last = v.id;
            }
            JsonObject out = new JsonObject();
            out.add("Versions", a);
            out.addProperty("ARN", s.arn);
            out.addProperty("Name", s.name);
            if (more) {
                out.addProperty("NextToken", last);
            }
            return out;
        });
    }

    private JsonObject updateStage(JsonObject req) {
        String stage = Args.req(req, "VersionStage");
        String remove = Args.str(req, "RemoveFromVersionId");
        String move = Args.str(req, "MoveToVersionId");
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            List<Version> vs = versions(c, s.name);
            Version from = null, to = null;
            for (Version v : vs) {
                if (v.id.equals(remove)) {
                    from = v;
                }
                if (v.id.equals(move)) {
                    to = v;
                }
            }
            if ((remove != null && from == null) || (move != null && to == null)) {
                throw new AwsException(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret version.");
            }
            if (remove != null && !from.stages.contains(stage)) {
                throw invalidParam("The staging label " + stage + " is not attached to version " + remove);
            }
            if (remove != null) {
                List<String> st = new ArrayList<>(from.stages);
                st.remove(stage);
                saveStages(c, s.name, from.id, st);
            }
            if (move != null) {
                for (Version v : vs) {
                    if (v.stages.contains(stage) && !v.id.equals(remove)) {
                        List<String> st = new ArrayList<>(v.stages);
                        st.remove(stage);
                        if (stage.equals("AWSCURRENT") && !st.contains("AWSPREVIOUS")) {
                            st.add("AWSPREVIOUS");
                        }
                        saveStages(c, s.name, v.id, st);
                    }
                }
                List<String> st = new ArrayList<>(to.stages);
                if (!st.contains(stage)) {
                    st.add(stage);
                }
                saveStages(c, s.name, to.id, st);
            }
            touch(c, s.name);
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.addProperty("Name", s.name);
            return out;
        });
    }

    private JsonObject randomPassword(JsonObject req) {
        Long lenArg = Args.lng(req, "PasswordLength");
        int len = lenArg == null ? 32 : lenArg.intValue();
        if (len < 4 || len > 4096) {
            throw invalidParam("Password length must be between 4 and 4096.");
        }
        String exclude = Args.str(req, "ExcludeCharacters") == null ? "" : Args.str(req, "ExcludeCharacters");
        List<String> classes = new ArrayList<>();
        if (!Boolean.TRUE.equals(Args.bool(req, "ExcludeLowercase"))) {
            classes.add("abcdefghijklmnopqrstuvwxyz");
        }
        if (!Boolean.TRUE.equals(Args.bool(req, "ExcludeUppercase"))) {
            classes.add("ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        }
        if (!Boolean.TRUE.equals(Args.bool(req, "ExcludeNumbers"))) {
            classes.add("0123456789");
        }
        if (!Boolean.TRUE.equals(Args.bool(req, "ExcludePunctuation"))) {
            classes.add("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
        }
        if (Boolean.TRUE.equals(Args.bool(req, "IncludeSpace"))) {
            classes.add(" ");
        }
        List<String> pools = new ArrayList<>();
        for (String c : classes) {
            StringBuilder sb = new StringBuilder();
            for (char ch : c.toCharArray()) {
                if (exclude.indexOf(ch) < 0) {
                    sb.append(ch);
                }
            }
            if (sb.length() > 0) {
                pools.add(sb.toString());
            }
        }
        if (pools.isEmpty()) {
            throw invalidParam("The password can't be generated using the specified parameters.");
        }
        boolean requireEach = !Boolean.FALSE.equals(Args.bool(req, "RequireEachIncludedType"));
        char[] out = new char[len];
        String all = String.join("", pools);
        for (int i = 0; i < len; i++) {
            out[i] = all.charAt(RANDOM.nextInt(all.length()));
        }
        if (requireEach && pools.size() <= len) {
            List<Integer> pos = new ArrayList<>();
            for (int i = 0; i < len; i++) {
                pos.add(i);
            }
            java.util.Collections.shuffle(pos, RANDOM);
            for (int i = 0; i < pools.size(); i++) {
                String p = pools.get(i);
                out[pos.get(i)] = p.charAt(RANDOM.nextInt(p.length()));
            }
        }
        JsonObject o = new JsonObject();
        o.addProperty("RandomPassword", new String(out));
        return o;
    }

    private JsonObject tag(JsonObject req, boolean add) {
        Map<String, String> tags = new LinkedHashMap<>();
        for (JsonObject t : Args.objects(req, "Tags")) {
            tags.put(Args.str(t, "Key"), Args.str(t, "Value") == null ? "" : Args.str(t, "Value"));
        }
        List<String> remove = Args.strings(req, "TagKeys");
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            if (add) {
                s.tags.putAll(tags);
            } else {
                remove.forEach(s.tags::remove);
            }
            if (s.tags.size() > 50) {
                throw new AwsException(400, "LimitExceededException", "Tag limit exceeded");
            }
            JsonObject t = new JsonObject();
            s.tags.forEach(t::addProperty);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET tags = ?::jsonb WHERE name = ?")) {
                ps.setString(1, t.toString());
                ps.setString(2, s.name);
                ps.executeUpdate();
            }
            return new JsonObject();
        });
    }

    private JsonObject arnName(Secret s) {
        JsonObject o = new JsonObject();
        o.addProperty("ARN", s.arn);
        o.addProperty("Name", s.name);
        return o;
    }

    private JsonObject putPolicy(JsonObject req) {
        String policy = Args.req(req, "ResourcePolicy");
        try {
            JsonParser.parseString(policy).getAsJsonObject();
        } catch (RuntimeException e) {
            throw new AwsException(400, "MalformedPolicyDocumentException", "This resource policy contains invalid JSON.");
        }
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET policy = ? WHERE name = ?")) {
                ps.setString(1, policy);
                ps.setString(2, s.name);
                ps.executeUpdate();
            }
            return arnName(s);
        });
    }

    private JsonObject getPolicy(JsonObject req) {
        return withSecret(Args.req(req, "SecretId"), false, false, (c, s, host) -> {
            JsonObject o = arnName(s);
            if (s.policy != null) {
                o.addProperty("ResourcePolicy", s.policy);
            }
            return o;
        });
    }

    private JsonObject deletePolicy(JsonObject req) {
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET policy = NULL WHERE name = ?")) {
                ps.setString(1, s.name);
                ps.executeUpdate();
            }
            return arnName(s);
        });
    }

    private JsonObject validatePolicy(JsonObject req) {
        JsonObject o = new JsonObject();
        String policy = Args.req(req, "ResourcePolicy");
        JsonArray errors = new JsonArray();
        try {
            JsonParser.parseString(policy).getAsJsonObject();
        } catch (RuntimeException e) {
            JsonObject err = new JsonObject();
            err.addProperty("CheckName", "SYNTAX_ERROR");
            err.addProperty("ErrorMessage", "This resource policy contains invalid JSON.");
            errors.add(err);
        }
        o.addProperty("PolicyValidationPassed", errors.size() == 0);
        o.add("ValidationErrors", errors);
        return o;
    }

    private JsonObject rotateSecret(JsonObject req) {
        String lambda = Args.str(req, "RotationLambdaARN");
        JsonObject rules = Args.obj(req, "RotationRules");
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            String l = lambda != null ? lambda : s.rotationLambda;
            JsonObject r = rules != null ? rules : s.rotationRules;
            boolean enabled = l != null || r != null;
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET rotation_lambda = ?, rotation_rules = ?::jsonb, "
                    + "rotation_enabled = ?, last_rotated = now(), last_changed = now() WHERE name = ?")) {
                ps.setString(1, l);
                ps.setString(2, r == null ? null : r.toString());
                ps.setBoolean(3, enabled);
                ps.setString(4, s.name);
                ps.executeUpdate();
            }
            JsonObject out = arnName(s);
            for (Version v : versions(c, s.name)) {
                if (v.stages.contains("AWSCURRENT")) {
                    out.addProperty("VersionId", v.id);
                }
            }
            return out;
        });
    }

    private JsonObject cancelRotate(JsonObject req) {
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET rotation_enabled = false WHERE name = ?")) {
                ps.setString(1, s.name);
                ps.executeUpdate();
            }
            return arnName(s);
        });
    }

    private JsonObject replicate(JsonObject req) {
        List<JsonObject> add = Args.objects(req, "AddReplicaRegions");
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            JsonArray reps = s.replicas.deepCopy();
            for (JsonObject r : add) {
                JsonObject st = new JsonObject();
                st.addProperty("Region", Args.str(r, "Region"));
                if (Args.str(r, "KmsKeyId") != null) {
                    st.addProperty("KmsKeyId", Args.str(r, "KmsKeyId"));
                }
                st.addProperty("Status", "InSync");
                st.addProperty("StatusMessage", "Replication succeeded");
                reps.add(st);
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET replicas = ?::jsonb WHERE name = ?")) {
                ps.setString(1, reps.toString());
                ps.setString(2, s.name);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.add("ReplicationStatus", reps);
            return out;
        });
    }

    private JsonObject removeRegions(JsonObject req) {
        List<String> remove = Args.strings(req, "RemoveReplicaRegions");
        return withSecret(Args.req(req, "SecretId"), true, false, (c, s, host) -> {
            JsonArray reps = new JsonArray();
            for (JsonElement e : s.replicas) {
                if (!remove.contains(e.getAsJsonObject().get("Region").getAsString())) {
                    reps.add(e);
                }
            }
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_secrets SET replicas = ?::jsonb WHERE name = ?")) {
                ps.setString(1, reps.toString());
                ps.setString(2, s.name);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("ARN", s.arn);
            out.add("ReplicationStatus", reps);
            return out;
        });
    }

    private JsonObject nameOnly(JsonObject req) {
        return withSecret(Args.req(req, "SecretId"), false, false, (c, s, host) -> {
            JsonObject o = new JsonObject();
            o.addProperty("ARN", s.arn);
            return o;
        });
    }
}

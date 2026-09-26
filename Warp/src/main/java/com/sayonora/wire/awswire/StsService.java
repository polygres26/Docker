// Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors.
// See Warp/NOTICE for the licence text.
package com.sayonora.wire.awswire;

import com.google.gson.JsonObject;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AWS STS (basic), Query protocol only (what every SDK uses): GetCallerIdentity, AssumeRole, AssumeRoleWithWebIdentity,
 * GetSessionToken, GetFederationToken, DecodeAuthorizationMessage and GetAccessKeyInfo. Temporary credentials
 * ({@code ASIA...} key, secret, session token) are stored in the {@code awsparams} store, and the unified endpoint's SigV4
 * validation ({@code WARP_AWS_IAM_CREDENTIALS}) accepts them until they expire. AssumeRole and AssumeRoleWithWebIdentity do not look
 * the role up (its trust policy and any session policy are not evaluated, the web-identity token is not verified), so any role
 * ARN can be assumed. AssumeRoleWithSAML is validated (XML signature, issuer, validity, audience, role attribute) against a SAML provider registered
 * through IAM's CreateSAMLProvider and the trust policy of an IAM role created with CreateRole (see {@link IamService}).
 */
public final class StsService extends AwsService {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    private static final String ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    private final AwsRuntime rt;
    private final AwsShards sh;
    private final ConcurrentHashMap<String, Object[]> cache = new ConcurrentHashMap<>();

    public StsService(AwsRuntime rt) {
        this.rt = rt;
        this.sh = new AwsShards(rt.registry, StoreType.AWSPARAMS);
    }

    @Override
    public String id() {
        return "sts";
    }

    @Override
    public String metricsProtocol() {
        return "stswire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("sts");
    }

    @Override
    public Set<String> targetPrefixes() {
        return Set.of();
    }

    @Override
    public boolean supportsQuery() {
        return true;
    }

    @Override
    public String xmlNamespace() {
        return "https://sts.amazonaws.com/doc/2011-06-15/";
    }

    @Override
    public Set<String> queryActions() {
        return Set.of("GetCallerIdentity", "AssumeRole", "AssumeRoleWithWebIdentity", "AssumeRoleWithSAML", "GetSessionToken",
                "GetFederationToken", "DecodeAuthorizationMessage", "GetAccessKeyInfo");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return op.startsWith("Get") || op.startsWith("Decode") ? SqlMetricsCollector.StatementKind.READ : SqlMetricsCollector.StatementKind.WRITE;
    }

    /** The secret and token of an unexpired temporary credential, or null. */
    AwsSigV4.Secret session(String accessKey) {
        Object[] hit = cache.get(accessKey);
        if (hit != null && (Long) hit[2] > System.currentTimeMillis()) {
            return (AwsSigV4.Secret) hit[0];
        }
        if (!sh.available()) {
            return null;
        }
        Object[] row = sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT secret, token, principal FROM warp_awsparams_sts_sessions WHERE access_key = ? "
                    + "AND expires_at > now()")) {
                ps.setString(1, accessKey);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? new Object[] {new AwsSigV4.Secret(rs.getString(1), rs.getString(2)), rs.getString(3)} : null;
                }
            }
        });
        if (row == null) {
            cache.remove(accessKey);
            return null;
        }
        cache.put(accessKey, new Object[] {row[0], row[1], System.currentTimeMillis() + 5000});
        return (AwsSigV4.Secret) row[0];
    }

    private String principalOf(String accessKey) {
        if (accessKey != null && accessKey.startsWith("ASIA") && sh.available()) {
            session(accessKey);
            Object[] hit = cache.get(accessKey);
            if (hit != null) {
                return (String) hit[1];
            }
        }
        return null;
    }

    /** Whether a role trust policy allows {@code federated} to perform {@code action} (Allow statements only). */
    static boolean trustAllows(String trustPolicy, String federated, String action) {
        try {
            com.google.gson.JsonElement st = com.google.gson.JsonParser.parseString(trustPolicy).getAsJsonObject().get("Statement");
            java.util.List<com.google.gson.JsonElement> list = new java.util.ArrayList<>();
            if (st.isJsonArray()) {
                st.getAsJsonArray().forEach(list::add);
            } else {
                list.add(st);
            }
            for (com.google.gson.JsonElement e : list) {
                JsonObject s = e.getAsJsonObject();
                if (!"Allow".equals(s.has("Effect") ? s.get("Effect").getAsString() : "")) {
                    continue;
                }
                JsonObject principal = s.has("Principal") && s.get("Principal").isJsonObject() ? s.getAsJsonObject("Principal") : null;
                boolean who = false;
                if (principal != null && principal.has("Federated")) {
                    com.google.gson.JsonElement f = principal.get("Federated");
                    who = f.isJsonArray() ? f.getAsJsonArray().contains(new com.google.gson.JsonPrimitive(federated))
                            : f.getAsString().equals(federated);
                }
                com.google.gson.JsonElement act = s.get("Action");
                boolean what = act != null && (act.isJsonArray() ? act.getAsJsonArray().contains(new com.google.gson.JsonPrimitive(action))
                        : act.getAsString().equals(action) || act.getAsString().equals("sts:*"));
                if (who && what) {
                    return true;
                }
            }
        } catch (RuntimeException ignored) {
            // an unreadable trust policy trusts nothing
        }
        return false;
    }

    private static String rand(String alphabet, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(alphabet.charAt(RANDOM.nextInt(alphabet.length())));
        }
        return sb.toString();
    }

    private static AwsException missing(String param) {
        return new AwsException(400, "MissingParameter", "The request must contain the parameter " + param);
    }

    private static int duration(JsonObject req, int def, int min, int max) {
        Integer d = Args.integer(req, "DurationSeconds");
        if (d == null) {
            return def;
        }
        if (d < min) {
            throw new AwsException(400, "ValidationError", "1 validation error detected: Value '" + d
                    + "' at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to " + min);
        }
        if (d > max) {
            throw new AwsException(400, "ValidationError", "1 validation error detected: Value '" + d
                    + "' at 'durationSeconds' failed to satisfy constraint: Member must have value less than or equal to " + max);
        }
        return d;
    }

    private JsonObject credentials(String principal, int seconds) {
        String key = "ASIA" + rand(UPPER, 16);
        String secret = rand(ALNUM, 40);
        byte[] tokenBytes = new byte[192];
        RANDOM.nextBytes(tokenBytes);
        String token = "FwoGZXIvYXdzE" + Base64.getEncoder().withoutPadding().encodeToString(tokenBytes);
        Instant exp = Instant.now().plusSeconds(seconds);
        if (sh.available()) {
            sh.conn(sh.home(), c -> {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_sts_sessions (access_key, secret, token, expires_at, principal) "
                        + "VALUES (?, ?, ?, ?, ?)")) {
                    ps.setString(1, key);
                    ps.setString(2, secret);
                    ps.setString(3, token);
                    ps.setTimestamp(4, Timestamp.from(exp));
                    ps.setString(5, principal);
                    ps.executeUpdate();
                }
                try (var st = c.createStatement()) {
                    st.executeUpdate("DELETE FROM warp_awsparams_sts_sessions WHERE expires_at < now() - interval '1 hour'");
                }
                return null;
            });
        } else if (rt.config.credentials.isEnabled()) {
            throw new AwsException(503, "ServiceUnavailable", "The awsparams store is not enabled, so temporary credentials cannot be issued "
                    + "while WARP_AWS_IAM_CREDENTIALS is set");
        }
        JsonObject c = new JsonObject();
        c.addProperty("AccessKeyId", key);
        c.addProperty("SecretAccessKey", secret);
        c.addProperty("SessionToken", token);
        c.addProperty("Expiration", exp.toString().replaceAll("\\.\\d+Z$", "Z"));
        return c;
    }

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        String acct = rt.config.accountId;
        switch (op) {
            case "GetCallerIdentity": {
                JsonObject o = new JsonObject();
                String principal = principalOf(call.accessKey());
                if (principal == null) {
                    o.addProperty("UserId", acct);
                    o.addProperty("Account", acct);
                    o.addProperty("Arn", "arn:aws:iam::" + acct + ":root");
                } else {
                    o.addProperty("UserId", "AROA" + rand(UPPER, 17) + ":" + principal.substring(principal.lastIndexOf('/') + 1));
                    o.addProperty("Account", acct);
                    o.addProperty("Arn", principal);
                }
                return o;
            }
            case "AssumeRole":
            case "AssumeRoleWithWebIdentity": {
                String roleArn = Args.str(req, "RoleArn");
                if (roleArn == null) {
                    throw missing("RoleArn");
                }
                String session = Args.str(req, "RoleSessionName");
                if (session == null) {
                    throw missing("RoleSessionName");
                }
                if (op.equals("AssumeRoleWithWebIdentity") && Args.str(req, "WebIdentityToken") == null) {
                    throw missing("WebIdentityToken");
                }
                if (!session.matches("^[\\w+=,.@-]{2,64}$")) {
                    throw new AwsException(400, "ValidationError", "1 validation error detected: Value '" + session
                            + "' at 'roleSessionName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*");
                }
                String[] p = roleArn.split(":");
                if (p.length < 6 || !p[2].equals("iam") || !p[5].startsWith("role/")) {
                    throw new AwsException(400, "ValidationError", "Invalid RoleArn: " + roleArn);
                }
                String roleName = p[5].substring(p[5].lastIndexOf('/') + 1);
                String assumedArn = "arn:aws:sts::" + p[4] + ":assumed-role/" + roleName + "/" + session;
                JsonObject o = new JsonObject();
                o.add("Credentials", credentials(assumedArn, op.equals("AssumeRole") ? duration(req, 3600, 900, 43200)
                        : duration(req, 3600, 900, 43200)));
                JsonObject u = new JsonObject();
                u.addProperty("AssumedRoleId", "AROA" + rand(UPPER, 17) + ":" + session);
                u.addProperty("Arn", assumedArn);
                o.add("AssumedRoleUser", u);
                o.addProperty("PackedPolicySize", 0);
                if (op.equals("AssumeRoleWithWebIdentity")) {
                    o.addProperty("SubjectFromWebIdentityToken", "warp-subject");
                    o.addProperty("Provider", Args.str(req, "ProviderId") == null ? "warp-web-identity" : Args.str(req, "ProviderId"));
                    o.addProperty("Audience", "warp-audience");
                }
                return o;
            }
            case "GetSessionToken": {
                JsonObject o = new JsonObject();
                o.add("Credentials", credentials("arn:aws:iam::" + acct + ":root", duration(req, 43200, 900, 129600)));
                return o;
            }
            case "GetFederationToken": {
                String name = Args.str(req, "Name");
                if (name == null) {
                    throw missing("Name");
                }
                if (name.length() < 2 || name.length() > 32 || !name.matches("^[\\w+=,.@-]+$")) {
                    throw new AwsException(400, "ValidationError", "1 validation error detected: Value '" + name
                            + "' at 'name' failed to satisfy constraint: Member must have length between 2 and 32");
                }
                String fed = "arn:aws:sts::" + acct + ":federated-user/" + name;
                JsonObject o = new JsonObject();
                o.add("Credentials", credentials(fed, duration(req, 43200, 900, 129600)));
                JsonObject u = new JsonObject();
                u.addProperty("FederatedUserId", acct + ":" + name);
                u.addProperty("Arn", fed);
                o.add("FederatedUser", u);
                o.addProperty("PackedPolicySize", 0);
                return o;
            }
            case "DecodeAuthorizationMessage": {
                String m = Args.str(req, "EncodedMessage");
                if (m == null) {
                    throw missing("EncodedMessage");
                }
                JsonObject o = new JsonObject();
                o.addProperty("DecodedMessage", m);
                return o;
            }
            case "GetAccessKeyInfo": {
                if (Args.str(req, "AccessKeyId") == null) {
                    throw missing("AccessKeyId");
                }
                JsonObject o = new JsonObject();
                o.addProperty("Account", acct);
                return o;
            }
            case "AssumeRoleWithSAML": {
                String roleArn = Args.str(req, "RoleArn");
                String principal = Args.str(req, "PrincipalArn");
                String assertion = Args.str(req, "SAMLAssertion");
                if (roleArn == null) {
                    throw missing("RoleArn");
                }
                if (principal == null) {
                    throw missing("PrincipalArn");
                }
                if (assertion == null) {
                    throw missing("SAMLAssertion");
                }
                IamService iam = (IamService) rt.service("iam");
                String metadata = iam.samlMetadata(principal);
                if (metadata == null) {
                    throw new AwsException(400, "InvalidIdentityToken", "The SAML provider " + principal + " does not exist");
                }
                String trust = iam.trustPolicy(roleArn);
                if (trust == null) {
                    throw new AwsException(403, "AccessDenied", "Role " + roleArn + " does not exist");
                }
                if (!trustAllows(trust, principal, "sts:AssumeRoleWithSAML")) {
                    throw new AwsException(403, "AccessDenied", "Not authorized to perform sts:AssumeRoleWithSAML");
                }
                SamlValidator.Assertion a;
                try {
                    a = SamlValidator.validate(assertion, metadata, roleArn, principal, Instant.now());
                } catch (SamlValidator.Invalid e) {
                    throw new AwsException(400, e.code, e.getMessage());
                }
                String roleName = roleArn.substring(roleArn.lastIndexOf('/') + 1);
                String session = a.sessionName().replaceAll("[^\\w+=,.@-]", "-");
                String assumedArn = "arn:aws:sts::" + acct + ":assumed-role/" + roleName + "/" + session;
                JsonObject o = new JsonObject();
                o.add("Credentials", credentials(assumedArn, duration(req, 3600, 900, 43200)));
                JsonObject u = new JsonObject();
                u.addProperty("AssumedRoleId", "AROA" + rand(UPPER, 17) + ":" + session);
                u.addProperty("Arn", assumedArn);
                o.add("AssumedRoleUser", u);
                o.addProperty("PackedPolicySize", 0);
                o.addProperty("Subject", a.subject());
                o.addProperty("SubjectType", "persistent");
                o.addProperty("Issuer", "warp-saml");
                o.addProperty("Audience", "https://signin.aws.amazon.com/saml");
                o.addProperty("NameQualifier", "warp");
                return o;
            }
            default:
                throw new AwsException(400, "InvalidAction", "Could not find operation " + op + " for version 2011-06-15");
        }
    }
}

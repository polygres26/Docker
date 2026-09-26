package com.sayonora.wire.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The slice of IAM that STS needs, Query protocol, in the {@code awsparams} store (first host): roles (name, path, trust
 * policy) and SAML providers (name, metadata document). There are no users, groups, policies or access keys, and nothing is
 * authorised against a role: STS reads a role's trust policy only for AssumeRoleWithSAML.
 */
public final class IamService extends AwsService {

    private static final Pattern ROLE = Pattern.compile("^[\\w+=,.@-]{1,64}$");
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    private final AwsRuntime rt;
    private final AwsShards sh;

    public IamService(AwsRuntime rt) {
        this.rt = rt;
        this.sh = new AwsShards(rt.registry, StoreType.AWSPARAMS);
    }

    @Override
    public String id() {
        return "iam";
    }

    @Override
    public String metricsProtocol() {
        return "iamwire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("iam");
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
        return "https://iam.amazonaws.com/doc/2010-05-08/";
    }

    @Override
    public Set<String> queryActions() {
        return Set.of("CreateRole", "GetRole", "DeleteRole", "ListRoles", "CreateSAMLProvider", "GetSAMLProvider", "DeleteSAMLProvider",
                "ListSAMLProviders");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return op.startsWith("Get") || op.startsWith("List") ? SqlMetricsCollector.StatementKind.READ : SqlMetricsCollector.StatementKind.WRITE;
    }

    private static AwsException noSuchEntity(String what) {
        return new AwsException(404, "NoSuchEntity", what);
    }

    private static String iso(Timestamp t) {
        return t.toInstant().toString().replaceAll("\\.\\d+Z$", "Z");
    }

    private JsonObject roleJson(String name, String arn, String roleId, String path, String trust, String desc, Timestamp created) {
        JsonObject r = new JsonObject();
        r.addProperty("Path", path);
        r.addProperty("RoleName", name);
        r.addProperty("RoleId", roleId);
        r.addProperty("Arn", arn);
        r.addProperty("CreateDate", iso(created));
        r.addProperty("AssumeRolePolicyDocument", URLEncoder.encode(trust, StandardCharsets.UTF_8));
        if (desc != null) {
            r.addProperty("Description", desc);
        }
        return r;
    }

    /** The trust policy of a role named by ARN or name, or null when it does not exist. */
    String trustPolicy(String roleRef) {
        String name = roleRef.substring(roleRef.lastIndexOf('/') + 1);
        return sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT trust_policy FROM warp_awsparams_iam_roles WHERE name = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
    }

    String samlMetadata(String providerArn) {
        String name = providerArn.substring(providerArn.lastIndexOf('/') + 1);
        return sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT metadata FROM warp_awsparams_iam_saml WHERE name = ?")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : null;
                }
            }
        });
    }

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        String acct = rt.config.accountId;
        switch (op) {
            case "CreateRole": {
                String name = Args.req(req, "RoleName");
                if (!ROLE.matcher(name).matches()) {
                    throw new AwsException(400, "ValidationError", "1 validation error detected: Value '" + name
                            + "' at 'roleName' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]+");
                }
                String trust = Args.req(req, "AssumeRolePolicyDocument");
                try {
                    JsonParser.parseString(trust).getAsJsonObject();
                } catch (RuntimeException e) {
                    throw new AwsException(400, "MalformedPolicyDocument", "Syntax errors in policy.");
                }
                String path = Args.str(req, "Path") == null ? "/" : Args.str(req, "Path");
                String arn = "arn:aws:iam::" + acct + ":role" + (path.equals("/") ? "/" : path + (path.endsWith("/") ? "" : "/")) + name;
                String id = "AROA" + rand(17);
                String desc = Args.str(req, "Description");
                return sh.conn(sh.home(), c -> {
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_iam_roles (name, arn, role_id, path, trust_policy, "
                            + "description) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING created_at")) {
                        ps.setString(1, name);
                        ps.setString(2, arn);
                        ps.setString(3, id);
                        ps.setString(4, path);
                        ps.setString(5, trust);
                        ps.setString(6, desc);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) {
                                throw new AwsException(409, "EntityAlreadyExists", "Role with name " + name + " already exists.");
                            }
                            JsonObject o = new JsonObject();
                            o.add("Role", roleJson(name, arn, id, path, trust, desc, rs.getTimestamp(1)));
                            return o;
                        }
                    }
                });
            }
            case "GetRole": {
                String name = Args.req(req, "RoleName");
                return sh.conn(sh.home(), c -> {
                    try (PreparedStatement ps = c.prepareStatement("SELECT arn, role_id, path, trust_policy, description, created_at "
                            + "FROM warp_awsparams_iam_roles WHERE name = ?")) {
                        ps.setString(1, name);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (!rs.next()) {
                                throw noSuchEntity("The role with name " + name + " cannot be found.");
                            }
                            JsonObject o = new JsonObject();
                            o.add("Role", roleJson(name, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5),
                                    rs.getTimestamp(6)));
                            return o;
                        }
                    }
                });
            }
            case "DeleteRole": {
                String name = Args.req(req, "RoleName");
                sh.conn(sh.home(), c -> {
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_iam_roles WHERE name = ?")) {
                        ps.setString(1, name);
                        if (ps.executeUpdate() == 0) {
                            throw noSuchEntity("The role with name " + name + " cannot be found.");
                        }
                    }
                    return null;
                });
                return new JsonObject();
            }
            case "ListRoles":
                return sh.conn(sh.home(), c -> {
                    JsonArray a = new JsonArray();
                    try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT name, arn, role_id, path, trust_policy, description, "
                            + "created_at FROM warp_awsparams_iam_roles ORDER BY name")) {
                        while (rs.next()) {
                            a.add(roleJson(rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getString(6),
                                    rs.getTimestamp(7)));
                        }
                    }
                    JsonObject o = new JsonObject();
                    o.add("Roles", a);
                    o.addProperty("IsTruncated", false);
                    return o;
                });
            case "CreateSAMLProvider": {
                String name = Args.req(req, "Name");
                String metadata = Args.req(req, "SAMLMetadataDocument");
                try {
                    SamlValidator.metadataKey(metadata);
                } catch (Exception e) {
                    throw new AwsException(400, "InvalidInput", "Invalid SAML metadata document: " + e.getMessage());
                }
                String arn = "arn:aws:iam::" + acct + ":saml-provider/" + name;
                return sh.conn(sh.home(), c -> {
                    try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_iam_saml (name, arn, metadata) VALUES (?, ?, ?) "
                            + "ON CONFLICT DO NOTHING")) {
                        ps.setString(1, name);
                        ps.setString(2, arn);
                        ps.setString(3, metadata);
                        if (ps.executeUpdate() == 0) {
                            throw new AwsException(409, "EntityAlreadyExists", "Provider with name " + name + " already exists.");
                        }
                    }
                    JsonObject o = new JsonObject();
                    o.addProperty("SAMLProviderArn", arn);
                    return o;
                });
            }
            case "GetSAMLProvider": {
                String arn = Args.req(req, "SAMLProviderArn");
                String meta = samlMetadata(arn);
                if (meta == null) {
                    throw noSuchEntity("SAMLProvider " + arn + " not found");
                }
                JsonObject o = new JsonObject();
                o.addProperty("SAMLMetadataDocument", meta);
                return o;
            }
            case "DeleteSAMLProvider": {
                String arn = Args.req(req, "SAMLProviderArn");
                sh.conn(sh.home(), c -> {
                    try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_iam_saml WHERE arn = ?")) {
                        ps.setString(1, arn);
                        if (ps.executeUpdate() == 0) {
                            throw noSuchEntity("SAMLProvider " + arn + " not found");
                        }
                    }
                    return null;
                });
                return new JsonObject();
            }
            case "ListSAMLProviders":
                return sh.conn(sh.home(), c -> {
                    JsonArray a = new JsonArray();
                    try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT arn, created_at FROM warp_awsparams_iam_saml ORDER BY name")) {
                        while (rs.next()) {
                            JsonObject p = new JsonObject();
                            p.addProperty("Arn", rs.getString(1));
                            p.addProperty("CreateDate", iso(rs.getTimestamp(2)));
                            a.add(p);
                        }
                    }
                    JsonObject o = new JsonObject();
                    o.add("SAMLProviderList", a);
                    return o;
                });
            default:
                throw new AwsException(400, "InvalidAction", "IAM operation " + op + " is not supported by Warp (only roles and SAML providers)");
        }
    }

    private static String rand(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            sb.append(UPPER.charAt(RANDOM.nextInt(UPPER.length())));
        }
        return sb.toString();
    }
}

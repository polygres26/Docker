package com.sayonora.wire.awswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.core.SqlMetricsCollector;
import com.sayonora.wire.core.StoreType;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * AWS Systems Manager Parameter Store on Postgres (the {@code awsparams} store), JSON 1.1, plus the Run Command bookkeeping
 * calls Floci's suite exercises (recorded, never executed). A parameter (head row plus one history row per version) lives on
 * the host owning hash(parameter name); GetParametersByPath, DescribeParameters and label lookups fan out. {@code SecureString}
 * values are sealed with a KMS key ({@code alias/aws/ssm} by default) through {@link KmsService}, so they need the KMS master
 * key. Parameter policies are stored and echoed by DescribeParameters, not enforced (nothing expires).
 */
public final class SsmService extends AwsService {

    private static final Pattern NAME = Pattern.compile("^[a-zA-Z0-9_.\\-/]{1,2048}$");
    private static final Pattern LABEL = Pattern.compile("^[a-zA-Z0-9_.\\-]{1,100}$");
    private static final Set<String> TYPES = Set.of("String", "StringList", "SecureString");

    private final AwsRuntime rt;
    private final KmsService kms;
    private final AwsShards sh;

    public SsmService(AwsRuntime rt, KmsService kms) {
        this.rt = rt;
        this.kms = kms;
        this.sh = new AwsShards(rt.registry, StoreType.AWSPARAMS);
    }

    @Override
    public String id() {
        return "ssm";
    }

    @Override
    public String metricsProtocol() {
        return "ssmwire";
    }

    @Override
    public Set<String> signingNames() {
        return Set.of("ssm");
    }

    @Override
    public Set<String> targetPrefixes() {
        return Set.of("AmazonSSM.");
    }

    @Override
    public boolean available() {
        return sh.available();
    }

    @Override
    public SqlMetricsCollector.StatementKind kindOf(String op) {
        return op.startsWith("Get") || op.startsWith("List") || op.startsWith("Describe") ? SqlMetricsCollector.StatementKind.READ
                : SqlMetricsCollector.StatementKind.WRITE;
    }

    @Override
    public String backendLabel(String op, JsonObject req) {
        try {
            String n = req == null ? null : Args.str(req, "Name");
            return n == null || !sh.available() ? "default" : sh.owner(stripSelector(n)[0]);
        } catch (RuntimeException e) {
            return "default";
        }
    }

    // ---------------------------------------------------------------------------------------------- model

    private record Param(String name, String type, long version, byte[] value, String keyId, String description, String dataType,
            String tier, String policies, String allowedPattern, Map<String, String> tags, Timestamp modified, String modifiedBy) {
    }

    private record Hist(String name, String type, long version, byte[] value, String keyId, String description, String dataType,
            String tier, String policies, String allowedPattern, List<String> labels, Timestamp modified, String modifiedBy) {
    }

    private static final String PARAM_COLS = "name, type, version, value, key_id, description, data_type, tier, policies, allowed_pattern, "
            + "tags::text, modified_at, modified_by";
    private static final String HIST_COLS = "name, type, version, value, key_id, description, data_type, tier, policies, allowed_pattern, "
            + "labels::text, modified_at, modified_by";

    private static Param readParam(ResultSet rs) throws SQLException {
        Map<String, String> tags = new TreeMap<>();
        JsonParser.parseString(rs.getString(11)).getAsJsonObject().entrySet().forEach(e -> tags.put(e.getKey(), e.getValue().getAsString()));
        return new Param(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getBytes(4), rs.getString(5), rs.getString(6),
                rs.getString(7), rs.getString(8), rs.getString(9), rs.getString(10), tags, rs.getTimestamp(12), rs.getString(13));
    }

    private static Hist readHist(ResultSet rs) throws SQLException {
        List<String> labels = new ArrayList<>();
        JsonParser.parseString(rs.getString(11)).getAsJsonArray().forEach(e -> labels.add(e.getAsString()));
        return new Hist(rs.getString(1), rs.getString(2), rs.getLong(3), rs.getBytes(4), rs.getString(5), rs.getString(6), rs.getString(7),
                rs.getString(8), rs.getString(9), rs.getString(10), labels, rs.getTimestamp(12), rs.getString(13));
    }

    private static AwsException notFound() {
        return new AwsException(400, "ParameterNotFound", "");
    }

    private static AwsException validation(String m) {
        return new AwsException(400, "ValidationException", m);
    }

    private static double epoch(Timestamp t) {
        return t == null ? 0 : t.getTime() / 1000.0;
    }

    /** Splits {@code name[:selector]} (also accepting a parameter ARN). */
    private String[] stripSelector(String ref) {
        String n = ref;
        if (n.startsWith("arn:")) {
            int i = n.indexOf(":parameter");
            if (i < 0) {
                throw validation("Invalid parameter ARN " + ref);
            }
            n = n.substring(i + 10);
        }
        int colon = n.lastIndexOf(':');
        if (colon > 0) {
            return new String[] {n.substring(0, colon), n.substring(colon + 1)};
        }
        return new String[] {n, null};
    }

    private String arn(String name) {
        return rt.config.arn("ssm", "parameter" + (name.startsWith("/") ? name : "/" + name));
    }

    private static void checkName(String name) {
        if (name == null || !NAME.matcher(name).matches()) {
            throw validation("Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive) and must use only allowed "
                    + "characters (a-z, A-Z, 0-9, _ (underscore), . (period), - (hyphen), / (forward slash)).");
        }
        String flat = name.startsWith("/") ? name.substring(1) : name;
        String lower = flat.toLowerCase();
        if (lower.startsWith("aws") || lower.startsWith("ssm")) {
            throw validation("Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive).");
        }
        if (name.endsWith("/")) {
            throw validation("Parameter name: must not end with a slash.");
        }
        if (name.split("/").length > 16) {
            throw validation("Parameter name: hierarchy depth exceeds the maximum of 15 levels.");
        }
    }

    // ---------------------------------------------------------------------------------------------- dispatch

    @Override
    public JsonObject invoke(String op, JsonObject req, Call call) throws Exception {
        return switch (op) {
            case "PutParameter" -> putParameter(req);
            case "GetParameter" -> getParameter(req);
            case "GetParameters" -> getParameters(req);
            case "GetParametersByPath" -> getByPath(req);
            case "DeleteParameter" -> deleteParameter(req);
            case "DeleteParameters" -> deleteParameters(req);
            case "DescribeParameters" -> describeParameters(req);
            case "GetParameterHistory" -> history(req);
            case "LabelParameterVersion" -> label(req);
            case "UnlabelParameterVersion" -> unlabel(req);
            case "AddTagsToResource" -> tags(req, true);
            case "RemoveTagsFromResource" -> tags(req, false);
            case "ListTagsForResource" -> listTags(req);
            case "SendCommand" -> sendCommand(req);
            case "GetCommandInvocation" -> getInvocation(req);
            case "ListCommands" -> listCommands(req);
            case "ListCommandInvocations" -> listInvocations(req);
            case "CancelCommand" -> cancelCommand(req);
            case "GetServiceSetting" -> serviceSetting(req);
            case "UpdateServiceSetting", "ResetServiceSetting" -> new JsonObject();
            default -> throw new AwsException(400, "InvalidAction", "Unknown operation " + op);
        };
    }

    // ---------------------------------------------------------------------------------------------- put / get

    private JsonObject putParameter(JsonObject req) {
        String name = Args.req(req, "Name");
        checkName(name);
        String value = Args.str(req, "Value");
        if (value == null) {
            throw validation("1 validation error detected: Value null at 'value' failed to satisfy constraint: Member must not be null");
        }
        String type = Args.str(req, "Type");
        if (type != null && !TYPES.contains(type)) {
            throw validation("1 validation error detected: Value '" + type + "' at 'type' failed to satisfy constraint: Member must satisfy "
                    + "enum value set: [String, SecureString, StringList]");
        }
        boolean overwrite = Boolean.TRUE.equals(Args.bool(req, "Overwrite"));
        List<JsonObject> tagList = Args.objects(req, "Tags");
        if (overwrite && !tagList.isEmpty()) {
            throw new AwsException(400, "InvalidParameters", "Invalid request: tags and overwrite can't be used together. To create a "
                    + "parameter with tags, please remove overwrite flag. To update tags for an existing parameter, please use "
                    + "AddTagsToResource or RemoveTagsFromResource.");
        }
        String pattern = Args.str(req, "AllowedPattern");
        if (pattern != null && !Pattern.compile(pattern).matcher(value).matches()) {
            throw new AwsException(400, "ParameterPatternMismatchException", "Parameter value: " + value
                    + " for parameter name: " + name + " does not match : " + pattern);
        }
        String tierArg = Args.str(req, "Tier");
        String policies = Args.str(req, "Policies");
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > 8192 || (bytes > 4096 && "Standard".equals(tierArg))) {
            throw new AwsException(400, "ValidationException",
                    "Parameter value is too large: " + bytes + " bytes (Standard tier allows 4096, Advanced 8192).");
        }
        String tier = policies != null || bytes > 4096 || "Advanced".equals(tierArg) ? "Advanced" : "Standard";
        String dataType = Args.str(req, "DataType") == null ? "text" : Args.str(req, "DataType");
        String keyArg = Args.str(req, "KeyId");
        String existingKey = null;
        String effType = type;
        // decide type/key for a new or overwritten parameter, then seal outside any connection
        Param existing = sh.conn(sh.owner(name), c -> loadParam(c, name));
        if (existing != null && !overwrite) {
            throw new AwsException(400, "ParameterAlreadyExists", "The parameter already exists. To overwrite this value, set the "
                    + "overwrite option in the request to true.");
        }
        if (existing == null && type == null) {
            throw validation("A parameter type is required when you create a parameter.");
        }
        if (effType == null) {
            effType = existing.type;
        }
        if (existing != null && overwrite && !effType.equals(existing.type) && type != null) {
            throw new AwsException(400, "ValidationException",
                    "The parameter type cannot be changed on overwrite (existing " + existing.type + ")");
        }
        byte[] stored;
        String keyId = null;
        if (effType.equals("SecureString")) {
            String ref = keyArg != null ? keyArg : existing != null && existing.keyId != null ? existing.keyId
                    : kms.awsManagedKey("alias/aws/ssm", "Default key that protects my SSM Parameter Store parameters when no other "
                            + "key is defined");
            if (keyArg != null && !kms.canUse(keyArg)) {
                throw new AwsException(400, "InvalidKeyId", "The KeyId " + keyArg + " does not exist or is not accessible.");
            }
            stored = kms.encryptFor(ref, value.getBytes(StandardCharsets.UTF_8), Map.of());
            keyId = keyArg != null ? keyArg : ref.startsWith("alias/") ? ref : ref;
            if (keyArg == null && (existing == null || existing.keyId == null)) {
                keyId = "alias/aws/ssm";
            }
        } else {
            if (effType.equals("StringList")) {
                for (String part : value.split(",", -1)) {
                    if (part.isEmpty() && value.length() > 0) {
                        // AWS accepts empty items, keep as is
                        break;
                    }
                }
            }
            stored = value.getBytes(StandardCharsets.UTF_8);
        }
        String fType = effType;
        String fKey = keyId;
        Map<String, String> tags = new TreeMap<>();
        tagList.forEach(t -> tags.put(Args.str(t, "Key"), Args.str(t, "Value") == null ? "" : Args.str(t, "Value")));
        String desc = Args.str(req, "Description");
        String who = "arn:aws:iam::" + rt.config.accountId + ":root";
        return sh.tx(sh.owner(name), c -> {
            Param cur = loadParamLocked(c, name);
            if (cur != null && !overwrite) {
                throw new AwsException(400, "ParameterAlreadyExists", "The parameter already exists. To overwrite this value, set the "
                        + "overwrite option in the request to true.");
            }
            long version = cur == null ? 1 : cur.version + 1;
            JsonObject t = new JsonObject();
            (cur == null ? tags : cur.tags).forEach(t::addProperty);
            String d = desc != null ? desc : cur == null ? null : cur.description;
            String pol = policies != null ? policies : cur == null ? null : cur.policies;
            String pat = pattern != null ? pattern : cur == null ? null : cur.allowedPattern;
            String tr = cur != null && cur.tier.equals("Advanced") ? "Advanced" : tier;
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_ssm_params (name, type, version, value, key_id, description, "
                    + "data_type, tier, policies, allowed_pattern, tags, modified_at, modified_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, now(), ?) "
                    + "ON CONFLICT (name) DO UPDATE SET type = EXCLUDED.type, version = EXCLUDED.version, value = EXCLUDED.value, "
                    + "key_id = EXCLUDED.key_id, description = EXCLUDED.description, data_type = EXCLUDED.data_type, tier = EXCLUDED.tier, "
                    + "policies = EXCLUDED.policies, allowed_pattern = EXCLUDED.allowed_pattern, modified_at = now(), modified_by = EXCLUDED.modified_by")) {
                ps.setString(1, name);
                ps.setString(2, fType);
                ps.setLong(3, version);
                ps.setBytes(4, stored);
                ps.setString(5, fKey);
                ps.setString(6, d);
                ps.setString(7, dataType);
                ps.setString(8, tr);
                ps.setString(9, pol);
                ps.setString(10, pat);
                ps.setString(11, t.toString());
                ps.setString(12, who);
                ps.executeUpdate();
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_ssm_history (name, version, type, value, key_id, description, "
                    + "data_type, tier, policies, allowed_pattern, modified_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setString(1, name);
                ps.setLong(2, version);
                ps.setString(3, fType);
                ps.setBytes(4, stored);
                ps.setString(5, fKey);
                ps.setString(6, d);
                ps.setString(7, dataType);
                ps.setString(8, tr);
                ps.setString(9, pol);
                ps.setString(10, pat);
                ps.setString(11, who);
                ps.executeUpdate();
            }
            // keep at most 100 versions, oldest first, never dropping a labelled one
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_ssm_history WHERE name = ? AND version IN (SELECT version FROM "
                    + "warp_awsparams_ssm_history WHERE name = ? AND labels = '[]'::jsonb ORDER BY version DESC OFFSET 100)")) {
                ps.setString(1, name);
                ps.setString(2, name);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.addProperty("Version", version);
            out.addProperty("Tier", tr);
            return out;
        });
    }

    private Param loadParam(Connection c, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + PARAM_COLS + " FROM warp_awsparams_ssm_params WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readParam(rs) : null;
            }
        }
    }

    private Param loadParamLocked(Connection c, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT " + PARAM_COLS + " FROM warp_awsparams_ssm_params WHERE name = ? FOR UPDATE")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? readParam(rs) : null;
            }
        }
    }

    /** Resolves {@code name[:version|label]} to a concrete history row; null when it does not exist. */
    private Object[] resolve(String ref) {
        String[] ns = stripSelector(ref);
        String name = ns[0], selector = ns[1];
        return sh.conn(sh.owner(name), c -> {
            Param head = loadParam(c, name);
            if (head == null) {
                return null;
            }
            if (selector == null) {
                return new Object[] {head, null};
            }
            Hist h = null;
            boolean numeric = selector.chars().allMatch(Character::isDigit);
            try (PreparedStatement ps = c.prepareStatement("SELECT " + HIST_COLS + " FROM warp_awsparams_ssm_history WHERE name = ? AND "
                    + (numeric ? "version = ?" : "jsonb_exists(labels, ?)"))) {
                ps.setString(1, name);
                if (numeric) {
                    ps.setLong(2, Long.parseLong(selector));
                } else {
                    ps.setString(2, selector);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        h = readHist(rs);
                    }
                }
            }
            if (h == null) {
                throw new AwsException(400, "ParameterVersionNotFound", "Systems Manager can't find the specified parameter version.");
            }
            return new Object[] {head, h};
        });
    }

    private String valueOf(byte[] stored, String type, boolean decrypt) {
        if (type.equals("SecureString")) {
            if (!decrypt) {
                return Base64.getEncoder().encodeToString(stored);
            }
            return new String(kms.decryptFor(stored, Map.of()), StandardCharsets.UTF_8);
        }
        return new String(stored, StandardCharsets.UTF_8);
    }

    private JsonObject paramJson(Param p, Hist h, String selector, boolean decrypt) {
        byte[] value = h != null ? h.value : p.value;
        long version = h != null ? h.version : p.version;
        Timestamp modified = h != null ? h.modified : p.modified;
        JsonObject o = new JsonObject();
        o.addProperty("Name", p.name);
        o.addProperty("Type", p.type);
        o.addProperty("Value", valueOf(value, p.type, decrypt));
        o.addProperty("Version", version);
        if (selector != null) {
            o.addProperty("Selector", ":" + selector);
        }
        o.addProperty("LastModifiedDate", epoch(modified));
        o.addProperty("ARN", arn(p.name));
        o.addProperty("DataType", p.dataType);
        return o;
    }

    private JsonObject getParameter(JsonObject req) {
        String ref = Args.req(req, "Name");
        boolean decrypt = Boolean.TRUE.equals(Args.bool(req, "WithDecryption"));
        Object[] r = resolve(ref);
        if (r == null) {
            throw new AwsException(400, "ParameterNotFound", "Parameter " + stripSelector(ref)[0] + " not found.");
        }
        JsonObject out = new JsonObject();
        out.add("Parameter", paramJson((Param) r[0], (Hist) r[1], stripSelector(ref)[1], decrypt));
        return out;
    }

    private JsonObject getParameters(JsonObject req) {
        List<String> names = Args.strings(req, "Names");
        if (names.isEmpty() || names.size() > 10) {
            throw validation("1 validation error detected: Value '" + names + "' at 'names' failed to satisfy constraint: Member must have "
                    + "length less than or equal to 10");
        }
        boolean decrypt = Boolean.TRUE.equals(Args.bool(req, "WithDecryption"));
        JsonArray ok = new JsonArray();
        JsonArray invalid = new JsonArray();
        for (String n : names) {
            try {
                Object[] r = resolve(n);
                if (r == null) {
                    invalid.add(n);
                } else {
                    ok.add(paramJson((Param) r[0], (Hist) r[1], stripSelector(n)[1], decrypt));
                }
            } catch (AwsException e) {
                if (e.code.equals("ParameterVersionNotFound")) {
                    invalid.add(n);
                } else {
                    throw e;
                }
            }
        }
        JsonObject out = new JsonObject();
        out.add("Parameters", ok);
        out.add("InvalidParameters", invalid);
        return out;
    }

    private static String likeEscape(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    private JsonObject getByPath(JsonObject req) {
        String path = Args.req(req, "Path");
        if (!path.startsWith("/")) {
            throw validation("The parameter doesn't meet the parameter name requirements. The parameter name must begin with a forward slash \"/\".");
        }
        boolean recursive = Boolean.TRUE.equals(Args.bool(req, "Recursive"));
        boolean decrypt = Boolean.TRUE.equals(Args.bool(req, "WithDecryption"));
        Integer maxArg = Args.integer(req, "MaxResults");
        int max = maxArg == null ? 10 : Math.max(1, Math.min(maxArg, 10));
        String token = Args.str(req, "NextToken");
        String after = token == null ? null : new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        String prefix = path.endsWith("/") ? path : path + "/";
        List<JsonObject> filters = Args.objects(req, "ParameterFilters");
        List<Param> all = new ArrayList<>();
        for (List<Param> l : sh.<List<Param>>onAll(c -> {
            List<Param> r = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + PARAM_COLS + " FROM warp_awsparams_ssm_params WHERE name LIKE ? ESCAPE '\\' ORDER BY name")) {
                ps.setString(1, likeEscape(prefix) + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        Param p = readParam(rs);
                        if (recursive || p.name.indexOf('/', prefix.length()) < 0) {
                            r.add(p);
                        }
                    }
                }
            }
            return r;
        })) {
            all.addAll(l);
        }
        all.sort((a, b) -> a.name.compareTo(b.name));
        JsonArray out = new JsonArray();
        boolean more = false;
        String last = null;
        for (Param p : all) {
            if (after != null && p.name.compareTo(after) <= 0) {
                continue;
            }
            if (!filterMatch(p, filters)) {
                continue;
            }
            if (out.size() >= max) {
                more = true;
                break;
            }
            out.add(paramJson(p, null, null, decrypt));
            last = p.name;
        }
        JsonObject res = new JsonObject();
        res.add("Parameters", out);
        if (more) {
            res.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(last.getBytes(StandardCharsets.UTF_8)));
        }
        return res;
    }

    private boolean filterMatch(Param p, List<JsonObject> filters) {
        for (JsonObject f : filters) {
            String key = Args.str(f, "Key");
            String option = Args.str(f, "Option") == null ? "Equals" : Args.str(f, "Option");
            List<String> vals = Args.strings(f, "Values");
            String subject = switch (key == null ? "" : key) {
                case "Name" -> p.name.startsWith("/") ? p.name : "/" + p.name;
                case "Type" -> p.type;
                case "KeyId" -> p.keyId == null ? "" : p.keyId;
                case "Tier" -> p.tier;
                case "DataType" -> p.dataType;
                case "Path" -> p.name;
                case "Label" -> "";
                default -> throw validation("The specified key is not valid: " + key);
            };
            boolean ok = false;
            for (String v : vals) {
                switch (option) {
                    case "Equals" -> ok |= key.equals("Name") ? (subject.equals(v) || p.name.equals(v)) : subject.equals(v);
                    case "BeginsWith" -> ok |= subject.startsWith(v) || (key.equals("Name") && p.name.startsWith(v));
                    case "Contains" -> ok |= subject.contains(v);
                    case "Recursive" -> ok |= p.name.startsWith(v.endsWith("/") ? v : v + "/");
                    case "OneLevel" -> {
                        String pre = v.endsWith("/") ? v : v + "/";
                        ok |= p.name.startsWith(pre) && p.name.indexOf('/', pre.length()) < 0;
                    }
                    default -> ok |= subject.equals(v);
                }
            }
            if (key.equals("Path") && (option.equals("Equals") || option.equals("OneLevel"))) {
                ok = false;
                for (String v : vals) {
                    String pre = v.endsWith("/") ? v : v + "/";
                    ok |= p.name.startsWith(pre) && p.name.indexOf('/', pre.length()) < 0;
                }
            }
            if (!ok) {
                return false;
            }
        }
        return true;
    }

    private JsonObject deleteParameter(JsonObject req) {
        String name = Args.req(req, "Name");
        sh.tx(sh.owner(name), c -> {
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_ssm_params WHERE name = ?")) {
                ps.setString(1, name);
                if (ps.executeUpdate() == 0) {
                    throw new AwsException(400, "ParameterNotFound", "Parameter " + name + " not found.");
                }
            }
            try (PreparedStatement ps = c.prepareStatement("DELETE FROM warp_awsparams_ssm_history WHERE name = ?")) {
                ps.setString(1, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject deleteParameters(JsonObject req) {
        List<String> names = Args.strings(req, "Names");
        if (names.isEmpty() || names.size() > 10) {
            throw validation("1 validation error detected: Value at 'names' failed to satisfy constraint: Member must have length less than or equal to 10");
        }
        JsonArray deleted = new JsonArray();
        JsonArray invalid = new JsonArray();
        for (String n : names) {
            try {
                JsonObject one = new JsonObject();
                one.addProperty("Name", n);
                deleteParameter(one);
                deleted.add(n);
            } catch (AwsException e) {
                if (!e.code.equals("ParameterNotFound")) {
                    throw e;
                }
                invalid.add(n);
            }
        }
        JsonObject out = new JsonObject();
        out.add("DeletedParameters", deleted);
        out.add("InvalidParameters", invalid);
        return out;
    }

    // ---------------------------------------------------------------------------------------------- describe / history / labels

    private JsonArray policiesJson(String policies) {
        JsonArray out = new JsonArray();
        if (policies != null) {
            try {
                for (JsonElement e : JsonParser.parseString(policies).getAsJsonArray()) {
                    JsonObject p = new JsonObject();
                    p.addProperty("PolicyText", e.toString());
                    p.addProperty("PolicyType", e.isJsonObject() && e.getAsJsonObject().has("Type")
                            ? e.getAsJsonObject().get("Type").getAsString() : "Expiration");
                    p.addProperty("PolicyStatus", "Pending");
                    out.add(p);
                }
            } catch (RuntimeException ignored) {
                // malformed policy text is echoed nowhere
            }
        }
        return out;
    }

    private JsonObject describeParameters(JsonObject req) {
        List<JsonObject> filters = new ArrayList<>(Args.objects(req, "ParameterFilters"));
        for (JsonObject legacy : Args.objects(req, "Filters")) {
            JsonObject f = new JsonObject();
            f.addProperty("Key", Args.str(legacy, "Key"));
            f.add("Values", legacy.get("Values"));
            filters.add(f);
        }
        Integer maxArg = Args.integer(req, "MaxResults");
        int max = maxArg == null ? 10 : Math.max(1, Math.min(maxArg, 50));
        String token = Args.str(req, "NextToken");
        String after = token == null ? null : new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8);
        List<Param> all = new ArrayList<>();
        for (List<Param> l : sh.<List<Param>>onAll(c -> {
            List<Param> r = new ArrayList<>();
            try (var st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT " + PARAM_COLS + " FROM warp_awsparams_ssm_params ORDER BY name")) {
                while (rs.next()) {
                    r.add(readParam(rs));
                }
            }
            return r;
        })) {
            all.addAll(l);
        }
        all.sort((a, b) -> a.name.compareTo(b.name));
        JsonArray out = new JsonArray();
        boolean more = false;
        String last = null;
        for (Param p : all) {
            if (after != null && p.name.compareTo(after) <= 0) {
                continue;
            }
            if (!filterMatch(p, filters)) {
                continue;
            }
            if (out.size() >= max) {
                more = true;
                break;
            }
            JsonObject o = new JsonObject();
            o.addProperty("Name", p.name);
            o.addProperty("ARN", arn(p.name));
            o.addProperty("Type", p.type);
            if (p.keyId != null) {
                o.addProperty("KeyId", p.keyId);
            }
            o.addProperty("LastModifiedDate", epoch(p.modified));
            o.addProperty("LastModifiedUser", p.modifiedBy);
            if (p.description != null) {
                o.addProperty("Description", p.description);
            }
            if (p.allowedPattern != null) {
                o.addProperty("AllowedPattern", p.allowedPattern);
            }
            o.addProperty("Version", p.version);
            o.addProperty("Tier", p.tier);
            if (p.policies != null) {
                o.add("Policies", policiesJson(p.policies));
            }
            o.addProperty("DataType", p.dataType);
            out.add(o);
            last = p.name;
        }
        JsonObject res = new JsonObject();
        res.add("Parameters", out);
        if (more) {
            res.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(last.getBytes(StandardCharsets.UTF_8)));
        }
        return res;
    }

    private JsonObject history(JsonObject req) {
        String name = Args.req(req, "Name");
        boolean decrypt = Boolean.TRUE.equals(Args.bool(req, "WithDecryption"));
        Integer maxArg = Args.integer(req, "MaxResults");
        int max = maxArg == null ? 50 : Math.max(1, Math.min(maxArg, 50));
        String token = Args.str(req, "NextToken");
        long after = token == null ? 0 : Long.parseLong(new String(Base64.getUrlDecoder().decode(token), StandardCharsets.UTF_8));
        List<Hist> rows = sh.conn(sh.owner(name), c -> {
            if (loadParam(c, name) == null) {
                throw new AwsException(400, "ParameterNotFound", "Parameter " + name + " not found.");
            }
            List<Hist> r = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT " + HIST_COLS + " FROM warp_awsparams_ssm_history WHERE name = ? AND version > ? ORDER BY version LIMIT ?")) {
                ps.setString(1, name);
                ps.setLong(2, after);
                ps.setInt(3, max + 1);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        r.add(readHist(rs));
                    }
                }
            }
            return r;
        });
        JsonArray out = new JsonArray();
        boolean more = rows.size() > max;
        for (int i = 0; i < Math.min(max, rows.size()); i++) {
            Hist h = rows.get(i);
            JsonObject o = new JsonObject();
            o.addProperty("Name", h.name);
            o.addProperty("Type", h.type);
            if (h.keyId != null) {
                o.addProperty("KeyId", h.keyId);
            }
            o.addProperty("LastModifiedDate", epoch(h.modified));
            o.addProperty("LastModifiedUser", h.modifiedBy);
            if (h.description != null) {
                o.addProperty("Description", h.description);
            }
            o.addProperty("Value", valueOf(h.value, h.type, decrypt));
            if (h.allowedPattern != null) {
                o.addProperty("AllowedPattern", h.allowedPattern);
            }
            o.addProperty("Version", h.version);
            JsonArray labels = new JsonArray();
            h.labels.forEach(labels::add);
            o.add("Labels", labels);
            o.addProperty("Tier", h.tier);
            if (h.policies != null) {
                o.add("Policies", policiesJson(h.policies));
            }
            o.addProperty("DataType", h.dataType);
            out.add(o);
        }
        JsonObject res = new JsonObject();
        res.add("Parameters", out);
        if (more) {
            res.addProperty("NextToken", Base64.getUrlEncoder().withoutPadding().encodeToString(
                    String.valueOf(rows.get(max - 1).version).getBytes(StandardCharsets.UTF_8)));
        }
        return res;
    }

    private JsonObject label(JsonObject req) {
        String name = Args.req(req, "Name");
        List<String> labels = Args.strings(req, "Labels");
        Long versionArg = Args.lng(req, "ParameterVersion");
        if (labels.isEmpty()) {
            throw validation("1 validation error detected: Value '[]' at 'labels' failed to satisfy constraint: Member must have length greater than or equal to 1");
        }
        JsonArray invalid = new JsonArray();
        List<String> good = new ArrayList<>();
        for (String l : labels) {
            String low = l.toLowerCase();
            if (!LABEL.matcher(l).matches() || low.startsWith("aws") || low.startsWith("ssm") || Character.isDigit(l.charAt(0))) {
                invalid.add(l);
            } else {
                good.add(l);
            }
        }
        if (invalid.size() > 0) {
            throw new AwsException(400, "ValidationException",
                    "The parameter version label " + invalid.get(0).getAsString() + " is not valid.");
        }
        return sh.tx(sh.owner(name), c -> {
            Param head = loadParamLocked(c, name);
            if (head == null) {
                throw new AwsException(400, "ParameterNotFound", "Parameter " + name + " not found.");
            }
            long v = versionArg == null ? head.version : versionArg;
            Hist target = null;
            try (PreparedStatement ps = c.prepareStatement("SELECT " + HIST_COLS + " FROM warp_awsparams_ssm_history WHERE name = ? AND version = ? FOR UPDATE")) {
                ps.setString(1, name);
                ps.setLong(2, v);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        target = readHist(rs);
                    }
                }
            }
            if (target == null) {
                throw new AwsException(400, "ParameterVersionNotFound", "Systems Manager could not find version " + v + " of " + name
                        + ". Verify the version and try again.");
            }
            List<String> merged = new ArrayList<>(target.labels);
            for (String l : good) {
                if (!merged.contains(l)) {
                    merged.add(l);
                }
            }
            if (merged.size() > 10) {
                throw new AwsException(400, "ParameterVersionLabelLimitExceeded", "A parameter version can have maximum 10 labels.");
            }
            // a label lives on one version: take it off whichever version has it now
            for (String l : good) {
                try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_ssm_history SET labels = labels - ?::text WHERE name = ? AND version <> ? AND jsonb_exists(labels, ?)")) {
                    ps.setString(1, l);
                    ps.setString(2, name);
                    ps.setLong(3, v);
                    ps.setString(4, l);
                    ps.executeUpdate();
                }
            }
            JsonArray arr = new JsonArray();
            merged.forEach(arr::add);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_ssm_history SET labels = ?::jsonb WHERE name = ? AND version = ?")) {
                ps.setString(1, arr.toString());
                ps.setString(2, name);
                ps.setLong(3, v);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.add("InvalidLabels", new JsonArray());
            out.addProperty("ParameterVersion", v);
            return out;
        });
    }

    private JsonObject unlabel(JsonObject req) {
        String name = Args.req(req, "Name");
        List<String> labels = Args.strings(req, "Labels");
        long v = Args.lng(req, "ParameterVersion") == null ? -1 : Args.lng(req, "ParameterVersion");
        return sh.tx(sh.owner(name), c -> {
            if (loadParamLocked(c, name) == null) {
                throw new AwsException(400, "ParameterNotFound", "Parameter " + name + " not found.");
            }
            Hist target = null;
            try (PreparedStatement ps = c.prepareStatement("SELECT " + HIST_COLS + " FROM warp_awsparams_ssm_history WHERE name = ? AND version = ? FOR UPDATE")) {
                ps.setString(1, name);
                ps.setLong(2, v);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        target = readHist(rs);
                    }
                }
            }
            if (target == null) {
                throw new AwsException(400, "ParameterVersionNotFound", "Systems Manager could not find version " + v + " of " + name);
            }
            JsonArray removed = new JsonArray();
            JsonArray invalid = new JsonArray();
            List<String> keep = new ArrayList<>(target.labels);
            for (String l : labels) {
                if (keep.remove(l)) {
                    removed.add(l);
                } else {
                    invalid.add(l);
                }
            }
            JsonArray arr = new JsonArray();
            keep.forEach(arr::add);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_ssm_history SET labels = ?::jsonb WHERE name = ? AND version = ?")) {
                ps.setString(1, arr.toString());
                ps.setString(2, name);
                ps.setLong(3, v);
                ps.executeUpdate();
            }
            JsonObject out = new JsonObject();
            out.add("RemovedLabels", removed);
            out.add("InvalidLabels", invalid);
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- tags

    private String tagTarget(JsonObject req) {
        String type = Args.req(req, "ResourceType");
        if (!type.equals("Parameter")) {
            throw new AwsException(400, "InvalidResourceType", "Only ResourceType Parameter is supported by this emulation");
        }
        String id = Args.req(req, "ResourceId");
        return id.startsWith("arn:") ? stripSelector(id)[0] : id;
    }

    private JsonObject tags(JsonObject req, boolean add) {
        String name = tagTarget(req);
        Map<String, String> tags = new LinkedHashMap<>();
        for (JsonObject t : Args.objects(req, "Tags")) {
            tags.put(Args.str(t, "Key"), Args.str(t, "Value") == null ? "" : Args.str(t, "Value"));
        }
        List<String> remove = Args.strings(req, "TagKeys");
        sh.tx(sh.owner(name), c -> {
            Param p = loadParamLocked(c, name);
            if (p == null) {
                throw new AwsException(400, "InvalidResourceId", "Invalid Resource Id");
            }
            Map<String, String> cur = new TreeMap<>(p.tags);
            if (add) {
                cur.putAll(tags);
            } else {
                remove.forEach(cur::remove);
            }
            if (cur.size() > 50) {
                throw new AwsException(400, "TooManyUpdates", "Tag limit exceeded");
            }
            JsonObject t = new JsonObject();
            cur.forEach(t::addProperty);
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_ssm_params SET tags = ?::jsonb WHERE name = ?")) {
                ps.setString(1, t.toString());
                ps.setString(2, name);
                ps.executeUpdate();
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject listTags(JsonObject req) {
        String name = tagTarget(req);
        return sh.conn(sh.owner(name), c -> {
            Param p = loadParam(c, name);
            if (p == null) {
                throw new AwsException(400, "InvalidResourceId", "Invalid Resource Id");
            }
            JsonArray a = new JsonArray();
            p.tags.forEach((k, v) -> {
                JsonObject t = new JsonObject();
                t.addProperty("Key", k);
                t.addProperty("Value", v);
                a.add(t);
            });
            JsonObject out = new JsonObject();
            out.add("TagList", a);
            return out;
        });
    }

    // ---------------------------------------------------------------------------------------------- run command (recorded only)

    private JsonObject commandJson(String id, ResultSet rs) throws SQLException {
        JsonObject c = new JsonObject();
        c.addProperty("CommandId", id);
        c.addProperty("DocumentName", rs.getString("document_name"));
        if (rs.getString("comment") != null) {
            c.addProperty("Comment", rs.getString("comment"));
        }
        c.addProperty("RequestedDateTime", epoch(rs.getTimestamp("requested_at")));
        c.addProperty("Status", rs.getString("command_status"));
        c.addProperty("StatusDetails", rs.getString("command_status"));
        c.addProperty("TimeoutSeconds", rs.getInt("timeout_seconds"));
        c.add("Parameters", JsonParser.parseString(rs.getString("parameters")));
        return c;
    }

    private JsonObject sendCommand(JsonObject req) {
        String doc = Args.req(req, "DocumentName");
        List<String> instances = Args.strings(req, "InstanceIds");
        if (instances.isEmpty()) {
            throw new AwsException(400, "InvalidInstanceId", "Instance IDs are required in this emulation (targets are not supported)");
        }
        Integer timeout = Args.integer(req, "TimeoutSeconds");
        if (timeout != null && timeout < 30) {
            throw validation("1 validation error detected: Value '" + timeout + "' at 'timeoutSeconds' failed to satisfy constraint: Member "
                    + "must have value greater than or equal to 30");
        }
        JsonObject params = Args.obj(req, "Parameters") == null ? new JsonObject() : Args.obj(req, "Parameters");
        String id = UUID.randomUUID().toString();
        String comment = Args.str(req, "Comment");
        return sh.tx(sh.home(), c -> {
            for (String inst : instances) {
                try (PreparedStatement ps = c.prepareStatement("INSERT INTO warp_awsparams_ssm_commands (command_id, instance_id, document_name, comment, "
                        + "parameters, timeout_seconds) VALUES (?, ?, ?, ?, ?::jsonb, ?)")) {
                    ps.setString(1, id);
                    ps.setString(2, inst);
                    ps.setString(3, doc);
                    ps.setString(4, comment);
                    ps.setString(5, params.toString());
                    ps.setInt(6, timeout == null ? 3600 : timeout);
                    ps.executeUpdate();
                }
            }
            JsonObject cmd;
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_awsparams_ssm_commands WHERE command_id = ? LIMIT 1")) {
                ps.setString(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    cmd = commandJson(id, rs);
                }
            }
            JsonArray ia = new JsonArray();
            instances.forEach(ia::add);
            cmd.add("InstanceIds", ia);
            cmd.addProperty("TargetCount", instances.size());
            JsonObject out = new JsonObject();
            out.add("Command", cmd);
            return out;
        });
    }

    private JsonObject getInvocation(JsonObject req) {
        String id = Args.req(req, "CommandId");
        String inst = Args.req(req, "InstanceId");
        return sh.conn(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_awsparams_ssm_commands WHERE command_id = ? AND instance_id = ?")) {
                ps.setString(1, id);
                ps.setString(2, inst);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        throw new AwsException(400, "InvocationDoesNotExist", "The command ID and instance ID you specified did not match any invocations.");
                    }
                    JsonObject o = new JsonObject();
                    o.addProperty("CommandId", id);
                    o.addProperty("InstanceId", inst);
                    o.addProperty("DocumentName", rs.getString("document_name"));
                    o.addProperty("Status", rs.getString("status"));
                    o.addProperty("StatusDetails", rs.getString("status"));
                    o.addProperty("StandardOutputContent", "");
                    o.addProperty("StandardErrorContent", "");
                    o.addProperty("ResponseCode", -1);
                    return o;
                }
            }
        });
    }

    private JsonObject listCommands(JsonObject req) {
        String id = Args.str(req, "CommandId");
        return sh.conn(sh.home(), c -> {
            JsonArray a = new JsonArray();
            try (PreparedStatement ps = c.prepareStatement("SELECT command_id, array_agg(instance_id ORDER BY instance_id) AS insts, min(document_name) AS document_name, "
                    + "min(comment) AS comment, min(parameters::text) AS parameters, min(command_status) AS command_status, min(timeout_seconds) AS timeout_seconds, "
                    + "min(requested_at) AS requested_at FROM warp_awsparams_ssm_commands WHERE (?::text IS NULL OR command_id = ?) GROUP BY command_id ORDER BY min(requested_at) DESC")) {
                ps.setString(1, id);
                ps.setString(2, id);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject cmd = commandJson(rs.getString(1), rs);
                        JsonArray ia = new JsonArray();
                        for (Object o : (Object[]) rs.getArray("insts").getArray()) {
                            ia.add(String.valueOf(o));
                        }
                        cmd.add("InstanceIds", ia);
                        cmd.addProperty("TargetCount", ia.size());
                        a.add(cmd);
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("Commands", a);
            return out;
        });
    }

    private JsonObject listInvocations(JsonObject req) {
        String id = Args.str(req, "CommandId");
        String inst = Args.str(req, "InstanceId");
        return sh.conn(sh.home(), c -> {
            JsonArray a = new JsonArray();
            try (PreparedStatement ps = c.prepareStatement("SELECT * FROM warp_awsparams_ssm_commands WHERE (?::text IS NULL OR command_id = ?) "
                    + "AND (?::text IS NULL OR instance_id = ?) ORDER BY requested_at DESC")) {
                ps.setString(1, id);
                ps.setString(2, id);
                ps.setString(3, inst);
                ps.setString(4, inst);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        JsonObject o = new JsonObject();
                        o.addProperty("CommandId", rs.getString("command_id"));
                        o.addProperty("InstanceId", rs.getString("instance_id"));
                        o.addProperty("DocumentName", rs.getString("document_name"));
                        o.addProperty("RequestedDateTime", epoch(rs.getTimestamp("requested_at")));
                        o.addProperty("Status", rs.getString("status"));
                        o.addProperty("StatusDetails", rs.getString("status"));
                        a.add(o);
                    }
                }
            }
            JsonObject out = new JsonObject();
            out.add("CommandInvocations", a);
            return out;
        });
    }

    private JsonObject cancelCommand(JsonObject req) {
        String id = Args.req(req, "CommandId");
        List<String> insts = Args.strings(req, "InstanceIds");
        sh.tx(sh.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("UPDATE warp_awsparams_ssm_commands SET status = 'Cancelled', command_status = 'Cancelled' WHERE "
                    + "command_id = ? AND (cardinality(?::text[]) = 0 OR instance_id = ANY (?::text[]))")) {
                ps.setString(1, id);
                java.sql.Array arr = c.createArrayOf("text", insts.toArray());
                ps.setArray(2, arr);
                ps.setArray(3, arr);
                if (ps.executeUpdate() == 0) {
                    throw new AwsException(400, "InvalidCommandId", "The command id " + id + " does not exist");
                }
            }
            return null;
        });
        return new JsonObject();
    }

    private JsonObject serviceSetting(JsonObject req) {
        JsonObject s = new JsonObject();
        s.addProperty("SettingId", Args.str(req, "SettingId"));
        s.addProperty("SettingValue", "default");
        s.addProperty("DefaultValue", "default");
        s.addProperty("Status", "Default");
        JsonObject out = new JsonObject();
        out.add("ServiceSetting", s);
        return out;
    }
}

package com.sayonora.wire.azurewire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.azurewire.ODataFilter.Val;
import com.sayonora.wire.azurewire.TableStore.Ent;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** The Azure Table service REST API (OData JSON) on top of {@link TableStore}. */
final class TableService {

    private static final Pattern ENTITY_PATH = Pattern.compile("^([A-Za-z][A-Za-z0-9]{2,62})\\(PartitionKey='((?:[^']|'')*)',\\s*RowKey='((?:[^']|'')*)'\\)$");
    private static final Pattern QUERY_PATH = Pattern.compile("^([A-Za-z][A-Za-z0-9]{2,62})\\(\\)$");
    private static final Pattern TABLE_PATH = Pattern.compile("^Tables\\('([^']*)'\\)$");
    private static final Pattern TABLE_NAME = Pattern.compile("^[A-Za-z][A-Za-z0-9]{2,62}$");
    private static final Pattern BAD_KEY = Pattern.compile("[\\\\/#?\\u0000-\\u001f\\u007f-\\u009f]");
    private static final int MAX_PAGE = 1000;

    final TableStore store;
    final AzureConfig cfg;
    final AzureAuth authn;

    TableService(TableStore store, AzureConfig cfg, AzureAuth authn) {
        this.store = store;
        this.cfg = cfg;
        this.authn = authn;
    }

    // ---- accept / metadata level

    static String level(AzReq r) {
        String a = r.header("Accept");
        if (a == null || a.isBlank() || a.contains("*/*") || a.equals("application/json")) {
            return "minimalmetadata";
        }
        if (a.contains("atom+xml") || a.contains("application/xml")) {
            throw new AzureException(415, "UnsupportedMediaType", "The content media type is not supported "
                    + "(Warp azurewire speaks OData JSON only, not Atom).");
        }
        String l = a.toLowerCase(Locale.ROOT);
        if (l.contains("odata=fullmetadata")) {
            return "fullmetadata";
        }
        if (l.contains("odata=nometadata")) {
            return "nometadata";
        }
        return "minimalmetadata";
    }

    static String contentType(String level) {
        return "application/json;odata=" + level + ";streaming=true;charset=utf-8";
    }

    private String base(AzReq r) {
        return r.raw.getScheme() + "://" + r.header("Host") + (r.hostStyle ? "/" : "/" + r.account + "/");
    }

    private void json(AzReq r, HttpServletResponse resp, int status, String level, JsonObject body) throws IOException {
        byte[] b = body.toString().getBytes(StandardCharsets.UTF_8);
        resp.setStatus(status);
        resp.setContentType(contentType(level));
        resp.setContentLength(b.length);
        resp.getOutputStream().write(b);
    }

    // ---- dispatch

    void handle(AzReq r, HttpServletResponse resp) throws IOException {
        String seg = r.first();
        String table = seg == null ? null : tableOf(seg);
        if ("OPTIONS".equals(r.method)) {
            AzCors.preflight(r, resp, corsRules(r.account));
            return;
        }
        AzureAuth.Result auth = authn.authenticate(r, id -> {
            if (table == null) {
                return null;
            }
            List<AzureAuth.Policy> acl = store.acl(r.account, table);
            if (acl != null) {
                for (AzureAuth.Policy p : acl) {
                    if (p.id().equals(id)) {
                        return p;
                    }
                }
            }
            return null;
        }, table, null);
        AzCors.applyActual(r, resp, corsRules(r.account));
        if (auth.anonymous()) {
            throw BlobService.noAuthKeyed();
        }
        resp.setHeader("DataServiceVersion", "3.0;");
        resp.setHeader("X-Content-Type-Options", "nosniff");
        resp.setHeader("Cache-Control", "no-cache");
        if (seg == null) {
            serviceLevel(r, resp, auth);
            return;
        }
        String comp = r.q("comp");
        if (seg.equals("$batch")) {
            BlobService.requireMethod(r, "POST");
            r.op = "EntityGroupTransaction";
            r.write = true;
            batch(r, resp, auth);
            return;
        }
        if (seg.equals("Tables")) {
            tables(r, resp, auth);
            return;
        }
        Matcher tm = TABLE_PATH.matcher(seg);
        if (tm.matches()) {
            BlobService.requireMethod(r, "DELETE", "GET");
            if ("DELETE".equals(r.method)) {
                r.op = "DeleteTable";
                r.write = true;
                auth.authorize('c', "d");
                if (!store.deleteTable(r.account, tm.group(1))) {
                    throw new AzureException(404, "ResourceNotFound", "The specified resource does not exist.");
                }
                resp.setStatus(204);
            } else {
                r.op = "GetTable";
                auth.authorize('c', "r");
                if (!store.tableExists(r.account, tm.group(1))) {
                    throw tableNotFound();
                }
                String lv = level(r);
                JsonObject o = new JsonObject();
                if (!lv.equals("nometadata")) {
                    o.addProperty("odata.metadata", base(r) + "$metadata#Tables/@Element");
                }
                o.addProperty("TableName", tm.group(1));
                json(r, resp, 200, lv, o);
            }
            return;
        }
        if ("acl".equals(comp)) {
            tableAcl(r, resp, auth, seg);
            return;
        }
        Matcher em = ENTITY_PATH.matcher(seg);
        if (em.matches()) {
            entity(r, resp, auth, em.group(1), unq(em.group(2)), unq(em.group(3)));
            return;
        }
        Matcher qm = QUERY_PATH.matcher(seg);
        if (qm.matches()) {
            BlobService.requireMethod(r, "GET");
            query(r, resp, auth, qm.group(1));
            return;
        }
        if (TABLE_NAME.matcher(seg).matches() && "POST".equals(r.method)) {
            insert(r, resp, auth, seg);
            return;
        }
        if (TABLE_NAME.matcher(seg).matches() && "GET".equals(r.method)) {
            query(r, resp, auth, seg);
            return;
        }
        throw AzErrors.invalidUri();
    }

    private static String tableOf(String seg) {
        int p = seg.indexOf('(');
        return p < 0 ? (seg.equals("Tables") || seg.equals("$batch") ? null : seg) : (seg.startsWith("Tables(") ? null : seg.substring(0, p));
    }

    private static String unq(String s) {
        return s.replace("''", "'");
    }

    private List<AzCors.Rule> corsRules(String account) {
        try {
            return AzCors.parse(AzServiceProps.cors(store.serviceProperties(account)));
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    static AzureException tableNotFound() {
        return new AzureException(404, "TableNotFound", "The table specified does not exist.");
    }

    private void serviceLevel(AzReq r, HttpServletResponse resp, AzureAuth.Result auth) throws IOException {
        String comp = r.q("comp");
        String restype = r.q("restype");
        if ("service".equals(restype) && "properties".equals(comp)) {
            if ("GET".equals(r.method)) {
                r.op = "GetServiceProperties";
                auth.authorize('s', "r");
                AzHttp.xml(resp, 200, AzServiceProps.render(store.serviceProperties(r.account), false));
                return;
            }
            BlobService.requireMethod(r, "PUT");
            r.op = "SetServiceProperties";
            r.write = true;
            auth.authorize('s', "w");
            store.setServiceProperties(r.account,
                    AzServiceProps.merge(store.serviceProperties(r.account), r.raw.getInputStream().readAllBytes(), false));
            resp.setStatus(202);
            return;
        }
        if ("service".equals(restype) && "stats".equals(comp)) {
            r.op = "GetServiceStats";
            auth.authorize('s', "r");
            AzHttp.xml(resp, 200, new AzXml().open("StorageServiceStats").open("GeoReplication").text("Status", "live")
                    .text("LastSyncTime", AzHttp.httpDate(Instant.now())).close().close().toString());
            return;
        }
        throw AzErrors.invalidUri();
    }

    private void tableAcl(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String table) throws IOException {
        if (auth.kind != AzureAuth.Kind.KEY && auth.kind != AzureAuth.Kind.BEARER) {
            throw new AzureException(403, "AuthorizationFailure", "This request is not authorized to perform this operation.");
        }
        List<AzureAuth.Policy> acl = store.acl(r.account, table);
        if (acl == null) {
            throw tableNotFound();
        }
        if ("PUT".equals(r.method)) {
            r.op = "SetTableAcl";
            r.write = true;
            store.setAcl(r.account, table, AzServiceProps.parseSignedIdentifiers(r.raw.getInputStream().readAllBytes()));
            resp.setStatus(204);
            return;
        }
        BlobService.requireMethod(r, "GET");
        r.op = "GetTableAcl";
        AzXml x = new AzXml().open("SignedIdentifiers");
        for (AzureAuth.Policy p : acl) {
            x.open("SignedIdentifier").text("Id", p.id()).open("AccessPolicy");
            if (p.start() != null) {
                x.text("Start", p.start());
            }
            if (p.expiry() != null) {
                x.text("Expiry", p.expiry());
            }
            if (p.permission() != null) {
                x.text("Permission", p.permission());
            }
            x.close().close();
        }
        AzHttp.xml(resp, 200, x.close().toString());
    }

    // ---- tables

    private void tables(AzReq r, HttpServletResponse resp, AzureAuth.Result auth) throws IOException {
        String lv = level(r);
        if ("POST".equals(r.method)) {
            r.op = "CreateTable";
            r.write = true;
            auth.authorize('c', "a");
            JsonObject body = parseBody(r);
            String name = body.has("TableName") && body.get("TableName").isJsonPrimitive() ? body.get("TableName").getAsString() : null;
            if (name == null || name.isEmpty()) {
                throw new AzureException(400, "TableNameEmpty", "The specified table name is empty.");
            }
            if (name.length() < 3 || name.length() > 63) {
                throw new AzureException(400, "InvalidResourceName", "The specified resource name length is not within the permissible limits.");
            }
            if (!TABLE_NAME.matcher(name).matches() || name.equalsIgnoreCase("tables")) {
                throw new AzureException(400, "InvalidResourceName", "The specified resource name contains invalid characters.");
            }
            if (!store.createTable(r.account, name)) {
                throw new AzureException(409, "TableAlreadyExists", "The table specified already exists.");
            }
            boolean noContent = "return-no-content".equalsIgnoreCase(r.header("Prefer"));
            if (noContent) {
                resp.setHeader("Preference-Applied", "return-no-content");
                resp.setStatus(204);
                return;
            }
            resp.setHeader("Preference-Applied", "return-content");
            JsonObject o = new JsonObject();
            if (!lv.equals("nometadata")) {
                o.addProperty("odata.metadata", base(r) + "$metadata#Tables/@Element");
            }
            if (lv.equals("fullmetadata")) {
                o.addProperty("odata.type", r.account + ".Tables");
                o.addProperty("odata.id", base(r) + "Tables('" + name + "')");
                o.addProperty("odata.editLink", "Tables('" + name + "')");
            }
            o.addProperty("TableName", name);
            json(r, resp, 201, lv, o);
            return;
        }
        BlobService.requireMethod(r, "GET");
        r.op = "QueryTables";
        auth.authorize('c', "r");
        ODataFilter f;
        try {
            f = ODataFilter.parse(r.q("$filter"));
        } catch (ODataFilter.ParseError e) {
            throw new AzureException(400, "InvalidInput", "The query condition specified in the request is invalid.");
        }
        int top = topParam(r);
        String after = unb64(r.q("NextTableName"));
        List<String> names = new ArrayList<>();
        int limit = Math.min(top, MAX_PAGE);
        String cursor = after;
        String next = null;
        outer:
        while (true) {
            List<String> batch = store.listTables(r.account, cursor, 500);
            for (String n : batch) {
                if (f.matches(nm -> nm.equals("TableName") ? new Val("Edm.String", n) : null)) {
                    if (names.size() == limit) {
                        next = n;
                        break outer;
                    }
                    names.add(n);
                }
            }
            if (batch.size() < 500) {
                break;
            }
            cursor = batch.get(batch.size() - 1) + "\u0000";
            after = null;
        }
        if (next != null) {
            resp.setHeader("x-ms-continuation-NextTableName", b64(next));
        }
        JsonObject o = new JsonObject();
        if (!lv.equals("nometadata")) {
            o.addProperty("odata.metadata", base(r) + "$metadata#Tables");
        }
        JsonArray arr = new JsonArray();
        for (String n : names) {
            JsonObject t = new JsonObject();
            if (lv.equals("fullmetadata")) {
                t.addProperty("odata.type", r.account + ".Tables");
                t.addProperty("odata.id", base(r) + "Tables('" + n + "')");
                t.addProperty("odata.editLink", "Tables('" + n + "')");
            }
            t.addProperty("TableName", n);
            arr.add(t);
        }
        o.add("value", arr);
        json(r, resp, 200, lv, o);
    }

    static String b64(String s) {
        return Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    static String unb64(String s) {
        if (s == null) {
            return null;
        }
        try {
            return new String(Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new AzureException(400, "InvalidInput", "The continuation token is not valid.");
        }
    }

    private static int topParam(AzReq r) {
        String v = r.q("$top");
        if (v == null) {
            return MAX_PAGE;
        }
        try {
            int n = Integer.parseInt(v);
            if (n <= 0) {
                throw new AzureException(400, "InvalidInput", "The value of $top must be positive.");
            }
            return n;
        } catch (NumberFormatException e) {
            throw new AzureException(400, "InvalidInput", "The value of $top is not valid.");
        }
    }

    private static JsonObject parseBody(AzReq r) throws IOException {
        byte[] raw = r.raw.getInputStream().readAllBytes();
        return parseJson(raw);
    }

    static JsonObject parseJson(byte[] raw) {
        try {
            return JsonParser.parseString(new String(raw, StandardCharsets.UTF_8)).getAsJsonObject();
        } catch (RuntimeException e) {
            throw new AzureException(400, "InvalidInput", "The request body is not valid JSON.");
        }
    }

    // ---- entities

    static Ent entityFromJson(JsonObject o) {
        Ent e = new Ent();
        Map<String, String> types = new LinkedHashMap<>();
        for (Map.Entry<String, JsonElement> en : o.entrySet()) {
            if (en.getKey().endsWith("@odata.type")) {
                types.put(en.getKey().substring(0, en.getKey().length() - 11), en.getValue().getAsString());
            }
        }
        int count = 0;
        for (Map.Entry<String, JsonElement> en : o.entrySet()) {
            String k = en.getKey();
            if (k.startsWith("odata.") || k.endsWith("@odata.type")) {
                continue;
            }
            JsonElement v = en.getValue();
            if (k.equals("PartitionKey") || k.equals("RowKey")) {
                if (!v.isJsonPrimitive() || !v.getAsJsonPrimitive().isString()) {
                    throw new AzureException(400, "InvalidInput", "The '" + k + "' property must be a string.");
                }
                String s = v.getAsString();
                if (s.length() > 1024 || BAD_KEY.matcher(s).find()) {
                    throw new AzureException(400, "InvalidInput", "The '" + k + "' property contains invalid characters or is too long.");
                }
                if (k.equals("PartitionKey")) {
                    e.pk = s;
                } else {
                    e.rk = s;
                }
                continue;
            }
            if (k.equals("Timestamp")) {
                continue;
            }
            if (++count > 252) {
                throw new AzureException(400, "TooManyProperties", "The entity contains more properties than allowed.");
            }
            if (v.isJsonNull()) {
                continue;
            }
            e.props.put(k, typed(k, v, types.get(k)));
        }
        if (e.pk == null || e.rk == null) {
            throw new AzureException(400, "PropertiesNeedValue", "The values are not specified for all properties in the entity.");
        }
        if (o.toString().length() > 1024 * 1024) {
            throw new AzureException(400, "EntityTooLarge", "The entity is larger than the maximum allowed size (1MB).");
        }
        return e;
    }

    private static Val typed(String name, JsonElement v, String type) {
        if (!v.isJsonPrimitive()) {
            throw new AzureException(400, "InvalidInput", "The property '" + name + "' has an unsupported value.");
        }
        try {
            if (v.getAsJsonPrimitive().isBoolean()) {
                return new Val("Edm.Boolean", v.getAsBoolean());
            }
            if (v.getAsJsonPrimitive().isNumber()) {
                String raw = v.getAsString();
                if ("Edm.Double".equals(type) || raw.contains(".") || raw.contains("e") || raw.contains("E")) {
                    return new Val("Edm.Double", v.getAsDouble());
                }
                long l = Long.parseLong(raw);
                if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
                    return new Val("Edm.Int32", (int) l);
                }
                return new Val("Edm.Double", (double) l);
            }
            String s = v.getAsString();
            if (type == null || type.equals("Edm.String")) {
                if (s.length() > 32768) {
                    throw new AzureException(400, "PropertyValueTooLarge", "The property value exceeds the maximum allowed size (64KB).");
                }
                return new Val("Edm.String", s);
            }
            return switch (type) {
                case "Edm.Int64" -> new Val("Edm.Int64", Long.parseLong(s));
                case "Edm.Int32" -> new Val("Edm.Int32", Integer.parseInt(s));
                case "Edm.Double" -> new Val("Edm.Double", s.equals("NaN") ? Double.NaN : s.equals("Infinity") ? Double.POSITIVE_INFINITY
                        : s.equals("-Infinity") ? Double.NEGATIVE_INFINITY : Double.parseDouble(s));
                case "Edm.Boolean" -> new Val("Edm.Boolean", Boolean.parseBoolean(s));
                case "Edm.DateTime" -> new Val("Edm.DateTime", ODataFilter.normalizeDate(s));
                case "Edm.Guid" -> new Val("Edm.Guid", UUID.fromString(s).toString());
                case "Edm.Binary" -> new Val("Edm.Binary", Base64.getDecoder().decode(s));
                default -> throw new AzureException(400, "InvalidInput", "The property '" + name + "' has an unsupported type " + type);
            };
        } catch (IllegalArgumentException | ODataFilter.ParseError ex) {
            throw new AzureException(400, "InvalidInput", "The value of property '" + name + "' is not a valid " + type + ".");
        }
    }

    JsonObject render(AzReq r, String table, Ent e, String level, boolean withMeta) {
        JsonObject o = new JsonObject();
        boolean minimal = level.equals("minimalmetadata");
        boolean full = level.equals("fullmetadata");
        if (withMeta && !level.equals("nometadata")) {
            o.addProperty("odata.metadata", base(r) + "$metadata#" + table + "/@Element");
        }
        if (!level.equals("nometadata")) {
            if (full) {
                o.addProperty("odata.type", r.account + "." + table);
                o.addProperty("odata.id", base(r) + table + "(PartitionKey='" + e.pk.replace("'", "''") + "',RowKey='"
                        + e.rk.replace("'", "''") + "')");
            }
            o.addProperty("odata.etag", e.etag());
            if (full) {
                o.addProperty("odata.editLink", table + "(PartitionKey='" + e.pk.replace("'", "''") + "',RowKey='"
                        + e.rk.replace("'", "''") + "')");
            }
        }
        o.addProperty("PartitionKey", e.pk);
        o.addProperty("RowKey", e.rk);
        if (full) {
            o.addProperty("Timestamp@odata.type", "Edm.DateTime");
        }
        o.addProperty("Timestamp", e.ts);
        for (Map.Entry<String, Val> p : e.props.entrySet()) {
            Val v = p.getValue();
            boolean annotate = !level.equals("nometadata") && !(v.type().equals("Edm.String") || v.type().equals("Edm.Boolean")
                    || v.type().equals("Edm.Int32") || v.type().equals("Edm.Double") && !(v.v() instanceof Double d
                    && (d.isNaN() || d.isInfinite())));
            if (annotate) {
                o.addProperty(p.getKey() + "@odata.type", v.type());
            }
            switch (v.type()) {
                case "Edm.Int32" -> o.addProperty(p.getKey(), (Integer) v.v());
                case "Edm.Boolean" -> o.addProperty(p.getKey(), (Boolean) v.v());
                case "Edm.Double" -> {
                    double d = (Double) v.v();
                    if (Double.isNaN(d) || Double.isInfinite(d)) {
                        o.addProperty(p.getKey(), d != d ? "NaN" : d > 0 ? "Infinity" : "-Infinity");
                    } else {
                        o.addProperty(p.getKey(), d);
                    }
                }
                case "Edm.Int64" -> o.addProperty(p.getKey(), String.valueOf(v.v()));
                case "Edm.Binary" -> o.addProperty(p.getKey(), Base64.getEncoder().encodeToString((byte[]) v.v()));
                default -> o.addProperty(p.getKey(), String.valueOf(v.v()));
            }
        }
        return o;
    }

    private boolean minimalSingle = false;

    private JsonObject select(JsonObject full, String sel) {
        if (sel == null || sel.isBlank() || sel.trim().equals("*")) {
            return full;
        }
        java.util.Set<String> want = new java.util.HashSet<>();
        for (String s : sel.split(",")) {
            want.add(s.trim());
        }
        JsonObject o = new JsonObject();
        for (Map.Entry<String, JsonElement> en : full.entrySet()) {
            String k = en.getKey();
            String base = k.endsWith("@odata.type") ? k.substring(0, k.length() - 11) : k;
            if (k.startsWith("odata.") || want.contains(base)) {
                o.add(k, en.getValue());
            }
        }
        return o;
    }

    private void requireTable(AzReq r, String table) {
        if (!store.tableExists(r.account, table)) {
            throw tableNotFound();
        }
    }

    private void insert(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String table) throws IOException {
        r.op = "InsertEntity";
        r.write = true;
        auth.authorize('o', "a");
        String lv = level(r);
        Ent e = entityFromJson(parseBody(r));
        requireTable(r, table);
        Res res = store.shards.tx(store.ownerHost(r.account, table, e.pk), c -> apply(c, r.account, table, "POST", e.pk, e.rk, e,
                null, r.header("Prefer"), false));
        writeRes(r, resp, table, lv, res);
    }

    record Res(int status, Ent entity, boolean returnContent) {
    }

    /** One entity operation inside a transaction. */
    Res apply(java.sql.Connection c, String account, String table, String method, String pk, String rk, Ent body, String ifMatch,
            String prefer, boolean batch) throws java.sql.SQLException {
        Ent cur = TableStore.get(c, account, table, pk, rk, true);
        switch (method) {
            case "POST" -> {
                if (cur != null) {
                    throw new AzureException(409, "EntityAlreadyExists", "The specified entity already exists.");
                }
                body.ts = TableStore.newTs();
                TableStore.put(c, account, table, body);
                boolean content = batch ? "return-content".equalsIgnoreCase(prefer) : !"return-no-content".equalsIgnoreCase(prefer);
                return new Res(content || batch && !"return-no-content".equalsIgnoreCase(prefer) ? 201 : 204, body, content);
            }
            case "PUT", "MERGE", "PATCH" -> {
                if (ifMatch != null) {
                    if (cur == null) {
                        throw new AzureException(404, "ResourceNotFound", "The specified resource does not exist.");
                    }
                    if (!ifMatch.trim().equals("*") && !ifMatch.trim().equals(cur.etag())) {
                        throw new AzureException(412, "UpdateConditionNotSatisfied",
                                "The update condition specified in the request was not satisfied.");
                    }
                }
                Ent n = body;
                if (!method.equals("PUT") && cur != null) {
                    n = new Ent();
                    n.pk = pk;
                    n.rk = rk;
                    n.props.putAll(cur.props);
                    n.props.putAll(body.props);
                }
                n.ts = TableStore.newTs();
                TableStore.put(c, account, table, n);
                return new Res(204, n, false);
            }
            case "DELETE" -> {
                if (ifMatch == null) {
                    throw AzErrors.missingHeader("If-Match");
                }
                if (cur == null) {
                    throw new AzureException(404, "ResourceNotFound", "The specified resource does not exist.");
                }
                if (!ifMatch.trim().equals("*") && !ifMatch.trim().equals(cur.etag())) {
                    throw new AzureException(412, "UpdateConditionNotSatisfied",
                            "The update condition specified in the request was not satisfied.");
                }
                TableStore.remove(c, account, table, pk, rk);
                return new Res(204, null, false);
            }
            default -> throw AzErrors.notAllowed(method);
        }
    }

    private void writeRes(AzReq r, HttpServletResponse resp, String table, String lv, Res res) throws IOException {
        if (res.entity() != null) {
            resp.setHeader("ETag", res.entity().etag());
        }
        if (res.status() == 201) {
            resp.setHeader("Preference-Applied", "return-content");
            resp.setHeader("Location", base(r) + table + "(PartitionKey='" + res.entity().pk + "',RowKey='" + res.entity().rk + "')");
            minimalSingle = false;
            json(r, resp, 201, lv, render(r, table, res.entity(), lv, true));
            return;
        }
        if ("POST".equals(r.method)) {
            resp.setHeader("Preference-Applied", "return-no-content");
        }
        resp.setStatus(res.status());
    }

    private void entity(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String table, String pk, String rk)
            throws IOException {
        String m = r.method;
        String lv = level(r);
        switch (m) {
            case "GET" -> {
                r.op = "GetEntity";
                auth.authorize('o', "r");
                requireTable(r, table);
                Ent e = store.getEntity(r.account, table, pk, rk);
                if (e == null) {
                    throw new AzureException(404, "ResourceNotFound", "The specified resource does not exist.");
                }
                resp.setHeader("ETag", e.etag());
                JsonObject o = render(r, table, e, lv, true);
                if (!lv.equals("nometadata")) {
                    o.addProperty("odata.metadata", base(r) + "$metadata#" + table + "/@Element");
                    JsonObject reordered = new JsonObject();
                    reordered.add("odata.metadata", o.get("odata.metadata"));
                    o.entrySet().forEach(en -> {
                        if (!en.getKey().equals("odata.metadata")) {
                            reordered.add(en.getKey(), en.getValue());
                        }
                    });
                    o = reordered;
                }
                json(r, resp, 200, lv, select(o, r.q("$select")));
            }
            case "PUT", "MERGE", "PATCH", "DELETE" -> {
                r.op = m.equals("DELETE") ? "DeleteEntity" : m.equals("PUT") ? "UpdateEntity" : "MergeEntity";
                r.write = true;
                auth.authorize('o', m.equals("DELETE") ? "d" : "u");
                Ent body = null;
                if (!m.equals("DELETE")) {
                    body = entityFromJson(withKeys(parseBody(r), pk, rk));
                    if (!body.pk.equals(pk) || !body.rk.equals(rk)) {
                        throw new AzureException(400, "InvalidInput", "The PartitionKey and RowKey in the request body must "
                                + "match the ones in the URL.");
                    }
                }
                requireTable(r, table);
                final Ent fb = body;
                Res res = store.shards.tx(store.ownerHost(r.account, table, pk), c -> apply(c, r.account, table, m, pk, rk, fb,
                        r.header("If-Match"), null, false));
                if (!m.equals("PUT") || true) {
                    if (res.entity() != null) {
                        resp.setHeader("ETag", res.entity().etag());
                    }
                }
                resp.setStatus(204);
            }
            case "POST" -> throw AzErrors.notAllowed(m);
            default -> throw AzErrors.notAllowed(m);
        }
    }

    private static JsonObject withKeys(JsonObject o, String pk, String rk) {
        if (!o.has("PartitionKey")) {
            o.addProperty("PartitionKey", pk);
        }
        if (!o.has("RowKey")) {
            o.addProperty("RowKey", rk);
        }
        return o;
    }

    private void query(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String table) throws IOException {
        r.op = "QueryEntities";
        auth.authorize('o', "r");
        String lv = level(r);
        requireTable(r, table);
        ODataFilter f;
        try {
            f = ODataFilter.parse(r.q("$filter"));
        } catch (ODataFilter.ParseError e) {
            throw new AzureException(400, "InvalidInput", "The query condition specified in the request is invalid.");
        }
        int top = Math.min(topParam(r), MAX_PAGE);
        TableStore.Page page = store.query(r.account, table, f, top, unb64(r.q("NextPartitionKey")), unb64(r.q("NextRowKey")));
        if (page.nextPk() != null) {
            resp.setHeader("x-ms-continuation-NextPartitionKey", b64(page.nextPk()));
            resp.setHeader("x-ms-continuation-NextRowKey", b64(page.nextRk()));
        }
        JsonObject o = new JsonObject();
        if (!lv.equals("nometadata")) {
            o.addProperty("odata.metadata", base(r) + "$metadata#" + table);
        }
        JsonArray arr = new JsonArray();
        for (Ent e : page.entities()) {
            JsonObject eo = render(r, table, e, lv, false);
            arr.add(select(eo, r.q("$select")));
        }
        o.add("value", arr);
        json(r, resp, 200, lv, o);
    }

    // ---- entity group transactions

    private void batch(AzReq r, HttpServletResponse resp, AzureAuth.Result auth) throws IOException {
        auth.authorize('o', "audu");
        String ct = r.header("Content-Type");
        Matcher bm = ct == null ? null : Pattern.compile("boundary=\"?([^\";]+)\"?").matcher(ct);
        if (bm == null || !bm.find()) {
            throw AzErrors.invalidHeader("Content-Type", ct);
        }
        byte[] raw = r.raw.getInputStream().readAllBytes();
        if (raw.length > 4 * 1024 * 1024) {
            throw new AzureException(413, "RequestBodyTooLarge", "The request body is too large and exceeds the maximum permissible limit.");
        }
        String body = new String(raw, StandardCharsets.UTF_8);
        Matcher cs = Pattern.compile("boundary=(changeset_[A-Za-z0-9-]+)").matcher(body);
        if (!cs.find()) {
            throw new AzureException(400, "InvalidInput", "The batch request must contain one changeset.");
        }
        String[] parts = body.split("--" + Pattern.quote(cs.group(1)));
        List<String[]> ops = new ArrayList<>();
        for (int i = 1; i < parts.length; i++) {
            String p = parts[i].replaceFirst("^\\r?\\n", "");
            if (p.startsWith("--")) {
                break;
            }
            int hdrEnd = p.indexOf("\r\n\r\n");
            if (hdrEnd < 0) {
                hdrEnd = p.indexOf("\n\n");
            }
            String http = hdrEnd < 0 ? p : p.substring(hdrEnd).stripLeading();
            int lineEnd = http.indexOf('\n');
            String reqLine = http.substring(0, lineEnd).trim();
            String rest = http.substring(lineEnd + 1);
            int bodyStart = rest.indexOf("\r\n\r\n");
            int sep = 4;
            if (bodyStart < 0) {
                bodyStart = rest.indexOf("\n\n");
                sep = 2;
            }
            String headerBlock = bodyStart < 0 ? rest : rest.substring(0, bodyStart);
            String jsonBody = bodyStart < 0 ? "" : rest.substring(bodyStart + sep).trim();
            ops.add(new String[] {reqLine, headerBlock, jsonBody});
        }
        if (ops.isEmpty()) {
            throw new AzureException(400, "InvalidInput", "The batch request contains no operations.");
        }
        if (ops.size() > 100) {
            throw new AzureException(400, "InvalidInput", "The batch request contains more than 100 operations.");
        }
        String lv = level(r);
        String rbatch = "batchresponse_" + UUID.randomUUID();
        String rcs = "changesetresponse_" + UUID.randomUUID();
        StringBuilder out = new StringBuilder();
        out.append("--").append(rbatch).append("\r\nContent-Type: multipart/mixed; boundary=").append(rcs).append("\r\n\r\n");
        String table = null;
        String batchPk = null;
        List<Object[]> plan = new ArrayList<>();
        java.util.Set<String> seen = new java.util.HashSet<>();
        int idx = 0;
        try {
            for (String[] op : ops) {
                String[] rl = op[0].split(" ");
                String method = rl[0];
                String target = rl[1];
                String path = target.startsWith("http") ? java.net.URI.create(target).getRawPath() : target;
                path = AzReq.decode(path, false);
                if (!r.hostStyle && path.startsWith("/" + r.account + "/")) {
                    path = path.substring(r.account.length() + 1);
                }
                String seg = path.startsWith("/") ? path.substring(1) : path;
                String t;
                String pk;
                String rk;
                Ent ent = null;
                Matcher em = ENTITY_PATH.matcher(seg);
                Map<String, String> hd = new LinkedHashMap<>();
                for (String line : op[1].split("\r?\n")) {
                    int c = line.indexOf(':');
                    if (c > 0) {
                        hd.put(line.substring(0, c).trim().toLowerCase(Locale.ROOT), line.substring(c + 1).trim());
                    }
                }
                if (em.matches()) {
                    t = em.group(1);
                    pk = unq(em.group(2));
                    rk = unq(em.group(3));
                    if (!method.equals("DELETE")) {
                        ent = entityFromJson(withKeys(parseJson(op[2].getBytes(StandardCharsets.UTF_8)), pk, rk));
                    }
                } else if (TABLE_NAME.matcher(seg).matches() && method.equals("POST")) {
                    t = seg;
                    ent = entityFromJson(parseJson(op[2].getBytes(StandardCharsets.UTF_8)));
                    pk = ent.pk;
                    rk = ent.rk;
                } else {
                    throw new AzureException(400, "InvalidInput", "Unsupported operation in batch: " + op[0]);
                }
                if (table == null) {
                    table = t;
                    batchPk = pk;
                } else if (!table.equals(t) || !batchPk.equals(pk)) {
                    throw new AzureException(400, "CommandsInBatchActOnDifferentPartitions",
                            "All commands in a batch must operate on same entity group.");
                }
                if (!seen.add(pk + "\u0000" + rk)) {
                    throw new AzureException(400, "InvalidDuplicateRow", "A command with RowKey '" + rk + "' is already present "
                            + "in the batch. An entity can appear only once in a batch. ");
                }
                plan.add(new Object[] {method, pk, rk, ent, hd.get("if-match"), hd.get("prefer")});
                idx++;
            }
            requireTable(r, table);
            final String ftable = table;
            List<Res> results = store.shards.tx(store.ownerHost(r.account, ftable, batchPk), c -> {
                List<Res> l = new ArrayList<>();
                for (int i = 0; i < plan.size(); i++) {
                    Object[] pl = plan.get(i);
                    try {
                        l.add(apply(c, r.account, ftable, (String) pl[0], (String) pl[1], (String) pl[2], (Ent) pl[3],
                                (String) pl[4], (String) pl[5], true));
                    } catch (AzureException e) {
                        AzureException w = new AzureException(e.status, e.code, i + ":" + e.getMessage());
                        throw w;
                    }
                }
                return l;
            });
            for (int i = 0; i < results.size(); i++) {
                Res res = results.get(i);
                out.append("--").append(rcs).append("\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\n");
                out.append("HTTP/1.1 ").append(res.status()).append(' ').append(res.status() == 201 ? "Created" : "No Content")
                        .append("\r\nX-Content-Type-Options: nosniff\r\nCache-Control: no-cache\r\nDataServiceVersion: 3.0;\r\n");
                if (res.status() == 201) {
                    String loc = base(r) + ftable + "(PartitionKey='" + res.entity().pk + "',RowKey='" + res.entity().rk + "')";
                    out.append("Location: ").append(loc).append("\r\nDataServiceId: ").append(loc).append("\r\n");
                }
                if (res.entity() != null) {
                    out.append("ETag: ").append(res.entity().etag()).append("\r\n");
                }
                if (res.status() == 201 && res.returnContent()) {
                    JsonObject o = render(r, ftable, res.entity(), lv, true);
                    out.append("Content-Type: ").append(contentType(lv)).append("\r\n\r\n").append(o).append("\r\n");
                } else {
                    out.append("\r\n");
                }
            }
        } catch (AzureException e) {
            AzureException w = e.getMessage() != null && e.getMessage().matches("^\\d+:.*") ? e
                    : new AzureException(e.status, e.code, idx + ":" + e.getMessage());
            out.append("--").append(rcs).append("\r\nContent-Type: application/http\r\nContent-Transfer-Encoding: binary\r\n\r\n");
            out.append("HTTP/1.1 ").append(w.status).append(' ').append(w.status == 404 ? "Not Found" : w.status == 409 ? "Conflict"
                    : w.status == 412 ? "Precondition Failed" : "Bad Request").append("\r\nContent-ID: ").append(idx + 1)
                    .append("\r\nDataServiceVersion: 3.0;\r\nContent-Type: ").append(contentType(lv)).append("\r\n\r\n");
            out.append(AzHttp.jsonError(w, r, lv)).append("\r\n");
        }
        out.append("--").append(rcs).append("--\r\n--").append(rbatch).append("--\r\n");
        byte[] b = out.toString().getBytes(StandardCharsets.UTF_8);
        resp.setStatus(202);
        resp.setContentType("multipart/mixed; boundary=" + rbatch);
        resp.setContentLength(b.length);
        resp.getOutputStream().write(b);
    }
}

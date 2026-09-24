package com.sayonora.wire.core.connector;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Turns a DynamoDB/MongoDB {@code WARP_BACKENDS} pseudo-URL into the operand map its connector's
 * schema factory takes -- the same {@code {..., "tables": {sqlName -> {..., "fields": [...]}}}}
 * shape the ported ThinkingSense factories were built around, just derived from Warp's own text
 * config instead of a JSON model file.
 *
 * <p><b>Grammar</b> (one {@code WARP_BACKENDS} entry is still {@code name=URL|user|password|fallback},
 * {@code ;}-separated -- nothing here may contain a raw {@code ;} or {@code |}, which is exactly why
 * the table/column config lives in the URL's own {@code &}-separated query string rather than as
 * embedded JSON or a {@code ;}-delimited table list):
 * <pre>{@code
 * dynamodb://<region>[?endpoint=<url>][&table.<sqlName>=<col>,<col>,...][&source.<sqlName>=<RealTableName>]
 *     user field     -> AWS accessKeyId      (optional; both blank = SDK default credential chain)
 *     password field -> AWS secretAccessKey  (may be a vault:/cyberark: reference)
 *
 * mongodb[+srv]://<host>:<port>[,<host>:<port>...]/<database>[?<driver options>]
 *         [&table.<sqlName>=<field>,<field>,...][&source.<sqlName>=<collectionName>]
 *     user/password fields -> the Mongo credential (password may be a vault:/cyberark: reference)
 * }</pre>
 * {@code table.<sqlName>} is REQUIRED per exposed table and its field list is explicit -- both
 * sources are schemaless (see {@code MongoSchemaFactory}'s javadoc), so there is nothing to
 * auto-discover. {@code source.<sqlName>} is optional; without it the real DynamoDB table / Mongo
 * collection name equals {@code sqlName}. Every other query parameter on a Mongo URL (e.g. {@code
 * authSource}, {@code replicaSet}, {@code directConnection}) is passed through to the driver
 * untouched; {@code table.*}/{@code source.*} are stripped first, since the driver would reject them
 * as unknown options. Values are URL-decoded, so a field name containing {@code &}/{@code =}/{@code ,}
 * can't be expressed -- a real, accepted limitation for v1.
 *
 * <p>Parsing never throws on missing tables: a backend with no {@code table.*} entries still
 * registers (so a typo doesn't silently drop the whole backend at startup), and the connector's own
 * factory rejects it with a clear error at the moment a federated query tries to mount it.
 */
public final class ConnectorOperands {

    private ConnectorOperands() {
    }

    public static Map<String, Object> parse(String url, String user, String password) {
        String lower = url == null ? "" : url.toLowerCase(Locale.ROOT);
        if (lower.startsWith("dynamodb://")) {
            return parseDynamo(url, user, password);
        }
        if (lower.startsWith("mongodb://") || lower.startsWith("mongodb+srv://")) {
            return parseMongo(url, user, password);
        }
        if (lower.startsWith("s3://")) {
            return parseS3(url, user, password);
        }
        if (lower.startsWith("kafka://")) {
            return parseKafka(url);
        }
        if (lower.startsWith("cassandra://")) {
            return parseCassandra(url, user, password);
        }
        if (lower.startsWith("splunk://")) {
            return parseSplunk(url, password);
        }
        throw new IllegalArgumentException("not a connector URL: " + url);
    }

    /**
     * {@code s3://<bucket>?key=<objectKey>&format=csv|xml|parquet[&recordElement=<xmlTag>]
     * [&region=][&provider=s3|s3-compatible][&endpoint=][&pathStyleAccess=true]
     * [&pushdownColumns=col,col][&table=<sqlName>]}. One exact object key per backend entry (no
     * glob/prefix-union -- see S3SchemaFactory's javadoc for why); {@code table} names the single
     * SQL table this backend exposes, defaulting to {@code "object"}. {@code user}/{@code password}
     * map to {@code accessKeyId}/{@code secretAccessKey}, same convention as
     * {@link #parseDynamo}'s own AWS credential fields -- blank/absent falls back to the SDK's
     * default credential chain, same as Dynamo.
     */
    private static Map<String, Object> parseS3(String url, String user, String password) {
        String rest = url.substring("s3://".length());
        int q = rest.indexOf('?');
        String bucket = (q < 0 ? rest : rest.substring(0, q)).replaceAll("/+$", "");
        Query query = Query.parse(q < 0 ? "" : rest.substring(q + 1));
        Map<String, String> p = query.passthrough;

        Map<String, Object> operand = new LinkedHashMap<>();
        operand.put("bucket", bucket.isBlank() ? null : bucket);
        operand.put("provider", p.getOrDefault("provider", "s3"));
        operand.put("region", p.get("region"));
        operand.put("endpoint", p.get("endpoint"));
        operand.put("pathStyleAccess", "true".equalsIgnoreCase(p.get("pathStyleAccess")));
        operand.put("accessKeyId", blankToNull(user));
        operand.put("secretAccessKey", blankToNull(password));

        Map<String, String> tableDef = new LinkedHashMap<>();
        tableDef.put("key", p.get("key"));
        tableDef.put("format", p.getOrDefault("format", "csv"));
        tableDef.put("recordElement", p.get("recordElement"));
        tableDef.put("pushdownColumns", p.get("pushdownColumns"));
        Map<String, Map<String, String>> tables = new LinkedHashMap<>();
        tables.put(p.getOrDefault("table", "object"), tableDef);
        operand.put("tables", tables);
        return operand;
    }

    /**
     * {@code kafka://<broker1:port>[,<broker2:port>...]/<topic>?format=json|string
     * [&fields=col,col,...][&table=<sqlName>]}. {@code fields} is required (and only meaningful)
     * for {@code format=json} -- see KafkaSchemaFactory's javadoc. {@code table} defaults to the
     * topic name itself. No credentials in this v1 grammar (matching the ported connector, which
     * only takes plaintext {@code bootstrap.servers} -- SASL/TLS broker auth is a real, separate
     * follow-on, not attempted here).
     */
    private static Map<String, Object> parseKafka(String url) {
        String rest = url.substring("kafka://".length());
        int q = rest.indexOf('?');
        String base = q < 0 ? rest : rest.substring(0, q);
        Query query = Query.parse(q < 0 ? "" : rest.substring(q + 1));
        Map<String, String> p = query.passthrough;

        int slash = base.indexOf('/');
        String bootstrapServers = slash < 0 ? base : base.substring(0, slash);
        String topic = slash < 0 ? null : blankToNull(base.substring(slash + 1));

        Map<String, Object> operand = new LinkedHashMap<>();
        operand.put("bootstrapServers", bootstrapServers.isBlank() ? null : bootstrapServers);
        Map<String, Object> tableDef = new LinkedHashMap<>();
        tableDef.put("topic", topic);
        tableDef.put("format", p.getOrDefault("format", "json"));
        String fieldsCsv = p.get("fields");
        List<String> fields = new ArrayList<>();
        if (fieldsCsv != null) {
            for (String f : fieldsCsv.split(",")) {
                if (!f.isBlank()) {
                    fields.add(f.trim());
                }
            }
        }
        tableDef.put("fields", fields);
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put(p.getOrDefault("table", topic == null ? "topic" : topic), tableDef);
        operand.put("tables", tables);
        return operand;
    }

    /**
     * {@code cassandra://<host:port>[,<host:port>...]/<keyspace>.<table>?localDc=<dc>
     * [&partitionKeyEquals=col,col][&allowFullScan=true][&table=<sqlName>]}. Exactly one of
     * {@code partitionKeyEquals}/{@code allowFullScan} is REQUIRED -- see CassandraSchemaFactory's
     * javadoc for why an unguarded full scan is refused by default. {@code user}/{@code password}
     * map to CQL auth credentials (blank/absent means no auth, matching a cluster with
     * {@code AllowAllAuthenticator}).
     */
    private static Map<String, Object> parseCassandra(String url, String user, String password) {
        String rest = url.substring("cassandra://".length());
        int q = rest.indexOf('?');
        String base = q < 0 ? rest : rest.substring(0, q);
        Query query = Query.parse(q < 0 ? "" : rest.substring(q + 1));
        Map<String, String> p = query.passthrough;

        int slash = base.indexOf('/');
        String contactPointsCsv = slash < 0 ? base : base.substring(0, slash);
        String keyspaceTable = slash < 0 ? null : blankToNull(base.substring(slash + 1));
        String keyspace = null;
        String table = null;
        if (keyspaceTable != null) {
            int dot = keyspaceTable.indexOf('.');
            if (dot > 0) {
                keyspace = keyspaceTable.substring(0, dot);
                table = keyspaceTable.substring(dot + 1);
            }
        }

        Map<String, Object> operand = new LinkedHashMap<>();
        List<String> contactPoints = new ArrayList<>();
        for (String cp : contactPointsCsv.split(",")) {
            if (!cp.isBlank()) {
                contactPoints.add(cp.trim());
            }
        }
        operand.put("contactPoints", contactPoints);
        operand.put("localDatacenter", p.get("localDc") != null ? p.get("localDc") : p.get("localDatacenter"));
        operand.put("username", blankToNull(user));
        operand.put("password", blankToNull(password));

        Map<String, Object> tableDef = new LinkedHashMap<>();
        tableDef.put("keyspace", keyspace);
        tableDef.put("table", table);
        tableDef.put("partitionKeyEquals", p.get("partitionKeyEquals"));
        tableDef.put("allowFullScan", "true".equalsIgnoreCase(p.get("allowFullScan")));
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put(p.getOrDefault("table", table == null ? "table" : table), tableDef);
        operand.put("tables", tables);
        return operand;
    }

    /**
     * {@code splunk://<host:port>?search=<SPL, URL-encoded>[&pushdownColumns=col,col]
     * [&pollTimeoutMs=<millis>][&table=<sqlName>]}. {@code password} (which may itself be a
     * {@code vault:}/{@code cyberark:} reference, resolved fresh at mount time by the factory --
     * never a literal in the URL) becomes the literal {@code Authorization} header value (e.g.
     * {@code "Splunk <token>"} or {@code "Bearer <token>"}) -- see SplunkSchemaFactory's javadoc.
     */
    private static Map<String, Object> parseSplunk(String url, String password) {
        String rest = url.substring("splunk://".length());
        int q = rest.indexOf('?');
        String host = (q < 0 ? rest : rest.substring(0, q)).replaceAll("/+$", "");
        Query query = Query.parse(q < 0 ? "" : rest.substring(q + 1));
        Map<String, String> p = query.passthrough;

        Map<String, Object> operand = new LinkedHashMap<>();
        operand.put("endpoint", host.isBlank() ? null : "https://" + host);
        operand.put("authHeader", blankToNull(password));
        Long pollTimeoutMs = p.get("pollTimeoutMs") != null ? Long.valueOf(p.get("pollTimeoutMs")) : null;
        operand.put("pollTimeoutMs", pollTimeoutMs);

        Map<String, Object> tableDef = new LinkedHashMap<>();
        tableDef.put("search", p.get("search"));
        tableDef.put("pushdownColumns", p.get("pushdownColumns") == null ? List.of()
                : List.of(p.get("pushdownColumns").split(",")));
        Map<String, Object> tables = new LinkedHashMap<>();
        tables.put(p.getOrDefault("table", "search"), tableDef);
        operand.put("tables", tables);
        return operand;
    }

    private static Map<String, Object> parseDynamo(String url, String user, String password) {
        String rest = url.substring("dynamodb://".length());
        int q = rest.indexOf('?');
        String region = (q < 0 ? rest : rest.substring(0, q)).replaceAll("/+$", "");
        Query query = Query.parse(q < 0 ? "" : rest.substring(q + 1));

        Map<String, Object> operand = new LinkedHashMap<>();
        operand.put("region", region.isBlank() ? null : region);
        operand.put("endpoint", query.passthrough.get("endpoint"));
        operand.put("accessKeyId", blankToNull(user));
        operand.put("secretAccessKey", blankToNull(password));
        Map<String, Map<String, Object>> tables = new LinkedHashMap<>();
        query.tableFields.forEach((sqlName, fields) -> {
            Map<String, Object> def = new LinkedHashMap<>();
            def.put("table", query.sources.getOrDefault(sqlName, sqlName));
            def.put("fields", fields);
            tables.put(sqlName, def);
        });
        operand.put("tables", tables);
        return operand;
    }

    private static Map<String, Object> parseMongo(String url, String user, String password) {
        int schemeEnd = url.indexOf("://") + 3;
        int q = url.indexOf('?', schemeEnd);
        String base = q < 0 ? url : url.substring(0, q);
        Query query = Query.parse(q < 0 ? "" : url.substring(q + 1));

        int slash = base.indexOf('/', schemeEnd);
        String database = slash < 0 ? null : blankToNull(base.substring(slash + 1));

        // Rebuild the connection string WITHOUT this grammar's own table./source. keys -- the Mongo
        // driver rejects unknown options -- keeping every real driver option in its original order.
        StringBuilder connectionString = new StringBuilder(base);
        if (!query.passthroughRaw.isEmpty()) {
            connectionString.append('?').append(String.join("&", query.passthroughRaw));
        }

        Map<String, Object> operand = new LinkedHashMap<>();
        operand.put("connectionString", connectionString.toString());
        operand.put("database", database);
        operand.put("user", blankToNull(user));
        operand.put("password", blankToNull(password));
        Map<String, Map<String, Object>> tables = new LinkedHashMap<>();
        query.tableFields.forEach((sqlName, fields) -> {
            Map<String, Object> def = new LinkedHashMap<>();
            def.put("database", database);
            def.put("collection", query.sources.getOrDefault(sqlName, sqlName));
            def.put("fields", fields);
            tables.put(sqlName, def);
        });
        operand.put("tables", tables);
        return operand;
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s;
    }

    private record Query(Map<String, List<String>> tableFields, Map<String, String> sources,
            Map<String, String> passthrough, List<String> passthroughRaw) {

        static Query parse(String raw) {
            Map<String, List<String>> tableFields = new LinkedHashMap<>();
            Map<String, String> sources = new LinkedHashMap<>();
            Map<String, String> passthrough = new LinkedHashMap<>();
            List<String> passthroughRaw = new ArrayList<>();
            for (String pair : raw.split("&")) {
                if (pair.isBlank()) {
                    continue;
                }
                int eq = pair.indexOf('=');
                String key = decode(eq < 0 ? pair : pair.substring(0, eq)).trim();
                String value = eq < 0 ? "" : decode(pair.substring(eq + 1)).trim();
                if (key.startsWith("table.") && key.length() > "table.".length()) {
                    List<String> fields = new ArrayList<>();
                    for (String f : value.split(",")) {
                        if (!f.isBlank()) {
                            fields.add(f.trim());
                        }
                    }
                    tableFields.put(key.substring("table.".length()), List.copyOf(fields));
                } else if (key.startsWith("source.") && key.length() > "source.".length()) {
                    sources.put(key.substring("source.".length()), value);
                } else {
                    passthrough.put(key, value);
                    passthroughRaw.add(pair);
                }
            }
            return new Query(tableFields, sources, passthrough, passthroughRaw);
        }

        private static String decode(String s) {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        }
    }
}

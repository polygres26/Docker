package com.polygres.wire.dynamowire;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Maps DynamoDB tables/items onto real Postgres tables, following ExtendDB's storage-postgres
 * design as described in its architecture guide (docs/manuals/01-architecture-guide.md, read via
 * WebFetch — GitHub raw content — for this port; the actual Rust source wasn't fetchable in this
 * environment, so this is a from-description reimplementation, not a port of its code):
 * <ul>
 *   <li>One real Postgres table per DynamoDB table: {@code dynamo_item_<name>}, with the
 *       partition key (and sort key, if present) as real indexed columns ({@code pk_value text},
 *       {@code sk_value text}, plus {@code sk_num numeric} when the sort key type is N, so numeric
 *       sort-key ordering/comparisons are correct rather than lexicographic).</li>
 *   <li>The full item — including the key attributes themselves — stored as JSONB in the
 *       {@code item} column, in DynamoDB's own typed-attribute-value JSON shape (see
 *       {@link AttributeValue}), so nothing is lost in translation and reads are exact round-trips.</li>
 *   <li>A catalog table {@code _dynamo_tables} holding each table's key schema, mirroring
 *       ExtendDB's separation of "engine layer" (expression parsing/filtering, done here in Java —
 *       see {@link ConditionExpressionEvaluator}/{@link UpdateExpressionParser}) from storage.</li>
 * </ul>
 * Divergence from ExtendDB noted where it matters: no GSI/LSI support in this pass (ExtendDB gives
 * each index its own Postgres table; out of scope here — see class javadoc on {@link DynamoWireServer}
 * for the full list of what's deferred). {@code Limit} is applied to the number of items returned
 * after FilterExpression, not to the number of items evaluated before it as real DynamoDB does —
 * a real, documented simplification, not an oversight.
 */
public final class PgItemStore {

    private static final Logger log = LoggerFactory.getLogger(PgItemStore.class);
    private static final Pattern SAFE_IDENT = Pattern.compile("[^a-zA-Z0-9_]");

    private final HikariDataSource ds;

    public PgItemStore(String host, int port, String database, String user, String password) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + database);
        if (user != null) cfg.setUsername(user);
        if (password != null) cfg.setPassword(password);
        cfg.setPoolName("dynamowire-pg-pool");
        cfg.setMaximumPoolSize(8);
        this.ds = new HikariDataSource(cfg);
        ensureCatalog();
    }

    private void ensureCatalog() {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute("""
                CREATE TABLE IF NOT EXISTS _dynamo_tables (
                    table_name text PRIMARY KEY,
                    pg_table text NOT NULL,
                    pk_name text NOT NULL,
                    pk_type text NOT NULL,
                    sk_name text,
                    sk_type text,
                    status text NOT NULL,
                    creation_time_millis bigint NOT NULL
                )
                """);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize _dynamo_tables catalog", e);
        }
    }

    private static String pgTableName(String dynamoTableName) {
        return "dynamo_item_" + SAFE_IDENT.matcher(dynamoTableName.toLowerCase()).replaceAll("_");
    }

    // ---------------------------------------------------------------- table management

    public TableSchema createTable(String tableName, String pkName, String pkType, String skName, String skType) {
        String pg = pgTableName(tableName);
        try (Connection c = ds.getConnection()) {
            try (var ps = c.prepareStatement("SELECT 1 FROM _dynamo_tables WHERE table_name = ?")) {
                ps.setString(1, tableName);
                if (ps.executeQuery().next()) {
                    throw new DynamoException("ResourceInUseException", "Table already exists: " + tableName);
                }
            }
            try (var st = c.createStatement()) {
                StringBuilder ddl = new StringBuilder("CREATE TABLE " + pg + " (pk_value text NOT NULL, sk_value text NOT NULL DEFAULT '', ");
                ddl.append("sk_num numeric, item jsonb NOT NULL, PRIMARY KEY (pk_value, sk_value))");
                st.execute(ddl.toString());
                st.execute("CREATE INDEX " + pg + "_pk_sknum_idx ON " + pg + " (pk_value, sk_num)");
            }
            long now = System.currentTimeMillis();
            try (var ps = c.prepareStatement(
                    "INSERT INTO _dynamo_tables (table_name, pg_table, pk_name, pk_type, sk_name, sk_type, status, creation_time_millis) VALUES (?,?,?,?,?,?,?,?)")) {
                ps.setString(1, tableName);
                ps.setString(2, pg);
                ps.setString(3, pkName);
                ps.setString(4, pkType);
                ps.setString(5, skName);
                ps.setString(6, skType);
                ps.setString(7, "ACTIVE");
                ps.setLong(8, now);
                ps.executeUpdate();
            }
            return new TableSchema(tableName, pkName, pkType, skName, skType, "ACTIVE", now);
        } catch (SQLException e) {
            throw new RuntimeException("CreateTable failed for " + tableName, e);
        }
    }

    public void deleteTable(String tableName) {
        TableSchema schema = describeTable(tableName);
        try (Connection c = ds.getConnection()) {
            try (var st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + pgTableName(tableName));
            }
            try (var ps = c.prepareStatement("DELETE FROM _dynamo_tables WHERE table_name = ?")) {
                ps.setString(1, tableName);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("DeleteTable failed for " + tableName, e);
        }
    }

    public TableSchema describeTable(String tableName) {
        try (Connection c = ds.getConnection();
                var ps = c.prepareStatement("SELECT pk_name, pk_type, sk_name, sk_type, status, creation_time_millis FROM _dynamo_tables WHERE table_name = ?")) {
            ps.setString(1, tableName);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) throw new DynamoException("ResourceNotFoundException", "Table not found: " + tableName);
                return new TableSchema(tableName, rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4), rs.getString(5), rs.getLong(6));
            }
        } catch (SQLException e) {
            throw new RuntimeException("DescribeTable failed for " + tableName, e);
        }
    }

    public long itemCount(TableSchema schema) {
        try (Connection c = ds.getConnection(); var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT count(*) FROM " + pgTableName(schema.tableName()))) {
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count items in " + schema.tableName(), e);
        }
    }

    public List<String> listTables() {
        List<String> out = new ArrayList<>();
        try (Connection c = ds.getConnection(); var st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT table_name FROM _dynamo_tables ORDER BY table_name")) {
            while (rs.next()) out.add(rs.getString(1));
        } catch (SQLException e) {
            throw new RuntimeException("ListTables failed", e);
        }
        return out;
    }

    // ---------------------------------------------------------------- item CRUD

    private static String keyToken(Map<String, AttributeValue> item, String attr) {
        AttributeValue v = item.get(attr);
        if (v == null) throw new DynamoException("ValidationException", "Missing required key attribute: " + attr);
        return v.scalar;
    }

    /** Returns the previous item (for ReturnValues=ALL_OLD), or null if none existed / condition check applies pre-write. */
    public Map<String, AttributeValue> putItem(TableSchema schema, Map<String, AttributeValue> item, String conditionExpr, ExpressionContext ctx) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(item, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(item, schema.sortKeyName()) : "";
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readForUpdate(c, pg, pk, sk);
                if (conditionExpr != null && !ConditionExpressionEvaluator.evaluate(conditionExpr, existing, ctx)) {
                    c.rollback();
                    throw new DynamoException("ConditionalCheckFailedException", "The conditional request failed");
                }
                String json = itemToJson(item).toString();
                BigDecimal skNum = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? new BigDecimal(sk) : null;
                try (var ps = c.prepareStatement(
                        "INSERT INTO " + pg + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?::jsonb) " +
                        "ON CONFLICT (pk_value, sk_value) DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item")) {
                    ps.setString(1, pk);
                    ps.setString(2, sk);
                    if (skNum != null) ps.setBigDecimal(3, skNum); else ps.setNull(3, java.sql.Types.NUMERIC);
                    ps.setString(4, json);
                    ps.executeUpdate();
                }
                c.commit();
                return existing;
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                if (e instanceof DynamoException de) throw de;
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("PutItem failed", e);
        }
    }

    private Map<String, AttributeValue> readForUpdate(Connection c, String pg, String pk, String sk) throws SQLException {
        try (var ps = c.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ? FOR UPDATE")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        }
    }

    public Map<String, AttributeValue> getItem(TableSchema schema, Map<String, AttributeValue> key) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(key, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(key, schema.sortKeyName()) : "";
        try (Connection c = ds.getConnection();
                var ps = c.prepareStatement("SELECT item FROM " + pg + " WHERE pk_value = ? AND sk_value = ?")) {
            ps.setString(1, pk);
            ps.setString(2, sk);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
            }
        } catch (SQLException e) {
            throw new RuntimeException("GetItem failed", e);
        }
    }

    public Map<String, AttributeValue> deleteItem(TableSchema schema, Map<String, AttributeValue> key, String conditionExpr, ExpressionContext ctx) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(key, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(key, schema.sortKeyName()) : "";
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readForUpdate(c, pg, pk, sk);
                if (conditionExpr != null && !ConditionExpressionEvaluator.evaluate(conditionExpr, existing, ctx)) {
                    c.rollback();
                    throw new DynamoException("ConditionalCheckFailedException", "The conditional request failed");
                }
                try (var ps = c.prepareStatement("DELETE FROM " + pg + " WHERE pk_value = ? AND sk_value = ?")) {
                    ps.setString(1, pk);
                    ps.setString(2, sk);
                    ps.executeUpdate();
                }
                c.commit();
                return existing;
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                if (e instanceof DynamoException de) throw de;
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("DeleteItem failed", e);
        }
    }

    public Map<String, AttributeValue> updateItem(TableSchema schema, Map<String, AttributeValue> key, String updateExpr,
            String conditionExpr, ExpressionContext ctx) {
        String pg = pgTableName(schema.tableName());
        String pk = keyToken(key, schema.partitionKeyName());
        String sk = schema.hasSortKey() ? keyToken(key, schema.sortKeyName()) : "";
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                Map<String, AttributeValue> existing = readForUpdate(c, pg, pk, sk);
                if (conditionExpr != null && !ConditionExpressionEvaluator.evaluate(conditionExpr, existing, ctx)) {
                    c.rollback();
                    throw new DynamoException("ConditionalCheckFailedException", "The conditional request failed");
                }
                Map<String, AttributeValue> item = existing != null ? new LinkedHashMap<>(existing) : new LinkedHashMap<>();
                // Key attributes are always present, even on brand-new items created by UpdateItem.
                item.putAll(key);
                UpdateExpressionParser.apply(updateExpr, item, ctx);
                item.putAll(key); // keys are immutable — re-assert after SET/REMOVE in case an expression touched them
                String json = itemToJson(item).toString();
                BigDecimal skNum = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? new BigDecimal(sk) : null;
                try (var ps = c.prepareStatement(
                        "INSERT INTO " + pg + " (pk_value, sk_value, sk_num, item) VALUES (?,?,?,?::jsonb) " +
                        "ON CONFLICT (pk_value, sk_value) DO UPDATE SET sk_num = EXCLUDED.sk_num, item = EXCLUDED.item")) {
                    ps.setString(1, pk);
                    ps.setString(2, sk);
                    if (skNum != null) ps.setBigDecimal(3, skNum); else ps.setNull(3, java.sql.Types.NUMERIC);
                    ps.setString(4, json);
                    ps.executeUpdate();
                }
                c.commit();
                return item;
            } catch (RuntimeException | SQLException e) {
                c.rollback();
                if (e instanceof DynamoException de) throw de;
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("UpdateItem failed", e);
        }
    }

    // ---------------------------------------------------------------- Query / Scan

    public record PageResult(List<Map<String, AttributeValue>> items, Map<String, AttributeValue> lastEvaluatedKey) {}

    public PageResult query(TableSchema schema, KeyConditionParser keyCond, ExpressionContext ctx,
            String filterExpr, Integer limit, Map<String, AttributeValue> exclusiveStartKey, boolean scanForward) {
        String pg = pgTableName(schema.tableName());
        AttributeValue pkVal = ctx.resolveValue(keyCond.partitionValueToken);
        StringBuilder sql = new StringBuilder("SELECT item, pk_value, sk_value FROM " + pg + " WHERE pk_value = ?");
        List<Object> params = new ArrayList<>();
        params.add(pkVal.scalar);
        appendSortCondition(sql, params, schema, keyCond, ctx);
        boolean numericSort = schema.hasSortKey() && "N".equals(schema.sortKeyType());
        String orderCol = numericSort ? "sk_num" : "sk_value";
        if (exclusiveStartKey != null && schema.hasSortKey()) {
            AttributeValue startSk = exclusiveStartKey.get(schema.sortKeyName());
            sql.append(scanForward ? " AND " + orderCol + " > ?" : " AND " + orderCol + " < ?");
            params.add(sortParam(schema, startSk));
        }
        sql.append(" ORDER BY ").append(orderCol).append(scanForward ? " ASC" : " DESC");
        return runAndFilter(pg, schema, sql.toString(), params, filterExpr, ctx, limit);
    }

    private void appendSortCondition(StringBuilder sql, List<Object> params, TableSchema schema, KeyConditionParser kc, ExpressionContext ctx) {
        String skCol = schema.hasSortKey() && "N".equals(schema.sortKeyType()) ? "sk_num" : "sk_value";
        switch (kc.sortOp) {
            case NONE -> {}
            case EQ -> { sql.append(" AND ").append(skCol).append(" = ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case LT -> { sql.append(" AND ").append(skCol).append(" < ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case LE -> { sql.append(" AND ").append(skCol).append(" <= ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case GT -> { sql.append(" AND ").append(skCol).append(" > ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case GE -> { sql.append(" AND ").append(skCol).append(" >= ?"); params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken))); }
            case BETWEEN -> {
                sql.append(" AND ").append(skCol).append(" BETWEEN ? AND ?");
                params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken)));
                params.add(sortParam(schema, ctx.resolveValue(kc.sortValueToken2)));
            }
            case BEGINS_WITH -> { sql.append(" AND sk_value LIKE ?"); params.add(escapeLike(ctx.resolveValue(kc.sortValueToken).scalar) + "%"); }
        }
    }

    private Object sortParam(TableSchema schema, AttributeValue v) {
        return "N".equals(schema.sortKeyType()) ? new BigDecimal(v.scalar) : v.scalar;
    }

    private static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    public PageResult scan(TableSchema schema, String filterExpr, ExpressionContext ctx, Integer limit, Map<String, AttributeValue> exclusiveStartKey) {
        String pg = pgTableName(schema.tableName());
        StringBuilder sql = new StringBuilder("SELECT item, pk_value, sk_value FROM " + pg);
        List<Object> params = new ArrayList<>();
        if (exclusiveStartKey != null) {
            sql.append(" WHERE (pk_value, sk_value) > (?, ?)");
            params.add(exclusiveStartKey.get(schema.partitionKeyName()).scalar);
            params.add(schema.hasSortKey() ? exclusiveStartKey.get(schema.sortKeyName()).scalar : "");
        }
        sql.append(" ORDER BY pk_value, sk_value");
        return runAndFilter(pg, schema, sql.toString(), params, filterExpr, ctx, limit);
    }

    private PageResult runAndFilter(String pg, TableSchema schema, String sql, List<Object> params, String filterExpr,
            ExpressionContext ctx, Integer limit) {
        List<Map<String, AttributeValue>> results = new ArrayList<>();
        Map<String, AttributeValue> lastKey = null;
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, AttributeValue> item = jsonToItem(JsonParser.parseString(rs.getString(1)).getAsJsonObject());
                    if (filterExpr != null && !ConditionExpressionEvaluator.evaluate(filterExpr, item, ctx)) continue;
                    results.add(item);
                    if (limit != null && results.size() >= limit) {
                        lastKey = keyOf(schema, item);
                        // Peek whether more rows exist to decide whether LastEvaluatedKey should really be set.
                        if (rs.next()) {
                            // there is at least one more row — keep lastKey as set above.
                        } else {
                            lastKey = null;
                        }
                        break;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Query/Scan failed", e);
        }
        return new PageResult(results, lastKey);
    }

    private Map<String, AttributeValue> keyOf(TableSchema schema, Map<String, AttributeValue> item) {
        Map<String, AttributeValue> k = new LinkedHashMap<>();
        k.put(schema.partitionKeyName(), item.get(schema.partitionKeyName()));
        if (schema.hasSortKey()) k.put(schema.sortKeyName(), item.get(schema.sortKeyName()));
        return k;
    }

    // ---------------------------------------------------------------- transactions (single-backend)

    /** Runs {@code work} inside one real Postgres transaction (BEGIN/COMMIT) — see DynamoWireServer's
     * javadoc on TransactWriteItems/TransactGetItems for why XA isn't needed here. */
    public <T> T runInTransaction(java.util.function.Function<Connection, T> work) {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                T result = work.apply(c);
                c.commit();
                return result;
            } catch (RuntimeException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Transaction failed", e);
        }
    }

    public String tableToPgName(String tableName) {
        return pgTableName(tableName);
    }

    // ---------------------------------------------------------------- JSON <-> item map

    public static JsonObject itemToJson(Map<String, AttributeValue> item) {
        JsonObject obj = new JsonObject();
        for (var e : item.entrySet()) obj.add(e.getKey(), e.getValue().toJson());
        return obj;
    }

    public static Map<String, AttributeValue> jsonToItem(JsonObject obj) {
        Map<String, AttributeValue> item = new LinkedHashMap<>();
        for (var e : obj.entrySet()) item.put(e.getKey(), AttributeValue.fromJson(e.getValue()));
        return item;
    }

    public void close() {
        ds.close();
    }
}

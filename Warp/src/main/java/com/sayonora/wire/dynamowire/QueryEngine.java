package com.sayonora.wire.dynamowire;

import com.google.gson.JsonParser;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Query and Scan over a table or one of its secondary indexes, on one shard or scattered over all
 * of them. Every shard returns its rows already ordered ({@code COLLATE "C"} byte order, numeric
 * order for numbers) and the shards' streams are merged k-way, so {@code Limit},
 * {@code ExclusiveStartKey}/{@code LastEvaluatedKey} pagination and the 1 MB page cut-off are exact
 * across shards. As in DynamoDB, {@code Limit} bounds the items <em>evaluated</em> (before the
 * filter), and {@code LastEvaluatedKey} is the key of the last evaluated item.
 */
final class QueryEngine {

    static final long PAGE_BYTES = 1024 * 1024;

    private final PgItemStore store;

    QueryEngine(PgItemStore store) {
        this.store = store;
    }

    /** What to read. {@code key == null} means Scan. */
    record Spec(TableSchema schema, TableSchema.IndexDef index, KeyPlanner.Plan key, Expr.Cond filter, Integer limit,
            Map<String, AttributeValue> startKey, boolean forward, int segment, int totalSegments,
            boolean collectItems) {}

    record Result(List<Map<String, AttributeValue>> items, int matched, int scannedCount, long scannedBytes,
            Map<String, AttributeValue> lastEvaluatedKey) {}

    /** One ORDER BY column: SQL text, whether numeric, and how to get its bind value from a key map. */
    private record Col(String sql, boolean numeric, String attr, TableSchema.KeyAttr keyAttr, String kind) {}

    // ---------------------------------------------------------------------------------------- run

    Result run(Spec spec) {
        TableSchema s = spec.schema();
        TableSchema.IndexDef idx = spec.index();
        String pg = PgItemStore.pgTableName(s.tableName());
        boolean scan = spec.key() == null;

        List<Col> order = orderColumns(s, idx, scan);
        StringBuilder sql = new StringBuilder("SELECT item::text, pk_value, sk_value");
        for (int i = 0; i < order.size(); i++) sql.append(", ").append(order.get(i).sql()).append(" AS o").append(i);
        sql.append(" FROM ").append(pg);
        List<String> where = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        if (!scan) {
            KeyPlanner.Plan k = spec.key();
            for (int i = 0; i < k.hashAttrs().size(); i++) {
                TableSchema.KeyAttr a = k.hashAttrs().get(i);
                if (idx == null || idx.local()) {
                    where.add("pk_value = ?");
                    params.add(KeyCodec.token(k.hashValues().get(i)));
                } else {
                    where.add(IndexSql.keyExpr(a) + " = ?");
                    params.add(IndexSql.bind(a, k.hashValues().get(i)));
                }
            }
            for (Expr.KeyCond c : k.rangeConds()) {
                TableSchema.KeyAttr a = rangeAttr(k, c.attr());
                String col = rangeCol(s, idx, a);
                switch (c.op()) {
                    case "EQ" -> { where.add(col + " = ?"); params.add(rangeBind(s, idx, a, c.v1())); }
                    case "LT" -> { where.add(col + " < ?"); params.add(rangeBind(s, idx, a, c.v1())); }
                    case "LE" -> { where.add(col + " <= ?"); params.add(rangeBind(s, idx, a, c.v1())); }
                    case "GT" -> { where.add(col + " > ?"); params.add(rangeBind(s, idx, a, c.v1())); }
                    case "GE" -> { where.add(col + " >= ?"); params.add(rangeBind(s, idx, a, c.v1())); }
                    case "BETWEEN" -> {
                        where.add(col + " BETWEEN ? AND ?");
                        params.add(rangeBind(s, idx, a, c.v1()));
                        params.add(rangeBind(s, idx, a, c.v2()));
                    }
                    case "BEGINS_WITH" -> {
                        where.add(col + " LIKE ?");
                        params.add(escapeLike(rangeBindText(a, c.v1())) + "%");
                    }
                    default -> throw new IllegalStateException(c.op());
                }
            }
        }
        if (idx != null) {
            for (TableSchema.KeyAttr a : idx.presenceKeys()) where.add(IndexSql.presence(a));
        }
        if (scan && spec.totalSegments() > 0) {
            where.add("(hashtextextended(pk_value, 0) & 9223372036854775807) % ? = ?");
            params.add((long) spec.totalSegments());
            params.add((long) spec.segment());
        }
        if (spec.startKey() != null) {
            StringBuilder lhs = new StringBuilder("("), rhs = new StringBuilder("(");
            for (int i = 0; i < order.size(); i++) {
                Col c = order.get(i);
                if (i > 0) { lhs.append(", "); rhs.append(", "); }
                lhs.append(c.sql());
                rhs.append("?::").append(c.numeric() ? "numeric" : "text");
                params.add(bindFromKey(s, c, spec.startKey()));
            }
            String op = spec.forward() ? " > " : " < ";
            where.add(lhs.append(")").toString() + op + rhs.append(")").toString());
        }
        if (!where.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ", where));
        sql.append(" ORDER BY ");
        for (int i = 0; i < order.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(order.get(i).sql()).append(spec.forward() ? " ASC" : " DESC");
        }
        if (spec.limit() != null) {
            sql.append(" LIMIT ").append(spec.limit());
        }

        // A read that returns a handful of rows runs as a plain autocommit statement (one round trip); anything
        // that may return many rows streams through a cursor, which needs a transaction on the connection.
        boolean bounded = isBounded(spec, scan);
        List<Connection> conns = openConnections(spec, scan);
        List<Cursor> cursors = new ArrayList<>();
        try {
            for (Connection c : conns) {
                if (!bounded) c.setAutoCommit(false);
                cursors.add(open(c, s, sql.toString(), params, order.size(), bounded));
            }
            return consume(spec, order, cursors);
        } catch (SQLException e) {
            throw new RuntimeException("Query/Scan failed", e);
        } finally {
            for (Cursor cur : cursors) cur.close();
            for (Connection c : conns) {
                try {
                    if (!bounded) {
                        c.rollback();
                        c.setAutoCommit(true);
                    }
                } catch (SQLException ignored) {
                    // returning to the pool anyway
                }
                try {
                    c.close();
                } catch (SQLException ignored) {
                    // nothing to do
                }
            }
        }
    }

    /** True when the statement can return only a few rows: a small Limit, or a query pinning the whole primary key. */
    private static boolean isBounded(Spec spec, boolean scan) {
        if (spec.limit() != null && spec.limit() <= 200) return true;
        if (scan || spec.index() != null) return false;
        TableSchema s = spec.schema();
        List<Expr.KeyCond> r = spec.key().rangeConds();
        return !s.hasSortKey() || (r.size() == 1 && r.get(0).op().equals("EQ"));
    }

    private List<Connection> openConnections(Spec spec, boolean scan) {
        try {
            boolean singleShard = !scan && (spec.index() == null || spec.index().local());
            if (singleShard) {
                String pk = KeyCodec.token(spec.key().hashValues().get(0));
                return new ArrayList<>(List.of(store.borrowShardConnection(pk)));
            }
            return store.borrowAllShardConnections();
        } catch (SQLException e) {
            throw new RuntimeException("Query/Scan failed", e);
        }
    }

    private Cursor open(Connection c, TableSchema s, String sql, List<Object> params, int orderCols, boolean bounded) throws SQLException {
        try {
            return execute(c, sql, params, orderCols, bounded);
        } catch (SQLException e) {
            if (!"42P01".equals(e.getSQLState()) || !store.mayCreateMissingTable(c)) throw e;
            if (!c.getAutoCommit()) c.rollback();
            store.createPhysicalTable(c, s);
            return execute(c, sql, params, orderCols, bounded);
        }
    }

    private Cursor execute(Connection c, String sql, List<Object> params, int orderCols, boolean bounded) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        try {
            if (!bounded) ps.setFetchSize(500);
            for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
            return new Cursor(ps, ps.executeQuery(), orderCols);
        } catch (SQLException e) {
            ps.close();
            throw e;
        }
    }

    // ------------------------------------------------------------------------------- columns

    private List<Col> orderColumns(TableSchema s, TableSchema.IndexDef idx, boolean scan) {
        List<Col> cols = new ArrayList<>();
        if (idx == null) {
            if (scan) cols.add(pkCol());
            if (s.hasSortKey()) cols.add(skCol(s));
            else cols.add(new Col("(sk_value COLLATE \"C\")", false, null, null, "SK"));
            return cols;
        }
        if (scan && !idx.local()) {
            for (TableSchema.KeyAttr a : idx.hash()) cols.add(attrCol(a));
        } else if (scan) {
            cols.add(pkCol());
        }
        for (TableSchema.KeyAttr a : idx.range()) cols.add(attrCol(a));
        cols.add(pkCol());
        cols.add(new Col("(sk_value COLLATE \"C\")", false, null, null, "SK"));
        return cols;
    }

    private static Col pkCol() {
        return new Col("(pk_value COLLATE \"C\")", false, null, null, "PK");
    }

    private static Col skCol(TableSchema s) {
        return "N".equals(s.sortKeyType()) ? new Col("sk_num", true, null, null, "SK")
                : new Col("(sk_value COLLATE \"C\")", false, null, null, "SK");
    }

    private static Col attrCol(TableSchema.KeyAttr a) {
        return new Col(IndexSql.keyExpr(a), IndexSql.numeric(a), a.name(), a, "ATTR");
    }

    private static TableSchema.KeyAttr rangeAttr(KeyPlanner.Plan k, String name) {
        for (TableSchema.KeyAttr a : k.rangeAttrs()) if (a.name().equals(name)) return a;
        throw new IllegalStateException(name);
    }

    /** SQL for a range attribute: the sort-key columns for the base table, else the index expression. */
    private String rangeCol(TableSchema s, TableSchema.IndexDef idx, TableSchema.KeyAttr a) {
        if (idx == null) return "N".equals(a.type()) ? "sk_num" : "(sk_value COLLATE \"C\")";
        return IndexSql.keyExpr(a);
    }

    private Object rangeBind(TableSchema s, TableSchema.IndexDef idx, TableSchema.KeyAttr a, AttributeValue v) {
        if (idx == null) return "N".equals(a.type()) ? new BigDecimal(v.scalar) : KeyCodec.token(v);
        return IndexSql.bind(a, v);
    }

    private String rangeBindText(TableSchema.KeyAttr a, AttributeValue prefix) {
        return "B".equals(a.type()) ? KeyCodec.hex(prefix.bytes()) : prefix.scalar;
    }

    private static String escapeLike(String s) {
        return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_");
    }

    private Object bindFromKey(TableSchema s, Col c, Map<String, AttributeValue> key) {
        switch (c.kind()) {
            case "PK":
                return KeyCodec.token(key.get(s.partitionKeyName()));
            case "SK":
                if (!s.hasSortKey()) return "";
                AttributeValue sk = key.get(s.sortKeyName());
                return c.numeric() ? new BigDecimal(sk.scalar) : KeyCodec.token(sk);
            default:
                return IndexSql.bind(c.keyAttr(), key.get(c.attr()));
        }
    }

    // ------------------------------------------------------------------------------- merge

    private static final class Cursor {
        final PreparedStatement ps;
        final ResultSet rs;
        final int orderCols;
        String json;
        Object[] keys;
        boolean exhausted;

        Cursor(PreparedStatement ps, ResultSet rs, int orderCols) {
            this.ps = ps;
            this.rs = rs;
            this.orderCols = orderCols;
        }

        boolean advance() throws SQLException {
            if (exhausted || !rs.next()) {
                exhausted = true;
                return false;
            }
            json = rs.getString(1);
            keys = new Object[orderCols];
            for (int i = 0; i < orderCols; i++) keys[i] = rs.getObject(4 + i);
            return true;
        }

        void close() {
            try { rs.close(); } catch (SQLException ignored) { }
            try { ps.close(); } catch (SQLException ignored) { }
        }
    }

    @SuppressWarnings("unchecked")
    private static int compareKeys(Object[] a, Object[] b, boolean forward) {
        for (int i = 0; i < a.length; i++) {
            int c;
            if (a[i] instanceof BigDecimal x && b[i] instanceof BigDecimal y) {
                c = x.compareTo(y);
            } else {
                c = PgItemStore.compareBytes(String.valueOf(a[i]), String.valueOf(b[i]));
            }
            if (c != 0) return forward ? c : -c;
        }
        return 0;
    }

    private Result consume(Spec spec, List<Col> order, List<Cursor> cursors) throws SQLException {
        TableSchema s = spec.schema();
        TableSchema.IndexDef idx = spec.index();
        PriorityQueue<Cursor> queue = new PriorityQueue<>((x, y) -> compareKeys(x.keys, y.keys, spec.forward()));
        for (Cursor c : cursors) if (c.advance()) queue.add(c);

        List<Map<String, AttributeValue>> items = new ArrayList<>();
        int matched = 0, evaluated = 0;
        long bytes = 0;
        Map<String, AttributeValue> lastEvaluated = null;
        boolean stoppedEarly = false;
        while (!queue.isEmpty()) {
            Cursor c = queue.poll();
            String json = c.json;
            if (c.advance()) queue.add(c);
            Map<String, AttributeValue> item = PgItemStore.jsonToItem(JsonParser.parseString(json).getAsJsonObject());
            evaluated++;
            bytes += AttributeValue.itemSize(item);
            lastEvaluated = item;
            Map<String, AttributeValue> view = idx != null && !idx.local() ? projectForIndex(s, idx, item) : item;
            if (spec.filter() == null || ExprEval.test(spec.filter(), view)) {
                matched++;
                if (spec.collectItems()) items.add(view);
            }
            if (spec.limit() != null && evaluated >= spec.limit()) {
                stoppedEarly = true;
                break;
            }
            if (bytes >= PAGE_BYTES) {
                stoppedEarly = true;
                break;
            }
        }
        Map<String, AttributeValue> lek = stoppedEarly ? lastEvaluatedKey(s, idx, lastEvaluated) : null;
        return new Result(items, matched, evaluated, bytes, lek);
    }

    /** The primary key of the item, plus the index key attributes when reading through an index. */
    static Map<String, AttributeValue> lastEvaluatedKey(TableSchema s, TableSchema.IndexDef idx, Map<String, AttributeValue> item) {
        Map<String, AttributeValue> k = new LinkedHashMap<>(ItemValidator.keyOf(s, item));
        if (idx != null) {
            for (TableSchema.KeyAttr a : idx.allKeys()) {
                AttributeValue v = item.get(a.name());
                if (v != null) k.put(a.name(), v);
            }
        }
        return k;
    }

    /** What a global secondary index physically holds of an item (per its projection). */
    static Map<String, AttributeValue> projectForIndex(TableSchema s, TableSchema.IndexDef idx, Map<String, AttributeValue> item) {
        if ("ALL".equals(idx.projectionType())) return item;
        Map<String, AttributeValue> out = new LinkedHashMap<>();
        out.put(s.partitionKeyName(), item.get(s.partitionKeyName()));
        if (s.hasSortKey()) out.put(s.sortKeyName(), item.get(s.sortKeyName()));
        for (TableSchema.KeyAttr a : idx.allKeys()) {
            AttributeValue v = item.get(a.name());
            if (v != null) out.put(a.name(), v);
        }
        if ("INCLUDE".equals(idx.projectionType())) {
            for (String n : idx.nonKeyAttributes()) {
                AttributeValue v = item.get(n);
                if (v != null) out.put(n, v);
            }
        }
        return out;
    }
}

package com.sayonora.wire.azurewire;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.wire.azurewire.ODataFilter.Val;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Table storage: the table catalog on the home host; an entity lives on the shard owning
 * hash(account/table/PartitionKey), so every partition (and therefore every entity group transaction) is one Postgres
 * transaction on one host. Queries without a PartitionKey equality scatter-gather over all shards and merge on
 * (PartitionKey, RowKey); the continuation token is simply the last (PartitionKey, RowKey) returned.
 */
final class TableStore {

    final AzShards shards;

    TableStore(AzShards shards) {
        this.shards = shards;
    }

    static final class Ent {
        String pk;
        String rk;
        String ts;
        final Map<String, Val> props = new LinkedHashMap<>();

        String etag() {
            return "W/\"datetime'" + java.net.URLEncoder.encode(ts, java.nio.charset.StandardCharsets.UTF_8) + "'\"";
        }
    }

    private static final java.util.concurrent.atomic.AtomicLong LAST = new java.util.concurrent.atomic.AtomicLong();

    /** Unique, increasing 100ns-resolution timestamp. */
    static String newTs() {
        Instant n = Instant.now();
        long ticks = n.getEpochSecond() * 10_000_000L + n.getNano() / 100;
        long v = LAST.updateAndGet(p -> Math.max(p + 1, ticks));
        return ODataFilter.tableTime(Instant.ofEpochSecond(v / 10_000_000L, (v % 10_000_000L) * 100));
    }

    String ownerHost(String account, String table, String pk) {
        return shards.owner(account + "/" + table + "/" + pk);
    }

    // ---- JSON <-> props

    static String propsJson(Map<String, Val> props) {
        JsonObject o = new JsonObject();
        props.forEach((k, v) -> {
            JsonObject p = new JsonObject();
            p.addProperty("t", v.type());
            switch (v.type()) {
                case "Edm.Int32" -> p.addProperty("v", (Integer) v.v());
                case "Edm.Double" -> {
                    double d = (Double) v.v();
                    if (Double.isNaN(d) || Double.isInfinite(d)) {
                        p.addProperty("v", d != d ? "NaN" : d > 0 ? "Infinity" : "-Infinity");
                    } else {
                        p.addProperty("v", d);
                    }
                }
                case "Edm.Boolean" -> p.addProperty("v", (Boolean) v.v());
                case "Edm.Int64" -> p.addProperty("v", String.valueOf(v.v()));
                case "Edm.Binary" -> p.addProperty("v", Base64.getEncoder().encodeToString((byte[]) v.v()));
                default -> p.addProperty("v", String.valueOf(v.v()));
            }
            o.add(k, p);
        });
        return o.toString();
    }

    static Map<String, Val> readProps(String json) {
        Map<String, Val> out = new LinkedHashMap<>();
        JsonObject o = JsonParser.parseString(json).getAsJsonObject();
        for (Map.Entry<String, JsonElement> e : o.entrySet()) {
            JsonObject p = e.getValue().getAsJsonObject();
            String t = p.get("t").getAsString();
            JsonElement v = p.get("v");
            Object val = switch (t) {
                case "Edm.Int32" -> v.getAsInt();
                case "Edm.Double" -> v.getAsJsonPrimitive().isString() ? Double.valueOf(v.getAsString().replace("Infinity", "Infinity"))
                        : (Object) v.getAsDouble();
                case "Edm.Boolean" -> v.getAsBoolean();
                case "Edm.Int64" -> Long.parseLong(v.getAsString());
                case "Edm.Binary" -> Base64.getDecoder().decode(v.getAsString());
                default -> v.getAsString();
            };
            out.put(e.getKey(), new Val(t, val));
        }
        return out;
    }

    static Ent read(ResultSet rs) throws SQLException {
        Ent e = new Ent();
        e.pk = rs.getString(1);
        e.rk = rs.getString(2);
        e.ts = rs.getString(3);
        e.props.putAll(readProps(rs.getString(4)));
        return e;
    }

    // ---- catalog

    boolean tableExists(String account, String table) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT 1 FROM warp_aztable_tables WHERE account=? AND name=?")) {
                ps.setString(1, account);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        });
    }

    boolean createTable(String account, String table) {
        return shards.conn(shards.home(), c -> BlobStore.exec(c, "INSERT INTO warp_aztable_tables (account, name) VALUES (?,?) "
                + "ON CONFLICT DO NOTHING", account, table) > 0);
    }

    boolean deleteTable(String account, String table) {
        boolean existed = shards.conn(shards.home(), c -> BlobStore.exec(c, "DELETE FROM warp_aztable_tables WHERE account=? "
                + "AND name=?", account, table) > 0);
        if (existed) {
            for (String h : shards.hosts()) {
                shards.conn(h, c -> BlobStore.exec(c, "DELETE FROM warp_aztable_entities WHERE account=? AND tbl=?", account,
                        table));
            }
        }
        return existed;
    }

    List<String> listTables(String account, String after, int limit) {
        return shards.conn(shards.home(), c -> {
            List<String> out = new ArrayList<>();
            try (PreparedStatement ps = c.prepareStatement("SELECT name FROM warp_aztable_tables WHERE account=? AND name >= ? "
                    + "ORDER BY name LIMIT " + limit)) {
                ps.setString(1, account);
                ps.setString(2, after == null ? "" : after);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.add(rs.getString(1));
                    }
                }
            }
            return out;
        });
    }

    List<AzureAuth.Policy> acl(String account, String table) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT acl::text FROM warp_aztable_tables WHERE account=? AND name=?")) {
                ps.setString(1, account);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    List<AzureAuth.Policy> out = new ArrayList<>();
                    for (JsonElement e : JsonParser.parseString(rs.getString(1)).getAsJsonArray()) {
                        JsonObject o = e.getAsJsonObject();
                        out.add(new AzureAuth.Policy(o.get("id").getAsString(), str(o, "start"), str(o, "expiry"), str(o, "permission")));
                    }
                    return out;
                }
            }
        });
    }

    private static String str(JsonObject o, String k) {
        return o.has(k) ? o.get(k).getAsString() : null;
    }

    void setAcl(String account, String table, List<AzureAuth.Policy> acl) {
        shards.conn(shards.home(), c -> BlobStore.exec(c, "UPDATE warp_aztable_tables SET acl=?::jsonb WHERE account=? AND name=?",
                BlobStore.aclJson(acl), account, table));
    }

    String serviceProperties(String account) {
        return shards.conn(shards.home(), c -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT properties::text FROM warp_aztable_service WHERE account=?")) {
                ps.setString(1, account);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getString(1) : "{}";
                }
            }
        });
    }

    void setServiceProperties(String account, String json) {
        shards.conn(shards.home(), c -> BlobStore.exec(c, "INSERT INTO warp_aztable_service (account, properties) VALUES (?, ?::jsonb) "
                + "ON CONFLICT (account) DO UPDATE SET properties = EXCLUDED.properties", account, json));
    }

    // ---- entity operations (connection-level so a batch can run them in one transaction)

    static Ent get(Connection c, String account, String table, String pk, String rk, boolean lock) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT pk, rk, ts, props::text FROM warp_aztable_entities WHERE account=? "
                + "AND tbl=? AND pk=? AND rk=?" + (lock ? " FOR UPDATE" : ""))) {
            ps.setString(1, account);
            ps.setString(2, table);
            ps.setString(3, pk);
            ps.setString(4, rk);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? read(rs) : null;
            }
        }
    }

    static void put(Connection c, String account, String table, Ent e) throws SQLException {
        BlobStore.exec(c, "INSERT INTO warp_aztable_entities (account, tbl, pk, rk, ts, props) VALUES (?,?,?,?,?,?::jsonb) "
                + "ON CONFLICT (account, tbl, pk, rk) DO UPDATE SET ts=EXCLUDED.ts, props=EXCLUDED.props", account, table, e.pk,
                e.rk, e.ts, propsJson(e.props));
    }

    static int remove(Connection c, String account, String table, String pk, String rk) throws SQLException {
        return BlobStore.exec(c, "DELETE FROM warp_aztable_entities WHERE account=? AND tbl=? AND pk=? AND rk=?", account, table,
                pk, rk);
    }

    Ent getEntity(String account, String table, String pk, String rk) {
        return shards.conn(ownerHost(account, table, pk), c -> get(c, account, table, pk, rk, false));
    }

    /** A per-shard cursor over matching rows in (pk, rk) order. */
    private final class Cursor {
        final String host;
        final String account;
        final String table;
        final ODataFilter filter;
        String lastPk;
        String lastRk;
        boolean after;
        boolean inclusiveStart;
        List<Ent> buf = List.of();
        int i;
        boolean done;

        Cursor(String host, String account, String table, ODataFilter filter, String afterPk, String afterRk) {
            this.host = host;
            this.account = account;
            this.table = table;
            this.filter = filter;
            if (afterPk != null) {
                lastPk = afterPk;
                lastRk = afterRk == null ? "" : afterRk;
                after = true;
                inclusiveStart = true;
            }
        }

        Ent peek() {
            while (true) {
                while (i < buf.size()) {
                    Ent e = buf.get(i);
                    if (filter.matches(name -> valOf(e, name))) {
                        return e;
                    }
                    i++;
                }
                if (done) {
                    return null;
                }
                fill();
            }
        }

        void next() {
            i++;
        }

        void fill() {
            StringBuilder sql = new StringBuilder("SELECT pk, rk, ts, props::text FROM warp_aztable_entities WHERE account=? AND tbl=?");
            List<Object> args = new ArrayList<>(List.of(account, table));
            for (ODataFilter.Bound b : filter.bounds()) {
                String col = b.prop().equals("PartitionKey") ? "pk" : "rk";
                String op = switch (b.op()) {
                    case "eq" -> "=";
                    case "gt" -> ">";
                    case "ge" -> ">=";
                    case "lt" -> "<";
                    default -> "<=";
                };
                sql.append(" AND ").append(col).append(' ').append(op).append(" ?");
                args.add(b.value());
            }
            if (after) {
                sql.append(inclusiveStart ? " AND (pk, rk) >= (?, ?)" : " AND (pk, rk) > (?, ?)");
                args.add(lastPk);
                args.add(lastRk);
            }
            sql.append(" ORDER BY pk, rk LIMIT 500");
            List<Ent> rows = shards.conn(host, c -> {
                List<Ent> out = new ArrayList<>();
                try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
                    for (int k = 0; k < args.size(); k++) {
                        ps.setObject(k + 1, args.get(k));
                    }
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.add(read(rs));
                        }
                    }
                }
                return out;
            });
            buf = rows;
            i = 0;
            if (rows.size() < 500) {
                done = true;
            }
            if (!rows.isEmpty()) {
                Ent l = rows.get(rows.size() - 1);
                lastPk = l.pk;
                lastRk = l.rk;
                after = true;
                inclusiveStart = false;
            }
        }
    }

    static Val valOf(Ent e, String name) {
        return switch (name) {
            case "PartitionKey" -> new Val("Edm.String", e.pk);
            case "RowKey" -> new Val("Edm.String", e.rk);
            case "Timestamp" -> new Val("Edm.DateTime", e.ts);
            default -> e.props.get(name);
        };
    }

    record Page(List<Ent> entities, String nextPk, String nextRk) {
    }

    Page query(String account, String table, ODataFilter filter, int top, String afterPk, String afterRk) {
        List<String> hosts = shards.hosts();
        String only = null;
        for (ODataFilter.Bound b : filter.bounds()) {
            if (b.prop().equals("PartitionKey") && b.op().equals("eq")) {
                only = b.value();
            }
        }
        List<Cursor> cursors = new ArrayList<>();
        if (only != null) {
            cursors.add(new Cursor(ownerHost(account, table, only), account, table, filter, afterPk, afterRk));
        } else {
            for (String h : hosts) {
                cursors.add(new Cursor(h, account, table, filter, afterPk, afterRk));
            }
        }
        List<Ent> out = new ArrayList<>();
        while (out.size() < top) {
            Cursor best = null;
            Ent be = null;
            for (Cursor c : cursors) {
                Ent e = c.peek();
                if (e != null && (be == null || e.pk.compareTo(be.pk) < 0 || e.pk.equals(be.pk) && e.rk.compareTo(be.rk) < 0)) {
                    best = c;
                    be = e;
                }
            }
            if (best == null) {
                return new Page(out, null, null);
            }
            best.next();
            out.add(be);
        }
        boolean more = false;
        for (Cursor c : cursors) {
            if (c.peek() != null) {
                more = true;
            }
        }
        if (!more) {
            return new Page(out, null, null);
        }
        Ent nxt = null;
        for (Cursor c : cursors) {
            Ent e = c.peek();
            if (e != null && (nxt == null || e.pk.compareTo(nxt.pk) < 0 || e.pk.equals(nxt.pk) && e.rk.compareTo(nxt.rk) < 0)) {
                nxt = e;
            }
        }
        return new Page(out, nxt.pk, nxt.rk);
    }

    Map<String, Long> counts(String account) {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String h : shards.hosts()) {
            shards.conn(h, c -> {
                try (PreparedStatement ps = c.prepareStatement("SELECT tbl, count(*) FROM warp_aztable_entities WHERE account=? "
                        + "GROUP BY tbl")) {
                    ps.setString(1, account);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            out.merge(rs.getString(1), rs.getLong(2), Long::sum);
                        }
                    }
                }
                return null;
            });
        }
        return out;
    }
}

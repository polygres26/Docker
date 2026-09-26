package com.sayonora.wire.bigtablewire;

import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import com.sayonora.wire.bigtablewire.admin.v2.CheckConsistencyRequest;
import com.sayonora.wire.bigtablewire.admin.v2.CheckConsistencyResponse;
import com.sayonora.wire.bigtablewire.admin.v2.ColumnFamily;
import com.sayonora.wire.bigtablewire.admin.v2.CreateTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.DeleteTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.DropRowRangeRequest;
import com.sayonora.wire.bigtablewire.admin.v2.GcRule;
import com.sayonora.wire.bigtablewire.admin.v2.GenerateConsistencyTokenRequest;
import com.sayonora.wire.bigtablewire.admin.v2.GenerateConsistencyTokenResponse;
import com.sayonora.wire.bigtablewire.admin.v2.GetTableRequest;
import com.sayonora.wire.bigtablewire.admin.v2.ListTablesRequest;
import com.sayonora.wire.bigtablewire.admin.v2.ListTablesResponse;
import com.sayonora.wire.bigtablewire.admin.v2.ModifyColumnFamiliesRequest;
import com.sayonora.wire.bigtablewire.admin.v2.Table;
import com.sayonora.wire.bigtablewire.admin.v2.UpdateTableMetadata;
import com.sayonora.wire.bigtablewire.admin.v2.UpdateTableRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The table admin API and the table catalog every data call consults. The catalog row (a serialized admin {@code Table}: column
 * families and their GC rules) lives on the first host of the project's hosts; a per-process cache keeps a table's schema for a
 * short while (dropped on this process's own DDL) so a data call does not pay a catalog round trip. Cells of dropped families and
 * deleted tables are removed from every host. {@link #gcSweep} applies the families' GC rules.
 */
final class BtAdmin {

    private static final Logger log = LoggerFactory.getLogger(BtAdmin.class);
    private static final Pattern PARENT = Pattern.compile("projects/[^/]+/instances/[^/]+");
    private static final Pattern ID = Pattern.compile("[_a-zA-Z0-9][-_.a-zA-Z0-9]{0,63}");
    private static final long CACHE_NANOS = 2_000_000_000L;

    private record Cached(Table table, long at) {
    }

    private final BtStore store;
    private final BtShards shards;
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    BtAdmin(BtStore store) {
        this.store = store;
        this.shards = store.shards();
    }

    // ------------------------------------------------------------------ catalog

    /** The table's schema, or NOT_FOUND {@code table "<name>" not found}. */
    Table table(String name) {
        Cached c = cache.get(name);
        long now = System.nanoTime();
        if (c != null && now - c.at < CACHE_NANOS) {
            return c.table;
        }
        byte[] spec = store.tableSpec(shards.home(BtShards.project(name)), name);
        if (spec == null) {
            cache.remove(name);
            throw BtException.notFoundTable(name);
        }
        Table t = parse(spec);
        cache.put(name, new Cached(t, now));
        return t;
    }

    List<String> hostsOf(String tableName) {
        return shards.hosts(BtShards.project(tableName));
    }

    private static Table parse(byte[] spec) {
        try {
            return Table.parseFrom(spec);
        } catch (InvalidProtocolBufferException e) {
            throw BtException.unknown("corrupt table metadata: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------------ RPCs

    Table createTable(CreateTableRequest req) {
        String parent = req.getParent();
        String id = req.getTableId();
        if (!PARENT.matcher(parent).matches()) {
            throw BtException.invalid("Error in field 'parent' : Invalid instance name, expected projects/<project>/instances/<instance>: " + parent);
        }
        if (!ID.matcher(id).matches()) {
            throw BtException.invalid("Error in field 'table_id' : table_id must match [_a-zA-Z0-9][-_.a-zA-Z0-9]* (at most 64 characters): '" + id + "'");
        }
        String name = parent + "/tables/" + id;
        Table.Builder b = Table.newBuilder().setName(name).setGranularity(Table.TimestampGranularity.MILLIS);
        for (Map.Entry<String, ColumnFamily> e : req.getTable().getColumnFamiliesMap().entrySet()) {
            checkFamilyName(e.getKey());
            checkFamily(e.getValue());
            b.putColumnFamilies(e.getKey(), e.getValue());
        }
        if (req.getTable().getDeletionProtection()) {
            b.setDeletionProtection(true);
        }
        Table t = b.build();
        if (!store.insertTable(shards.home(BtShards.project(name)), name, parent, t.toByteArray())) {
            throw new BtException(io.grpc.Status.Code.ALREADY_EXISTS, "table \"" + name + "\" already exists");
        }
        cache.remove(name);
        return t;
    }

    Table getTable(GetTableRequest req) {
        return view(table(req.getName()), req.getView(), true);
    }

    ListTablesResponse listTables(ListTablesRequest req) {
        String parent = req.getParent();
        int size = req.getPageSize() > 0 ? req.getPageSize() : 100_000;
        String after = req.getPageToken().isEmpty() ? null : new String(Base64.getUrlDecoder().decode(req.getPageToken()), StandardCharsets.UTF_8);
        List<byte[][]> rows = store.listTables(shards.home(BtShards.project(parent)), parent, after, size + 1);
        ListTablesResponse.Builder out = ListTablesResponse.newBuilder();
        Table.View v = req.getView() == Table.View.VIEW_UNSPECIFIED ? Table.View.NAME_ONLY : req.getView();
        for (int i = 0; i < rows.size() && i < size; i++) {
            out.addTables(view(parse(rows.get(i)[1]), v, false));
        }
        if (rows.size() > size) {
            String last = new String(rows.get(size - 1)[0], StandardCharsets.UTF_8);
            out.setNextPageToken(Base64.getUrlEncoder().withoutPadding().encodeToString(last.getBytes(StandardCharsets.UTF_8)));
        }
        return out.build();
    }

    private static Table view(Table t, Table.View v, boolean getDefaultsToSchema) {
        if (v == Table.View.NAME_ONLY) {
            return Table.newBuilder().setName(t.getName()).build();
        }
        return t;
    }

    Table modifyColumnFamilies(ModifyColumnFamiliesRequest req) {
        String name = req.getName();
        table(name); // NOT_FOUND
        java.util.Set<String> dropped = new java.util.LinkedHashSet<>();
        byte[] next = store.updateSpec(shards.home(BtShards.project(name)), name, cur -> {
            Table.Builder b = parse(cur).toBuilder();
            dropped.clear();
            for (ModifyColumnFamiliesRequest.Modification m : req.getModificationsList()) {
                String id = m.getId();
                switch (m.getModCase()) {
                    case CREATE:
                        checkFamilyName(id);
                        checkFamily(m.getCreate());
                        if (b.containsColumnFamilies(id)) {
                            throw new BtException(io.grpc.Status.Code.ALREADY_EXISTS, "family \"" + id + "\" already exists");
                        }
                        b.putColumnFamilies(id, m.getCreate());
                        dropped.remove(id);
                        break;
                    case UPDATE: {
                        if (!b.containsColumnFamilies(id)) {
                            throw BtException.unknown("no such family \"" + id + "\"");
                        }
                        checkFamily(m.getUpdate());
                        List<String> paths = m.getUpdateMask().getPathsList();
                        if (paths.isEmpty() || paths.contains("gc_rule")) {
                            ColumnFamily.Builder nb = b.getColumnFamiliesOrThrow(id).toBuilder();
                            if (m.getUpdate().hasGcRule()) {
                                nb.setGcRule(m.getUpdate().getGcRule());
                            } else {
                                nb.clearGcRule();
                            }
                            b.putColumnFamilies(id, nb.build());
                        }
                        break;
                    }
                    case DROP:
                        if (!b.containsColumnFamilies(id)) {
                            throw BtException.unknown("can't delete unknown family \"" + id + "\"");
                        }
                        b.removeColumnFamilies(id);
                        dropped.add(id);
                        break;
                    default:
                        throw BtException.unknown("unknown modification type " + m.getModCase());
                }
            }
            return b.build().toByteArray();
        });
        cache.remove(name);
        if (next == null) {
            throw BtException.notFoundTable(name);
        }
        for (String fam : dropped) {
            for (String host : shards.hosts(BtShards.project(name))) {
                store.deleteFamilyCells(host, name, fam);
            }
        }
        return parse(next);
    }

    Empty deleteTable(DeleteTableRequest req) {
        String name = req.getName();
        Table t = table(name);
        if (t.getDeletionProtection()) {
            throw BtException.precondition("table \"" + name + "\" is protected from deletion");
        }
        if (!store.deleteTable(shards.home(BtShards.project(name)), name)) {
            throw BtException.notFoundTable(name);
        }
        cache.remove(name);
        for (String host : shards.hosts(BtShards.project(name))) {
            store.deleteTableCells(host, name);
        }
        return Empty.getDefaultInstance();
    }

    Empty dropRowRange(DropRowRangeRequest req) {
        String name = req.getName();
        table(name);
        byte[] start;
        byte[] end;
        switch (req.getTargetCase()) {
            case ROW_KEY_PREFIX:
                start = req.getRowKeyPrefix().toByteArray();
                if (start.length == 0) {
                    throw BtException.unknown("missing row key prefix");
                }
                end = successor(start);
                break;
            case DELETE_ALL_DATA_FROM_TABLE:
                start = null;
                end = null;
                break;
            default:
                throw BtException.unknown("missing row key prefix");
        }
        for (String host : shards.hosts(BtShards.project(name))) {
            store.dropRange(host, name, start, end);
        }
        return Empty.getDefaultInstance();
    }

    /** The smallest key greater than every key with this prefix, or null when there is none (prefix of 0xFF bytes). */
    static byte[] successor(byte[] prefix) {
        int n = prefix.length;
        while (n > 0 && (prefix[n - 1] & 0xff) == 0xff) {
            n--;
        }
        if (n == 0) {
            return null;
        }
        byte[] out = java.util.Arrays.copyOf(prefix, n);
        out[n - 1]++;
        return out;
    }

    GenerateConsistencyTokenResponse generateConsistencyToken(GenerateConsistencyTokenRequest req) {
        table(req.getName());
        return GenerateConsistencyTokenResponse.newBuilder().setConsistencyToken("TokenFor-" + req.getName()).build();
    }

    CheckConsistencyResponse checkConsistency(CheckConsistencyRequest req) {
        table(req.getName());
        if (!("TokenFor-" + req.getName()).equals(req.getConsistencyToken())) {
            throw BtException.invalid("token \"" + req.getConsistencyToken() + "\" not valid");
        }
        return CheckConsistencyResponse.newBuilder().setConsistent(true).build();
    }

    /** UpdateTable: only deletion_protection is updatable; the operation is complete on return. */
    Operation updateTable(UpdateTableRequest req) {
        String name = req.getTable().getName();
        table(name);
        List<String> paths = req.getUpdateMask().getPathsList();
        if (paths.isEmpty()) {
            throw BtException.invalid("Error in field 'update_mask' : at least one path is required");
        }
        for (String p : paths) {
            if (!p.equals("deletion_protection")) {
                throw BtException.unimplemented("UpdateTable: field '" + p + "' can't be updated");
            }
        }
        byte[] next = store.updateSpec(shards.home(BtShards.project(name)), name,
                cur -> parse(cur).toBuilder().setDeletionProtection(req.getTable().getDeletionProtection()).build().toByteArray());
        cache.remove(name);
        Table t = parse(next);
        UpdateTableMetadata md = UpdateTableMetadata.newBuilder().setName(name).setStartTime(Timestamps.fromMillis(System.currentTimeMillis()))
                .setEndTime(Timestamps.fromMillis(System.currentTimeMillis())).build();
        return Operation.newBuilder().setName(name + "/operations/" + UUID.randomUUID()).setDone(true)
                .setMetadata(Any.pack(md)).setResponse(Any.pack(t)).build();
    }

    // ------------------------------------------------------------------ validation

    private static void checkFamilyName(String id) {
        if (!ID.matcher(id).matches()) {
            throw BtException.invalid("Error in field 'column_families' : column family name must match [_a-zA-Z0-9][-_.a-zA-Z0-9]* (at most 64 characters): '" + id + "'");
        }
    }

    private static void checkFamily(ColumnFamily f) {
        if (f.hasValueType()) {
            throw BtException.unimplemented("column family value types (aggregate columns) are not supported");
        }
    }

    // ------------------------------------------------------------------ GC

    /** Applies every table's column family GC rules on every host; returns the number of cells removed. */
    int gcSweep() {
        int removed = 0;
        for (String home : shards.allHosts()) {
            List<byte[][]> tables;
            try {
                tables = store.listAllTables(home);
            } catch (RuntimeException e) {
                log.debug("bigtablewire gc: listing tables on {} failed: {}", home, e.getMessage());
                continue;
            }
            long now = System.currentTimeMillis() * 1000;
            for (byte[][] row : tables) {
                String name = new String(row[0], StandardCharsets.UTF_8);
                Table t = parse(row[1]);
                for (Map.Entry<String, ColumnFamily> e : t.getColumnFamiliesMap().entrySet()) {
                    GcRule r = e.getValue().getGcRule();
                    if (r.getRuleCase() == GcRule.RuleCase.RULE_NOT_SET) {
                        continue;
                    }
                    for (String host : shards.hosts(BtShards.project(name))) {
                        try {
                            removed += store.gc(host, name, e.getKey(), r, now);
                        } catch (RuntimeException ex) {
                            log.debug("bigtablewire gc of {} failed on {}: {}", name, host, ex.getMessage());
                        }
                    }
                }
            }
        }
        return removed;
    }
}

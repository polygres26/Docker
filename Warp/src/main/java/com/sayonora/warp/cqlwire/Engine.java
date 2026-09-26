package com.sayonora.warp.cqlwire;

import com.sayonora.warp.cqlwire.Ast.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Executes parsed statements against the catalog and the sharded cell store. */
final class Engine {

    final CqlShards shards;
    final CqlStore store;
    final Catalog catalog;
    final Ddl ddl;
    final Dml dml;
    final Select select;
    private final CopyOnWriteArrayList<Consumer<Result.SchemaEvent>> listeners = new CopyOnWriteArrayList<>();

    Engine(CqlShards shards, long schemaTtlMs) {
        this.shards = shards;
        this.store = new CqlStore(shards);
        this.catalog = new Catalog(store, schemaTtlMs);
        this.ddl = new Ddl(this);
        this.dml = new Dml(this);
        this.select = new Select(this);
    }

    void addListener(Consumer<Result.SchemaEvent> l) {
        listeners.add(l);
    }

    void removeListener(Consumer<Result.SchemaEvent> l) {
        listeners.remove(l);
    }

    void publish(Result.SchemaEvent ev) {
        for (Consumer<Result.SchemaEvent> l : listeners) {
            try {
                l.accept(ev);
            } catch (RuntimeException ignored) {
                // a closed session
            }
        }
    }

    <T> T timed(String host, String op, Supplier<T> fn) {
        return fn.get();
    }

    void timed(String host, String op, Runnable fn) {
        fn.run();
    }

    static boolean isWrite(Stmt s) {
        return s instanceof Insert || s instanceof Update || s instanceof Delete || s instanceof Batch || s instanceof CreateKeyspace
                || s instanceof AlterKeyspace || s instanceof DropKeyspace || s instanceof CreateTable || s instanceof AlterTable
                || s instanceof DropTable || s instanceof Truncate || s instanceof CreateIndex || s instanceof DropIndex
                || s instanceof CreateType || s instanceof AlterType || s instanceof DropType;
    }

    static String opName(Stmt s) {
        return s.getClass().getSimpleName().toUpperCase(Locale.ROOT);
    }

    Result execute(Stmt st, Eval.Binds b, QueryOpts o, ClientState cs) {
        if (st instanceof Ast.Select s) {
            return select.select(s, b, cs, o);
        }
        if (st instanceof Insert i) {
            return dml.insertStmt(i, b, cs, o);
        }
        if (st instanceof Update u) {
            return dml.updateStmt(u, b, cs, o);
        }
        if (st instanceof Delete d) {
            return dml.deleteStmt(d, b, cs, o);
        }
        if (st instanceof Batch bt) {
            return dml.batch(bt, b, cs, o);
        }
        if (st instanceof Use u) {
            Schema.Keyspace k = catalog.schema().ks(u.ks());
            if (k == null) {
                throw CqlError.invalid("Keyspace '" + u.ks() + "' does not exist");
            }
            cs.keyspace = u.ks();
            return Result.setKeyspace(u.ks());
        }
        if (st instanceof CreateKeyspace c) {
            return ddl.createKeyspace(c);
        }
        if (st instanceof AlterKeyspace c) {
            return ddl.alterKeyspace(c);
        }
        if (st instanceof DropKeyspace c) {
            Result r = ddl.dropKeyspace(c);
            if (c.name().equals(cs.keyspace)) {
                cs.keyspace = cs.keyspace;
            }
            return r;
        }
        if (st instanceof CreateTable c) {
            return ddl.createTable(c, cs);
        }
        if (st instanceof AlterTable c) {
            return ddl.alterTable(c, cs);
        }
        if (st instanceof DropTable c) {
            return ddl.dropTable(c, cs);
        }
        if (st instanceof Truncate c) {
            return ddl.truncate(c, cs);
        }
        if (st instanceof CreateIndex c) {
            return ddl.createIndex(c, cs);
        }
        if (st instanceof DropIndex c) {
            return ddl.dropIndex(c, cs);
        }
        if (st instanceof CreateType c) {
            return ddl.createType(c, cs);
        }
        if (st instanceof AlterType c) {
            return ddl.alterType(c, cs);
        }
        if (st instanceof DropType c) {
            return ddl.dropType(c, cs);
        }
        if (st instanceof Describe d) {
            return new DescribeStmt(this).describe(d, cs);
        }
        if (st instanceof Other oth) {
            switch (oth.kind()) {
                case "grant", "revoke", "list" -> {
                    if ("anonymous".equals(cs.user)) {
                        throw CqlError.unauthorized("You have to be logged in and not anonymous to perform this request");
                    }
                    throw CqlError.invalid("Permission management is not supported by Warp's cqlwire");
                }
                case "create materialized", "alter materialized", "drop materialized" ->
                    throw CqlError.invalid("Materialized views are disabled. Enable in cassandra.yaml to use.");
                case "create function", "create aggregate", "drop function", "drop aggregate" ->
                    throw CqlError.invalid("User-defined functions are disabled in cassandra.yaml - set user_defined_functions_enabled=true to enable");
                default -> throw CqlError.invalid("Role management is not supported by Warp's cqlwire");
            }
        }
        return Result.VOID;
    }

    // ------------------------------------------------------------------ prepare

    /** The description of a prepared statement. */
    static final class Prepared {
        String id;
        String query;
        String keyspace;
        Stmt stmt;
        List<Result.ColSpec> binds = new ArrayList<>();
        List<Result.ColSpec> results = List.of();
        int[] pkIndexes = new int[0];
        boolean isSelectStar;
        byte[] idBytes;

        byte[] idBytes() {
            return idBytes;
        }
    }

    private static final class Binder {
        final Schema.Table table;
        final Result.ColSpec[] specs;
        final Map<String, Integer> eqBinds = new LinkedHashMap<>();

        Binder(Schema.Table t, int n) {
            this.table = t;
            this.specs = new Result.ColSpec[n];
        }

        void put(Bind b, CqlType type, String name) {
            if (b.index() < specs.length) {
                specs[b.index()] = new Result.ColSpec(table == null ? "" : table.ks, table == null ? "" : table.name, b.name() != null ? b.name() : name,
                        type);
            }
        }

        void term(Term t, CqlType type, String name) {
            if (t == null || type == null) {
                return;
            }
            if (t instanceof Bind b) {
                put(b, type, name);
            } else if (t instanceof ListLit l) {
                for (Term x : l.items()) {
                    term(x, type.args.isEmpty() ? null : type.args.get(0), "[" + type.k.cql + "_value]");
                }
            } else if (t instanceof BraceLit br) {
                if (type.k == CqlType.K.MAP) {
                    for (Term[] p : br.pairs()) {
                        term(p[0], type.args.get(0), "[map_key]");
                        term(p[1], type.args.get(1), "[map_value]");
                    }
                } else if (type.k == CqlType.K.SET) {
                    for (Term x : br.items()) {
                        term(x, type.args.get(0), "[set_value]");
                    }
                } else if (type.k == CqlType.K.UDT) {
                    br.fields().forEach((f, x) -> {
                        int i = type.fieldNames.indexOf(f);
                        if (i >= 0) {
                            term(x, type.args.get(i), f);
                        }
                    });
                }
            } else if (t instanceof TupleLit tl && type.k == CqlType.K.TUPLE) {
                for (int i = 0; i < tl.items().size() && i < type.args.size(); i++) {
                    term(tl.items().get(i), type.args.get(i), "[tuple_component]");
                }
            } else if (t instanceof TypeHint th) {
                term(th.t(), CqlType.natives().get(th.type()), name);
            } else if (t instanceof Func f) {
                if (f.name().equalsIgnoreCase("fromjson")) {
                    for (Term x : f.args()) {
                        term(x, CqlType.TEXT, "[json]");
                    }
                } else if (f.name().equalsIgnoreCase("mintimeuuid") || f.name().equalsIgnoreCase("maxtimeuuid")) {
                    for (Term x : f.args()) {
                        term(x, CqlType.TIMESTAMP, name);
                    }
                }
            } else if (t instanceof Arith a) {
                term(a.l(), type, name);
                term(a.r(), type, name);
            }
        }

        void relations(List<Relation> rels) {
            for (Relation r : rels) {
                if (r.token()) {
                    term(r.rhs(), CqlType.BIGINT, "partition key token");
                    continue;
                }
                if (r.multi()) {
                    List<CqlType> ts = new ArrayList<>();
                    for (String n : r.cols()) {
                        Schema.Column c = table.col(n);
                        if (c != null) {
                            ts.add(c.type);
                        }
                    }
                    CqlType tt = CqlType.tuple(ts, false);
                    String nm = "(" + String.join(",", r.cols()) + ")";
                    if (r.op().equals("in")) {
                        if (r.inList() != null) {
                            r.inList().forEach(x -> term(x, tt, nm));
                        } else {
                            term(r.rhs(), CqlType.list(tt, false), "in(" + nm + ")");
                        }
                    } else {
                        term(r.rhs(), tt, nm);
                    }
                    continue;
                }
                Schema.Column c = table.col(r.cols().get(0));
                if (c == null) {
                    continue;
                }
                CqlType ct = c.type;
                if (r.index() != null) {
                    term(r.index(), c.type.k == CqlType.K.MAP ? c.type.args.get(0) : CqlType.INT, "[" + c.name + "_key]");
                    ct = c.type.k == CqlType.K.MAP ? c.type.args.get(1) : c.type.args.get(0);
                }
                switch (r.op()) {
                    case "in" -> {
                        if (r.inList() != null) {
                            final CqlType cc = ct;
                            r.inList().forEach(x -> term(x, cc, c.name));
                        } else {
                            term(r.rhs(), CqlType.list(ct, false), "in(" + c.name + ")");
                        }
                    }
                    case "contains" -> term(r.rhs(), c.type.k == CqlType.K.MAP ? c.type.args.get(1) : c.type.args.get(0), c.name);
                    case "contains key" -> term(r.rhs(), c.type.args.get(0), c.name);
                    default -> {
                        term(r.rhs(), ct, c.name);
                        if (r.op().equals("=") && r.rhs() instanceof Bind bd && c.kind == Schema.Kind.PARTITION_KEY) {
                            eqBinds.put(c.name, bd.index());
                        }
                    }
                }
            }
        }

        void conds(List<Cond> conds) {
            for (Cond c : conds) {
                Schema.Column col = table.col(c.col());
                if (col == null) {
                    continue;
                }
                CqlType ct = col.type;
                if (c.index() != null) {
                    term(c.index(), col.type.k == CqlType.K.MAP ? col.type.args.get(0) : CqlType.INT, "[" + col.name + "_key]");
                    ct = col.type.k == CqlType.K.MAP ? col.type.args.get(1) : col.type.args.get(0);
                } else if (c.field() != null && col.type.k == CqlType.K.UDT) {
                    int i = col.type.fieldNames.indexOf(c.field());
                    ct = i >= 0 ? col.type.args.get(i) : ct;
                }
                if (c.op().equals("in")) {
                    if (c.inList() != null) {
                        final CqlType cc = ct;
                        c.inList().forEach(x -> term(x, cc, col.name));
                    } else {
                        term(c.rhs(), CqlType.list(ct, false), "in(" + col.name + ")");
                    }
                } else {
                    term(c.rhs(), ct, col.name);
                }
            }
        }
    }

    Prepared prepare(String query, ClientState cs) {
        Parser.Parsed parsed = Parser.parseFull(query);
        Prepared p = new Prepared();
        p.query = query;
        p.stmt = parsed.stmt();
        p.keyspace = cs.keyspace;
        int n = parsed.binds();
        Binder bd = null;
        Stmt st = parsed.stmt();
        List<Stmt> flat = st instanceof Batch b ? b.stmts() : List.of(st);
        Binder[] holder = new Binder[1];
        Result.ColSpec[] specs = new Result.ColSpec[n];
        for (Stmt s : flat) {
            Schema.Table t = null;
            String ks = null, tn = null;
            if (s instanceof Insert i) {
                ks = i.ks();
                tn = i.table();
            } else if (s instanceof Update u) {
                ks = u.ks();
                tn = u.table();
            } else if (s instanceof Delete d) {
                ks = d.ks();
                tn = d.table();
            } else if (s instanceof Ast.Select q) {
                ks = q.ks();
                tn = q.table();
            }
            if (tn != null) {
                t = dml.table(ks, tn, cs, false);
            }
            bd = new Binder(t, n);
            if (s instanceof Insert i) {
                for (int k = 0; k < i.cols().size(); k++) {
                    Schema.Column c = t.col(i.cols().get(k));
                    if (c == null) {
                        throw CqlError.invalid("Undefined column name " + i.cols().get(k));
                    }
                    bd.term(i.values().get(k), c.type, c.name);
                    if (c.kind == Schema.Kind.PARTITION_KEY && i.values().get(k) instanceof Bind b2) {
                        bd.eqBinds.put(c.name, b2.index());
                    }
                }
                if (i.json() != null && !i.values().isEmpty()) {
                    bd.term(i.values().get(0), CqlType.TEXT, "[json]");
                }
                bd.term(i.ttl(), CqlType.INT, "[ttl]");
                bd.term(i.ts(), CqlType.BIGINT, "[timestamp]");
            } else if (s instanceof Update u) {
                for (Assign a : u.assigns()) {
                    Schema.Column c = t.col(a.col());
                    if (c == null) {
                        continue;
                    }
                    CqlType vt = c.type;
                    if (a.index() != null) {
                        bd.term(a.index(), c.type.k == CqlType.K.MAP ? c.type.args.get(0) : CqlType.INT,
                                c.type.k == CqlType.K.MAP ? "key(" + c.name + ")" : "idx(" + c.name + ")");
                        vt = c.type.k == CqlType.K.MAP ? c.type.args.get(1) : c.type.args.get(0);
                    } else if (c.type.isCounter()) {
                        vt = CqlType.BIGINT;
                    } else if (a.arith() == '-' && c.type.isCollection()) {
                        vt = c.type.k == CqlType.K.MAP ? CqlType.set(c.type.args.get(0), false) : c.type.k == CqlType.K.SET ? c.type : CqlType.list(c.type.args.get(0), false);
                    }
                    bd.term(a.value(), vt, c.name);
                }
                bd.relations(u.where());
                bd.conds(u.conds());
                bd.term(u.ttl(), CqlType.INT, "[ttl]");
                bd.term(u.ts(), CqlType.BIGINT, "[timestamp]");
            } else if (s instanceof Delete d) {
                for (DelTarget tg : d.targets()) {
                    Schema.Column c = t.col(tg.col());
                    if (c != null && tg.index() != null) {
                        bd.term(tg.index(), c.type.k == CqlType.K.MAP ? c.type.args.get(0) : CqlType.INT, "[" + c.name + "_key]");
                    }
                }
                bd.relations(d.where());
                bd.conds(d.conds());
                bd.term(d.ts(), CqlType.BIGINT, "[timestamp]");
            } else if (s instanceof Ast.Select q) {
                bd.relations(q.where());
                bd.term(q.limit(), CqlType.INT, "[limit]");
                bd.term(q.perPartitionLimit(), CqlType.INT, "[per_partition_limit]");
                for (Selector sel : q.selectors()) {
                    if (sel.star()) {
                        p.isSelectStar = true;
                    }
                }
            }
            for (int k = 0; k < n; k++) {
                if (bd.specs[k] != null && specs[k] == null) {
                    specs[k] = bd.specs[k];
                }
            }
            if (s instanceof Ast.Select q) {
                p.results = select.resultSpecs(q, cs, Eval.Binds.NONE);
                // partition key indexes for token aware routing
                if (t != null) {
                    int[] idx = new int[t.partition.size()];
                    boolean all = true;
                    for (int k = 0; k < idx.length; k++) {
                        Integer ix = bd.eqBinds.get(t.partition.get(k).name);
                        if (ix == null) {
                            all = false;
                            break;
                        }
                        idx[k] = ix;
                    }
                    p.pkIndexes = all ? idx : new int[0];
                }
            } else if (flat.size() == 1 && t != null) {
                int[] idx = new int[t.partition.size()];
                boolean all = true;
                for (int k = 0; k < idx.length; k++) {
                    Integer ix = bd.eqBinds.get(t.partition.get(k).name);
                    if (ix == null) {
                        all = false;
                        break;
                    }
                    idx[k] = ix;
                }
                p.pkIndexes = all ? idx : new int[0];
                boolean lwt = s instanceof Insert i2 && i2.ine() || s instanceof Update u2 && (u2.ifExists() || !u2.conds().isEmpty())
                        || s instanceof Delete d2 && (d2.ifExists() || !d2.conds().isEmpty());
                if (lwt) {
                    p.results = List.of(new Result.ColSpec(t.ks, t.name, "[applied]", CqlType.BOOLEAN));
                }
            }
        }
        for (int k = 0; k < n; k++) {
            if (specs[k] == null) {
                // unresolved marker (e.g. an argument of a function): typed as text/blob-compatible
                specs[k] = new Result.ColSpec("", "", "[value]", CqlType.BLOB);
            }
            p.binds.add(specs[k]);
        }
        if (bd != null && bd.table != null) {
            p.keyspace = bd.table.ks;
        }
        return p;
    }
}

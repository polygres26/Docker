package com.sayonora.warp.core;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Whether the shards of the declaratively sharded tables can take part in two-phase commit, found out by reading each shard's own
 * configuration rather than by waiting for the first multi-shard write to fail. Without it a cluster whose Postgres has
 * {@code max_prepared_transactions=0} (the default) looks healthy and silently writes with commit-last, which can leave some shards
 * committed and others not.
 *
 * <p>Read-only probes, one per engine: Postgres reads {@code max_prepared_transactions}; SQL Server looks for the JDBC XA extended
 * procedures in {@code master}; Oracle reads {@code DBA_PENDING_TRANSACTIONS} (the privilege XA recovery needs); MySQL/MariaDB checks
 * InnoDB is present (XA needs it; the {@code XA RECOVER} privilege is not checked). A probe that cannot run is {@code UNKNOWN}, never
 * {@code READY}: the check only claims what it saw.
 */
public final class TwoPhaseReadiness {

    private static final Logger log = LoggerFactory.getLogger(TwoPhaseReadiness.class);

    public enum State { READY, NOT_READY, UNKNOWN, UNSUPPORTED }

    public record Report(String backend, String engine, State state, String detail, String fix, java.util.List<String> tables) {
        public JsonObject toJson() {
            JsonObject o = new JsonObject();
            o.addProperty("backend", backend);
            o.addProperty("engine", engine);
            o.addProperty("state", state.name());
            o.addProperty("detail", detail);
            if (fix != null) {
                o.addProperty("fix", fix);
            }
            JsonArray t = new JsonArray();
            tables.forEach(t::add);
            o.add("tables", t);
            return o;
        }
    }

    private final BackendRegistry registry;
    private final java.util.function.Supplier<List<RouterStage.TableShardRule>> rules;
    private final Function<BackendTarget, Probe> probes;

    /** What a probe saw on one backend. */
    public record Probe(State state, String detail, String fix) {
    }

    public TwoPhaseReadiness(BackendRegistry registry, java.util.function.Supplier<List<RouterStage.TableShardRule>> rules) {
        this(registry, rules, TwoPhaseReadiness::probe);
    }

    TwoPhaseReadiness(BackendRegistry registry, java.util.function.Supplier<List<RouterStage.TableShardRule>> rules,
            Function<BackendTarget, Probe> probes) {
        this.registry = registry;
        this.rules = rules;
        this.probes = probes;
    }

    /** Probes every backend that holds a shard of a declaratively sharded table that spans more than one backend. */
    public List<Report> check() {
        java.util.Map<String, List<String>> tablesByBackend = new java.util.LinkedHashMap<>();
        for (RouterStage.TableShardRule rule : rules.get()) {
            List<String> backends = ShardingStrategy.allBackends(rule.strategy());
            if (backends.size() < 2) {
                continue;
            }
            for (String b : backends) {
                tablesByBackend.computeIfAbsent(b, k -> new ArrayList<>()).add(rule.tableName());
            }
        }
        List<Report> out = new ArrayList<>();
        for (var e : tablesByBackend.entrySet()) {
            BackendTarget target = registry.resolveForRouting(e.getKey());
            if (target == null) {
                out.add(new Report(e.getKey(), "unknown", State.UNKNOWN, "the backend is not registered", null, e.getValue()));
                continue;
            }
            Probe p;
            try {
                p = probes.apply(target);
            } catch (RuntimeException ex) {
                p = new Probe(State.UNKNOWN, "the check could not run: " + ex.getMessage(), null);
            }
            out.add(new Report(e.getKey(), String.valueOf(target.dialect()), p.state(), p.detail(), p.fix(), e.getValue()));
        }
        return out;
    }

    /** Logs the outcome: one warning per backend that cannot do two-phase commit, an error when writes are set to require it. */
    public List<Report> checkAndLog() {
        List<Report> reports = check();
        boolean required = "required".equalsIgnoreCase(System.getenv("WARP_SHARD_WRITE_2PC"));
        for (Report r : reports) {
            switch (r.state()) {
                case NOT_READY, UNSUPPORTED -> {
                    String msg = "shard backend '{}' ({}) cannot take part in two-phase commit: {}{} Multi-shard writes to {} "
                            + (required ? "will be REFUSED (WARP_SHARD_WRITE_2PC=required)." : "will use commit-last, which can leave some shards committed and others not.");
                    String fix = r.fix() == null ? "" : " Fix: " + r.fix() + ".";
                    if (required) {
                        log.error(msg, r.backend(), r.engine(), r.detail(), fix, r.tables());
                    } else {
                        log.warn(msg, r.backend(), r.engine(), r.detail(), fix, r.tables());
                    }
                }
                case UNKNOWN -> log.warn("shard backend '{}' ({}): could not tell whether it supports two-phase commit: {}", r.backend(), r.engine(), r.detail());
                case READY -> log.info("shard backend '{}' ({}) supports two-phase commit: {}", r.backend(), r.engine(), r.detail());
            }
        }
        return reports;
    }

    static Probe probe(BackendTarget target) {
        SourceDialect d = target.dialect();
        if (d == null) {
            return new Probe(State.UNSUPPORTED, "engine not recognized", null);
        }
        String sql;
        switch (d) {
            case POSTGRES -> sql = "SELECT current_setting('max_prepared_transactions')";
            case SQL_SERVER -> sql = "SELECT CASE WHEN OBJECT_ID('master.dbo.xp_sqljdbc_xa_init') IS NULL THEN 0 ELSE 1 END";
            case ORACLE -> sql = "SELECT COUNT(*) FROM DBA_PENDING_TRANSACTIONS";
            case MYSQL -> sql = "SELECT COUNT(*) FROM information_schema.ENGINES WHERE ENGINE = 'InnoDB' AND SUPPORT IN ('YES', 'DEFAULT')";
            default -> {
                return new Probe(State.UNSUPPORTED, "no XA support is wired up for " + d, null);
            }
        }
        try (Connection c = target.open(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return interpret(d, rs.getString(1));
        } catch (Exception e) {
            if (d == SourceDialect.ORACLE) {
                return new Probe(State.NOT_READY, "cannot read DBA_PENDING_TRANSACTIONS: " + e.getMessage(),
                        "GRANT SELECT ON DBA_PENDING_TRANSACTIONS, FORCE ANY TRANSACTION and EXECUTE ON DBMS_XA to the Warp user");
            }
            return new Probe(State.UNKNOWN, "the probe failed: " + e.getMessage(), null);
        }
    }

    /** Turns the probe's one value into a verdict; separate so it can be tested without a database. */
    static Probe interpret(SourceDialect d, String value) {
        long n;
        try {
            n = Long.parseLong(value == null ? "" : value.trim());
        } catch (NumberFormatException e) {
            return new Probe(State.UNKNOWN, "unexpected value '" + value + "'", null);
        }
        return switch (d) {
            case POSTGRES -> n > 0
                    ? new Probe(State.READY, "max_prepared_transactions=" + n, null)
                    : new Probe(State.NOT_READY, "max_prepared_transactions=0, so PREPARE TRANSACTION is refused",
                            "set max_prepared_transactions to at least the number of concurrent multi-shard writes (e.g. 100) and restart Postgres");
            case SQL_SERVER -> n > 0
                    ? new Probe(State.READY, "the JDBC XA procedures are installed", null)
                    : new Probe(State.NOT_READY, "the JDBC XA extended procedures are not installed in master",
                            "install them (sqljdbc_xa.dll / xp_sqljdbc_xa_init) and grant SqlJDBCXAUser to the Warp login");
            case ORACLE -> new Probe(State.READY, "DBA_PENDING_TRANSACTIONS is readable", null);
            case MYSQL -> n > 0
                    ? new Probe(State.READY, "InnoDB is available (the XA RECOVER privilege was not checked)", null)
                    : new Probe(State.NOT_READY, "InnoDB is not available; XA needs it", "enable the InnoDB storage engine");
            default -> new Probe(State.UNSUPPORTED, "no XA support is wired up for " + d, null);
        };
    }
}

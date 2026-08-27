package com.polygres.wire.core;

import java.sql.Connection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.lookup.LikePattern;

/**
 * Wraps a real, already-built Calcite {@link Schema} (a {@code JdbcSchema} in every real usage --
 * see {@link ShardJoinExecutor}/{@link SchemaFederationStage}'s own javadoc) so every table it
 * exposes goes through {@link StatisticsAwareTable} instead of being handed to the planner as-is,
 * with a real row count from {@link StatisticsStore} instead of Calcite's own default
 * {@code Statistics.UNKNOWN}. Ported from the sibling Omnigate project's own class of the same
 * name/shape (real, tested, production code there) -- {@code JdbcSchema} itself is still built
 * exactly as before in the caller, entirely untouched; this is what actually gets mounted into the
 * federated connection's root schema instead.
 *
 * <p><b>Implements {@link Wrapper}, delegating to the real schema -- load-bearing, not defensive
 * dressing</b> (found live there, not re-derived here from scratch): without it, a federated query
 * fails outright ({@code "not a interface javax.sql.DataSource"}) -- Calcite's own JDBC-to-
 * Enumerable code generation path unwraps the mounted schema to get at the real {@code JdbcSchema}/
 * {@code DataSource} underneath to actually run generated SQL against the backend, and {@code
 * AbstractSchema} doesn't forward that by default. {@link #getExpression} is overridden for the
 * same reason: the runtime code Calcite generates needs to reference the real {@code JdbcSchema}
 * object at execution time, not this wrapper, which only exists to change what the PLANNER believes
 * about row counts.
 */
final class StatisticsAwareSchema extends AbstractSchema implements Wrapper {

    private final Schema delegate;
    private final Connection statsConnection;
    private final String pgSchemaName;
    private final String cacheKeyPrefix;
    private final StatisticsStore statistics;

    /** @param statsConnection a real, already-open JDBC connection to the SAME backend {@code
     *     delegate} mounts -- used only to run {@link StatisticsStore}'s own
     *     {@code pg_class.reltuples} lookup once per table, at schema-mount time; never held open
     *     past this constructor's caller's own connection lifetime, and never used to run the
     *     actual federated query itself.
     * @param pgSchemaName the real Postgres schema name to look statistics up under (may differ
     *     from whatever name this Calcite schema is mounted as -- see {@link ShardJoinExecutor}'s
     *     own internal {@code __polywire_shardN} mount names, which are never the real Postgres
     *     schema).
     * @param cacheKeyPrefix a caller-chosen prefix (e.g. a shard/backend name) so the SAME table
     *     name on two DIFFERENT backends caches two separate row counts, not one shared (wrong) one. */
    StatisticsAwareSchema(Schema delegate, Connection statsConnection, String pgSchemaName, String cacheKeyPrefix,
            StatisticsStore statistics) {
        this.delegate = delegate;
        this.statsConnection = statsConnection;
        this.pgSchemaName = pgSchemaName;
        this.cacheKeyPrefix = cacheKeyPrefix;
        this.statistics = statistics;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> wrapped = new LinkedHashMap<>();
        for (String name : delegate.tables().getNames(LikePattern.any())) {
            Table real = delegate.tables().get(name);
            if (real == null) {
                continue; // named a moment ago by getNames(), gone by the time we looked it up -- skip rather than NPE
            }
            Long rowCount = statistics.rowCount(statsConnection, cacheKeyPrefix + "." + name, pgSchemaName, name);
            wrapped.put(name, new StatisticsAwareTable(real, rowCount));
        }
        return wrapped;
    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        if (delegate instanceof Wrapper wrapper) {
            return wrapper.unwrap(aClass);
        }
        return aClass.isInstance(delegate) ? aClass.cast(delegate) : null;
    }

    @Override
    public Expression getExpression(SchemaPlus parentSchema, String name) {
        return delegate.getExpression(parentSchema, name);
    }
}

package com.nexagres.wire.core;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;

/** A thin delegating {@link TranslatableTable} wrapping a real Calcite {@code JdbcTable} to
 * override only {@link #getStatistic()} -- see {@link StatisticsAwareSchema}'s own javadoc for why
 * this exists at all. Ported from the sibling Omnigate project's own class of the same name/shape;
 * Warp's own version doesn't carry column NDV/distinct-count statistics the way Omnigate's does
 * (that only ever fed Omnigate's opt-in embedded-planner path, which isn't ported here -- see
 * {@link ShardJoinExecutor}/{@link SchemaFederationStage}'s own scope notes) -- real row counts,
 * via {@link FederationStatistics}, are already a real improvement over Calcite's default
 * {@code Statistics.UNKNOWN} on their own. */
final class StatisticsAwareTable implements TranslatableTable, Wrapper {

    private final Table delegate;
    private final Statistic statistic;

    StatisticsAwareTable(Table delegate, Long rowCount) {
        this.delegate = delegate;
        this.statistic = rowCount == null ? Statistics.UNKNOWN : Statistics.of((double) rowCount, java.util.List.of());
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return delegate.getRowType(typeFactory);
    }

    @Override
    public Statistic getStatistic() {
        return statistic;
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return delegate.getJdbcTableType();
    }

    @Override
    public boolean isRolledUp(String column) {
        return delegate.isRolledUp(column);
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return delegate.rolledUpColumnValidInsideAgg(column, call, parent, config);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        // delegate must itself be a TranslatableTable (true for every real Table this wrapper is
        // ever constructed with -- every JdbcTable Calcite's own JdbcSchema produces implements
        // it); if a future caller wraps something that isn't, this fails loudly here rather than
        // silently producing a wrong plan.
        if (!(delegate instanceof TranslatableTable translatable)) {
            throw new IllegalStateException(
                    "StatisticsAwareTable: wrapped table " + delegate + " does not implement TranslatableTable");
        }
        return translatable.toRel(context, relOptTable);
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
}

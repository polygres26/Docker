package com.nexagres.wire.core;

import com.nexagres.wire.rollup.RollupDefinition;
import com.nexagres.wire.rollup.RollupStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RollupStage implements PipelineStage {

    private static final Logger log = LoggerFactory.getLogger(RollupStage.class);

    private static final Pattern SELECT_PREFIX = Pattern.compile("^\\s*select\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern LOOKS_AGGREGATE = Pattern.compile(
            "\\bgroup\\s+by\\b|\\b(sum|count|avg|min|max)\\s*\\(", Pattern.CASE_INSENSITIVE);

    private final RollupStore store;
    private final BackendRegistry backendRegistry;

    public RollupStage(RollupStore store, BackendRegistry backendRegistry) {
        this.store = store;
        this.backendRegistry = backendRegistry;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        String sql = statement.sqlText();
        if (!SELECT_PREFIX.matcher(sql).find() || !LOOKS_AGGREGATE.matcher(sql).find()) {
            return next.proceed(statement);
        }
        RollupDefinition candidate = store.matchingDefinition(sql);
        if (candidate == null || !store.isFresh(candidate)) {
            return next.proceed(statement);
        }
        ExecutionResult accelerated;
        try {
            accelerated = tryAccelerate(candidate, statement);
        } catch (RuntimeException | SQLException e) {
            log.debug("rollup: substitution attempt failed for \"{}\", falling through to the real table ({})",
                    candidate.name(), e.toString());
            accelerated = null;
        }
        return accelerated != null ? accelerated : next.proceed(statement);
    }

    private ExecutionResult tryAccelerate(RollupDefinition def, Statement statement) throws SQLException {
        BackendTarget target = backendRegistry.get(def.backendName());
        if (target == null) {
            return null;
        }
        // Real driver-class lookup, not a hardcoded "org.postgresql.Driver" literal -- a rollup's
        // own backend can now genuinely be a non-Postgres engine (Oracle today; see
        // BackendDriverRegistry's own javadoc). An unrecognized URL prefix falls through to the
        // real table, same as target == null above -- never a wrong answer, just no acceleration.
        String driverClassName = BackendDriverRegistry.driverClassNameFor(target.jdbcUrl());
        if (driverClassName == null) {
            return null;
        }
        List<RelOptRule> rules = new ArrayList<>(EnumerableRules.rules());
        Connection calciteConnection = DriverManager.getConnection("jdbc:calcite:lex=JAVA;caseSensitive=false");
        try {
            CalciteConnection cc = calciteConnection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = cc.getRootSchema();

            DataSource dataSource = JdbcSchema.dataSource(
                    target.jdbcUrl(), driverClassName, target.user(), target.password());
            SqlDialect dialect = JdbcSchema.createDialect(dataSource);
            org.apache.calcite.linq4j.tree.Expression expression =
                    org.apache.calcite.schema.Schemas.subSchemaExpression(rootSchema, def.backendName(), JdbcSchema.class);
            JdbcConvention convention = JdbcConvention.of(dialect, expression, def.backendName());
            
            JdbcSchema jdbcSchema = new JdbcSchema(dataSource, dialect, convention, null, null);
            rootSchema.add(def.backendName(), jdbcSchema);
            rules.addAll(JdbcRules.rules(convention));

            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(rootSchema.getSubSchema(def.backendName()))
                    .parserConfig(org.apache.calcite.sql.parser.SqlParser.config()
                            .withCaseSensitive(false)
                            .withUnquotedCasing(org.apache.calcite.avatica.util.Casing.UNCHANGED))
                    .programs(Programs.ofRules(rules))
                    .build();
            
            Planner planner = Frameworks.getPlanner(config);
            RelNode materializationQueryRel = convert(planner, def.definingSql());
            planner.close();
            planner.reset();
            RelNode materializationTableRel = convert(planner, "SELECT * FROM " + def.rollupTableName());
            planner.close();
            planner.reset();
            RelNode incomingQueryRel = convert(planner, statement.sqlText());

            RelOptMaterialization materialization = new RelOptMaterialization(materializationTableRel,
                    materializationQueryRel, null, List.of(def.backendName(), def.rollupTableName()));
            List<Pair<RelNode, List<RelOptMaterialization>>> rewrites =
                    RelOptMaterializations.useMaterializedViews(incomingQueryRel, List.of(materialization));
            if (rewrites.isEmpty()) {
                return null;
            }
            RelNode rewritten = rewrites.get(0).left;
            
            org.apache.calcite.rel.rel2sql.RelToSqlConverter toSql =
                    new org.apache.calcite.rel.rel2sql.RelToSqlConverter(dialect);
            SqlNode rewrittenSql = toSql.visitRoot(rewritten).asStatement();
            String rewrittenSqlText = rewrittenSql.toSqlString(dialect).getSql();
            try (Connection backendConnection = target.open();
                    PreparedStatement ps = backendConnection.prepareStatement(rewrittenSqlText)) {
                ExecutionResult result = JdbcBackendExecutor.executeOnPreparedStatement(ps, statement.bindParams());
                store.recordHit(def.name());
                log.info("rollup: \"{}\" accelerated a query via {} ({})", def.name(), def.rollupTableName(), rewrittenSqlText);
                return result;
            }
        } finally {
            calciteConnection.close();
        }
    }

    private static RelNode convert(Planner planner, String sql) throws SQLException {
        try {
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);
            RelRoot root = planner.rel(validated);
            return root.rel;
        } catch (Exception e) {
            throw ErrorCatalog.sqlExceptionWithCause("ERR_ROLLUP_PLAN_FAILED", e, sql, e.getMessage());
        }
    }
}

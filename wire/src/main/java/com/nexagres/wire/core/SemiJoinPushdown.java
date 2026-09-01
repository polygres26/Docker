package com.nexagres.wire.core;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real semi-join pushdown for {@link ShardJoinExecutor}/{@link SchemaFederationStage}'s federated
 * equi-joins -- the "no bloom-filter/semi-join pushdown to cut what's shipped between shards"
 * follow-up both classes' own javadoc previously flagged as unbuilt.
 *
 * <p><b>Why an exact semi-join, not a literal Bloom filter</b>: a real Bloom filter is a runtime
 * data structure one execution engine builds and tests row-by-row inside its own process -- there's
 * no portable way to ship one into a REMOTE backend's own SQL (Postgres/MySQL/Oracle/MSSQL have no
 * standard "test membership in this filter" predicate without deploying a custom function per
 * backend, which this project's own established pattern -- see {@code
 * docker/oracle_compat_functions.sql} -- treats as an opt-in customer prerequisite, never something
 * auto-provisioned). An exact semi-join via a real {@code col IN (v1, v2, ...)} predicate, built
 * from the SMALLER side's own real distinct key values and pushed into the LARGER side's own SQL
 * text, gets the same real win (the large side ships only rows that can possibly match, instead of
 * its whole row set) using plain, portable SQL every backend already supports -- exact, not
 * probabilistic, so there's no false-positive rate to reason about either.
 *
 * <p><b>How it fits the existing two-pass architecture</b>: both callers already build a fully
 * mounted Calcite connection (every shard/backend registered as its own schema) before parsing the
 * real query -- this class runs a SECOND, cheap query directly through that SAME connection (Calcite
 * pushes projection/DISTINCT down into the JDBC backend itself, same as any other query through a
 * mounted {@code JdbcSchema}) to collect the build side's real distinct join-key values, then hands
 * the caller back a rewritten SQL string with the probe side's own table reference wrapped in a
 * filtering subquery -- the caller re-parses/re-optimizes THAT text exactly the same way it already
 * parses/optimizes the original, so every existing downstream step (plan capture, leaf-scan
 * profiling, execution) is completely unchanged.
 *
 * <p><b>Deliberately conservative -- degrades to "no pushdown" (never a wrong answer) whenever
 * anything is ambiguous</b>: requires a real {@link StatisticsStore}-backed row-count ESTIMATE for
 * BOTH sides (no stats configured, or either probe failed, means no confident basis for picking a
 * build side, so this doesn't guess); requires the query's {@code ON} clause to be a single simple
 * {@code a.col = b.col} equality between exactly the caller's own two known table references;
 * requires each of those two references to appear EXACTLY ONCE anywhere in the statement (rules out
 * self-joins or a second, unrelated use of the same table elsewhere in the query -- blanket-filtering
 * every occurrence of a table reference used in more than one place could silently change an
 * unrelated part of the query's own result, which this never risks). Any of these failing just skips
 * the optimization and returns the original SQL unchanged -- the real federated join still runs,
 * just without the extra filter.
 */
final class SemiJoinPushdown {

    private static final Logger log = LoggerFactory.getLogger(SemiJoinPushdown.class);
    private static final int DEFAULT_MAX_KEYS = 20_000;

    private static final Pattern ON_EQUI = Pattern.compile(
            "\\bON\\s+([\\w.\"]+?)\\.(\\w+)\\s*=\\s*([\\w.\"]+?)\\.(\\w+)\\b", Pattern.CASE_INSENSITIVE);

    private SemiJoinPushdown() {
    }

    /** One resolved {@code a.col = b.col} equi-join condition between two of the caller's own known
     * table references (never an arbitrary/unknown identifier -- see {@link #detectEqui}). */
    record Equi(String leftRef, String leftColumn, String rightRef, String rightColumn) {
    }

    /** @param refToSource maps each known table reference (the literal, schema-qualified string as
     *     it appears in the ORIGINAL statement, e.g. {@code "orders_db.orders"} or
     *     {@code "shard.customers"}) to the real SQL SOURCE to query it through -- for {@link
     *     SchemaFederationStage} that's just the reference itself (identity); for {@link
     *     ShardJoinExecutor} it's that table's own already-built {@code UNION ALL}-across-shards
     *     expression. Must have exactly 2 entries -- this class's whole detection logic is scoped to
     *     a single two-way join (see this class's own javadoc on why: correctness, not effort). */
    static Equi detectEqui(String sql, Map<String, String> refToSource) {
        if (refToSource.size() != 2) {
            return null;
        }
        for (String ref : refToSource.keySet()) {
            if (countOccurrences(sql, ref) != 1) {
                log.debug("semi-join pushdown: \"{}\" appears more than once (or not at all) in the statement "
                        + "-- skipping, can't safely tell which occurrence the join actually uses", ref);
                return null;
            }
        }
        Map<String, String> aliasToRef = aliasMap(sql, refToSource.keySet());
        Matcher m = ON_EQUI.matcher(sql);
        if (!m.find()) {
            log.debug("semi-join pushdown: no simple \"a.col = b.col\" ON clause found -- skipping");
            return null;
        }
        String leftToken = stripQuotes(m.group(1));
        String leftCol = m.group(2);
        String rightToken = stripQuotes(m.group(3));
        String rightCol = m.group(4);
        String leftRef = aliasToRef.get(leftToken.toLowerCase(java.util.Locale.ROOT));
        String rightRef = aliasToRef.get(rightToken.toLowerCase(java.util.Locale.ROOT));
        if (leftRef == null || rightRef == null || leftRef.equals(rightRef)
                || !refToSource.containsKey(leftRef) || !refToSource.containsKey(rightRef)) {
            log.debug("semi-join pushdown: ON clause's own \"{}\"/\"{}\" tokens didn't resolve to exactly the "
                    + "two known table references -- skipping", leftToken, rightToken);
            return null;
        }
        return new Equi(leftRef, leftCol, rightRef, rightCol);
    }

    /** Real, capped distinct-key collection for the build (smaller) side -- runs directly through
     * the caller's already-fully-mounted Calcite connection, so a cross-shard UNION source is
     * handled exactly like a single-backend one: Calcite pushes the DISTINCT/column projection down
     * into each backend itself (its own, ordinary JDBC-adapter behavior for a plain SQL query over a
     * mounted {@code JdbcSchema}, the same mechanism {@code EXPLAIN PLAN FOR} already relies on
     * running through this same raw connection). Deliberately doesn't carry over any of the original
     * statement's own {@code WHERE} filters -- collecting a (safe, only-ever-a-superset) unfiltered
     * key set is simpler and still correct, just not maximally tight; a real further refinement, not
     * done here.
     *
     * @return {@code null} (not empty) when the real key count exceeds {@code maxKeys} -- an
     *     {@code IN} list that big stops being a net win, and some backends cap literal counts, so
     *     the caller abandons the optimization outright rather than force an oversized filter. An
     *     empty (non-null) list is a real, valid, different outcome: the build side genuinely has no
     *     rows, so the caller can short-circuit the probe side with a guaranteed-empty filter. */
    static List<Object> collectDistinctKeys(Connection calciteConnection, String buildSourceSql,
            String buildKeyColumn, int maxKeys) throws SQLException {
        String sql = "SELECT DISTINCT " + buildKeyColumn + " FROM (" + buildSourceSql + ") __warp_semijoin_build "
                + "FETCH NEXT " + (maxKeys + 1) + " ROWS ONLY";
        List<Object> keys = new ArrayList<>();
        try (Statement st = calciteConnection.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) {
                Object value = rs.getObject(1);
                if (value != null) {
                    keys.add(value);
                }
            }
        }
        if (keys.size() > maxKeys) {
            log.info("semi-join pushdown: build side has more than {} distinct keys -- abandoning the "
                    + "optimization, real join still runs unfiltered", maxKeys);
            return null;
        }
        return keys;
    }

    /** Builds a derived-table SQL fragment that filters {@code probeSourceSql} down to only the rows
     * whose {@code probeKeyColumn} appears in {@code keys} -- or, when {@code keys} is empty, a real
     * {@code WHERE 1=0} short-circuit (an inner equi-join against zero build-side keys can never
     * match anything, so there's no need to touch the probe backend's real data at all). */
    static String buildFilteredSource(String probeSourceSql, String probeKeyColumn, List<Object> keys) {
        String filter = keys.isEmpty() ? "1=0" : probeKeyColumn + " IN (" + literalList(keys) + ")";
        return "(SELECT * FROM (" + probeSourceSql + ") __warp_semijoin_probe WHERE " + filter + ")";
    }

    static int maxKeysFromEnvOrDefault() {
        String raw = System.getenv("WARP_SEMIJOIN_MAX_KEYS");
        if (raw == null || raw.isBlank()) {
            return DEFAULT_MAX_KEYS;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return DEFAULT_MAX_KEYS;
        }
    }

    /** Replaces the SOLE occurrence of {@code ref} in {@code sql} with {@code filteredSource} --
     * safe precisely because {@link #detectEqui} already required {@code ref} to appear exactly
     * once. Mirrors {@link ShardJoinExecutor}'s own union-substitution idiom: whatever the client
     * wrote immediately after the reference (an explicit alias, or nothing) is left untouched. */
    static String substituteRef(String sql, String ref, String filteredSource) {
        return Pattern.compile("(?i)\\b" + Pattern.quote(ref) + "\\b")
                .matcher(sql)
                .replaceFirst(Matcher.quoteReplacement(filteredSource));
    }

    private static int countOccurrences(String sql, String ref) {
        Matcher m = Pattern.compile("(?i)\\b" + Pattern.quote(ref) + "\\b").matcher(sql);
        int count = 0;
        while (m.find()) {
            count++;
        }
        return count;
    }

    /** Maps every alias a known reference is given in the statement (and the reference's own text,
     * unaliased use is valid SQL too) to that reference -- so an {@code ON} clause written in terms
     * of aliases (the overwhelmingly common case, e.g. {@code o.customer_id = c.id}) still resolves
     * back to the real {@code schema.table} references {@link #detectEqui}'s caller knows about. */
    private static Map<String, String> aliasMap(String sql, Set<String> refs) {
        Map<String, String> aliasToRef = new java.util.HashMap<>();
        for (String ref : refs) {
            aliasToRef.put(ref.toLowerCase(java.util.Locale.ROOT), ref);
            Matcher m = Pattern.compile("(?i)\\b" + Pattern.quote(ref) + "\\b\\s+(?:AS\\s+)?(\\w+)").matcher(sql);
            if (m.find()) {
                String alias = m.group(1);
                if (!alias.equalsIgnoreCase("ON") && !alias.equalsIgnoreCase("WHERE") && !alias.equalsIgnoreCase("JOIN")) {
                    aliasToRef.put(alias.toLowerCase(java.util.Locale.ROOT), ref);
                }
            }
        }
        return aliasToRef;
    }

    private static String stripQuotes(String token) {
        return token.replace("\"", "");
    }

    private static String literalList(List<Object> values) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(sqlLiteral(values.get(i)));
        }
        return sb.toString();
    }

    private static String sqlLiteral(Object value) {
        if (value instanceof Number) {
            return value.toString();
        }
        // Real defensive escaping -- a real key value containing a literal "'" (unlikely for a join
        // key, but not impossible) must not be able to break out of the string literal.
        return "'" + value.toString().replace("'", "''") + "'";
    }
}

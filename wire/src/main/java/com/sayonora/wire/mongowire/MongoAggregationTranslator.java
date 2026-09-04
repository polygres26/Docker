package com.sayonora.wire.mongowire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Translates a real MongoDB {@code aggregate} pipeline into ONE SQL query against the JSONB
 * {@code doc} column {@link PostgresDocumentStore} already uses -- closing a real, high-impact
 * gap found auditing this frontend for GA transparency: {@code aggregate} wasn't handled AT ALL
 * before this (fell through to {@code commandNotFound}), and it's the mechanism a typical app
 * uses for grouped reports (totals by customer, counts by status) as well as, in many modern
 * driver/ODM versions, simple sort+project+limit queries issued via {@code aggregate()} instead
 * of {@code find()}.
 *
 * <p>Scope, deliberately narrow, matching this codebase's own discipline of shipping a real,
 * verified slice rather than a partial general-purpose pipeline engine: exactly this stage order,
 * each optional except {@code $group} is exclusive with a bare projection --
 * <pre>{@code [$match] [$group] [$sort] [$limit] [$project]}</pre>
 * -- {@code $project} is refused when {@code $group} is also present (the accumulator names
 * already ARE the output field names in that case). Every other stage ({@code $lookup},
 * {@code $unwind}, {@code $facet}, {@code $bucket}, multiple {@code $group}s, a {@code $group}
 * with a composite/document {@code _id}, a stage out of this order) is refused with a clear error
 * naming exactly what wasn't understood, not silently dropped or mis-executed. {@code $group}
 * accumulators are limited to {@code $sum}/{@code $avg}/{@code $min}/{@code $max} on a top-level
 * field reference or the literal {@code 1} (a real {@code $sum: 1} for counting) -- no
 * {@code $push}/{@code $addToSet}/{@code $first}/{@code $last} (array-building accumulators, a
 * separate follow-up), and {@code _id} is limited to {@code null} (one aggregate row for the
 * whole collection) or a single top-level field reference (no dotted paths, no composite/document
 * keys -- same restriction {@link MongoQueryTranslator} already applies to filters).
 */
final class MongoAggregationTranslator {

    record AggregateQuery(String sql, List<String> jsonbParams) {
    }

    private MongoAggregationTranslator() {
    }

    static AggregateQuery translate(String table, BsonArray pipeline) {
        int idx = 0;
        BsonDocument matchFilter = null;
        if (idx < pipeline.size() && hasStage(pipeline, idx, "$match")) {
            matchFilter = stageValue(pipeline, idx, "$match").asDocument();
            idx++;
        }
        BsonDocument groupSpec = null;
        if (idx < pipeline.size() && hasStage(pipeline, idx, "$group")) {
            groupSpec = stageValue(pipeline, idx, "$group").asDocument();
            idx++;
        }
        BsonDocument sortSpec = null;
        if (idx < pipeline.size() && hasStage(pipeline, idx, "$sort")) {
            sortSpec = stageValue(pipeline, idx, "$sort").asDocument();
            idx++;
        }
        Integer limit = null;
        if (idx < pipeline.size() && hasStage(pipeline, idx, "$limit")) {
            limit = stageValue(pipeline, idx, "$limit").asNumber().intValue();
            idx++;
        }
        BsonDocument projectSpec = null;
        if (idx < pipeline.size() && hasStage(pipeline, idx, "$project")) {
            projectSpec = stageValue(pipeline, idx, "$project").asDocument();
            idx++;
        }
        if (idx != pipeline.size()) {
            String stageName = pipeline.get(idx).asDocument().getFirstKey();
            throw unsupported("stage \"" + stageName + "\" (at position " + idx + ") -- only "
                    + "[$match] [$group] [$sort] [$limit] [$project], in that order, each optional, "
                    + "is supported in this pass");
        }
        if (groupSpec != null && projectSpec != null) {
            throw unsupported("$project after $group -- the accumulator field names in $group are "
                    + "already the output shape in this pass; a separate $project isn't supported yet");
        }

        List<String> params = new ArrayList<>();
        String matchSql = "";
        if (matchFilter != null) {
            MongoQueryTranslator.Where where = MongoQueryTranslator.translate(matchFilter);
            matchSql = where.sql();
            params.addAll(where.jsonbParams());
        }

        if (groupSpec == null) {
            return translateWithoutGroup(table, matchSql, params, sortSpec, limit, projectSpec);
        }
        return translateWithGroup(table, matchSql, params, groupSpec, sortSpec, limit);
    }

    private static AggregateQuery translateWithoutGroup(String table, String matchSql, List<String> params,
            BsonDocument sortSpec, Integer limit, BsonDocument projectSpec) {
        String selectExpr = projectSpec == null ? "doc" : buildProjection(projectSpec);
        StringBuilder sql = new StringBuilder("SELECT ").append(selectExpr).append(" FROM ").append(table)
                .append(matchSql);
        appendOrderBy(sql, sortSpec, false);
        appendLimit(sql, limit);
        return new AggregateQuery(sql.toString(), params);
    }

    private static AggregateQuery translateWithGroup(String table, String matchSql, List<String> params,
            BsonDocument groupSpec, BsonDocument sortSpec, Integer limit) {
        if (!groupSpec.containsKey("_id")) {
            throw unsupported("$group with no _id field");
        }
        BsonValue idSpec = groupSpec.get("_id");
        String idExprSql;
        String idFieldRef;
        if (idSpec.isNull()) {
            idExprSql = "'null'::jsonb";
            idFieldRef = null;
        } else if (idSpec.isString() && idSpec.asString().getValue().startsWith("$")) {
            idFieldRef = idSpec.asString().getValue().substring(1);
            requireSimpleFieldName(idFieldRef, "$group._id");
            idExprSql = "doc->" + quoteLiteral(idFieldRef);
        } else {
            throw unsupported("$group._id shape (only null or a single top-level \"$field\" reference "
                    + "is supported in this pass, not a composite/document _id)");
        }

        List<String> innerColumns = new ArrayList<>();
        List<String> outerFields = new ArrayList<>();
        innerColumns.add(idExprSql + " AS grp_id");
        for (Map.Entry<String, BsonValue> entry : groupSpec.entrySet()) {
            if ("_id".equals(entry.getKey())) {
                continue;
            }
            String outputField = entry.getKey();
            requireSimpleFieldName(outputField, "$group output field");
            BsonDocument accumulator = entry.getValue().asDocument();
            if (accumulator.size() != 1) {
                throw unsupported("$group accumulator for \"" + outputField + "\" (exactly one operator expected)");
            }
            Map.Entry<String, BsonValue> accEntry = accumulator.entrySet().iterator().next();
            innerColumns.add(accumulatorSql(accEntry.getKey(), accEntry.getValue(), outputField) + " AS "
                    + quoteIdent(outputField));
            outerFields.add(outputField);
        }

        StringBuilder inner = new StringBuilder("SELECT ").append(String.join(", ", innerColumns))
                .append(" FROM ").append(table).append(matchSql).append(" GROUP BY ").append(idExprSql);

        StringBuilder outerSelect = new StringBuilder("jsonb_build_object('_id', grp_id");
        for (String field : outerFields) {
            outerSelect.append(", ").append(quoteLiteral(field)).append(", ").append(quoteIdent(field));
        }
        outerSelect.append(") AS doc");

        StringBuilder sql = new StringBuilder("SELECT ").append(outerSelect)
                .append(" FROM (").append(inner).append(") t");
        appendOrderBy(sql, sortSpec, true);
        appendLimit(sql, limit);
        return new AggregateQuery(sql.toString(), params);
    }

    private static String accumulatorSql(String op, BsonValue operand, String outputField) {
        if ("$sum".equals(op) && operand.isNumber() && operand.asNumber().intValue() == 1) {
            return "count(*)";
        }
        String field = requireFieldRef(operand, "$group.\"" + outputField + "\"'s " + op);
        String numericExpr = "(doc->>" + quoteLiteral(field) + ")::numeric";
        return switch (op) {
            case "$sum" -> "sum(" + numericExpr + ")";
            case "$avg" -> "avg(" + numericExpr + ")";
            case "$min" -> "min(doc->>" + quoteLiteral(field) + ")";
            case "$max" -> "max(doc->>" + quoteLiteral(field) + ")";
            default -> throw unsupported("$group accumulator \"" + op + "\" -- only $sum/$avg/$min/$max "
                    + "(on a top-level field reference or, for $sum, the literal 1) are supported in "
                    + "this pass, not $push/$addToSet/$first/$last");
        };
    }

    private static String requireFieldRef(BsonValue value, String where) {
        if (!value.isString() || !value.asString().getValue().startsWith("$")) {
            throw unsupported(where + " -- expected a \"$field\" reference");
        }
        String field = value.asString().getValue().substring(1);
        requireSimpleFieldName(field, where);
        return field;
    }

    private static void requireSimpleFieldName(String field, String where) {
        if (field.isEmpty() || field.contains(".") || field.startsWith("$")) {
            throw unsupported(where + ": \"" + field + "\" -- only a top-level field name is supported "
                    + "in this pass, not a dotted path or nested expression");
        }
    }

    private static String buildProjection(BsonDocument projectSpec) {
        List<String> fields = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : projectSpec.entrySet()) {
            String field = entry.getKey();
            if ("_id".equals(field)) {
                // Mongo's own default: _id is included unless explicitly excluded with 0 -- an
                // explicit 0 here is honored (skip it); anything else falls through to inclusion.
                if (entry.getValue().isNumber() && entry.getValue().asNumber().intValue() == 0) {
                    continue;
                }
            }
            requireSimpleFieldName(field, "$project");
            if (!entry.getValue().isNumber() || entry.getValue().asNumber().intValue() != 1) {
                throw unsupported("$project value for \"" + field + "\" -- only plain inclusion "
                        + "({field: 1}) is supported in this pass, not computed expressions or renaming");
            }
            fields.add(field);
        }
        if (fields.isEmpty()) {
            throw unsupported("$project with no included fields");
        }
        StringBuilder sql = new StringBuilder("jsonb_build_object(");
        boolean first = true;
        if (!projectSpec.containsKey("_id") || !(projectSpec.get("_id").isNumber() && projectSpec.get("_id").asNumber().intValue() == 0)) {
            sql.append("'_id', doc->'_id'");
            first = false;
        }
        for (String field : fields) {
            if (!first) {
                sql.append(", ");
            }
            first = false;
            sql.append(quoteLiteral(field)).append(", doc->").append(quoteLiteral(field));
        }
        sql.append(") AS doc");
        return sql.toString();
    }

    private static void appendOrderBy(StringBuilder sql, BsonDocument sortSpec, boolean groupedResult) {
        if (sortSpec == null || sortSpec.isEmpty()) {
            return;
        }
        List<String> terms = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : sortSpec.entrySet()) {
            String field = entry.getKey();
            requireSimpleFieldName(field, "$sort");
            String direction = entry.getValue().asNumber().intValue() < 0 ? "DESC" : "ASC";
            String column = "_id".equals(field) && groupedResult ? "grp_id"
                    // doc->'field' (jsonb), NOT doc->>'field' (text) -- jsonb's own comparison
                    // operators sort scalars by type first, then value (numbers compare
                    // numerically among themselves), so this sorts a numeric field correctly
                    // without needing to know the field's type ahead of time. Using ->>'s plain
                    // text form here was a real bug, found live: it sorted "100.0" before "5.0"
                    // lexicographically.
                    : groupedResult ? quoteIdent(field) : "doc->" + quoteLiteral(field);
            terms.add(column + " " + direction);
        }
        sql.append(" ORDER BY ").append(String.join(", ", terms));
    }

    private static void appendLimit(StringBuilder sql, Integer limit) {
        if (limit != null && limit > 0) {
            sql.append(" LIMIT ").append(limit);
        }
    }

    private static boolean hasStage(BsonArray pipeline, int idx, String stageName) {
        BsonValue stage = pipeline.get(idx);
        return stage.isDocument() && stage.asDocument().size() == 1 && stage.asDocument().containsKey(stageName);
    }

    private static BsonValue stageValue(BsonArray pipeline, int idx, String stageName) {
        return pipeline.get(idx).asDocument().get(stageName);
    }

    private static String quoteIdent(String field) {
        return "\"" + field.replace("\"", "\"\"") + "\"";
    }

    private static String quoteLiteral(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static IllegalArgumentException unsupported(String what) {
        return new IllegalArgumentException("aggregate: unsupported " + what);
    }
}

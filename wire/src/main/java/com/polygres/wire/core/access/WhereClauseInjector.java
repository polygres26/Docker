package com.polygres.wire.core.access;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class WhereClauseInjector {

    private WhereClauseInjector() {
    }

    private static final Pattern CLAUSE_BOUNDARY = Pattern.compile(
            "\\b(group\\s+by|having|order\\s+by|limit|offset|fetch\\s+(?:first|next))\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern WHERE_KEYWORD = Pattern.compile("\\bwhere\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern PLACEHOLDER = Pattern.compile("\\?");

    public record Injected(String sqlText, List<Object> bindParams) {
    }

    public static Injected inject(String sqlText, List<Object> bindParams, String filterColumn, Object value) {
        String trimmed = sqlText.stripTrailing();
        boolean hadTrailingSemicolon = trimmed.endsWith(";");
        String body = hadTrailingSemicolon ? trimmed.substring(0, trimmed.length() - 1) : trimmed;

        int insertAt = findInsertionPoint(body);
        boolean hasWhere = WHERE_KEYWORD.matcher(body.substring(0, insertAt)).find();
        String predicate = (hasWhere ? " AND " : " WHERE ") + filterColumn + " = ?";

        String before = body.substring(0, insertAt).stripTrailing();
        String after = body.substring(insertAt).stripLeading();
        String newSql = before + predicate + (after.isEmpty() ? "" : " " + after) + (hadTrailingSemicolon ? ";" : "");

        int placeholderIndexBeforeInsertion = countPlaceholders(body.substring(0, insertAt));
        List<Object> newBinds = new ArrayList<>(bindParams);
        newBinds.add(placeholderIndexBeforeInsertion, value);

        return new Injected(newSql, newBinds);
    }

    private static int findInsertionPoint(String body) {
        Matcher matcher = CLAUSE_BOUNDARY.matcher(body);
        return matcher.find() ? matcher.start() : body.length();
    }

    private static int countPlaceholders(String sqlPrefix) {
        int count = 0;
        Matcher matcher = PLACEHOLDER.matcher(sqlPrefix);
        while (matcher.find()) {
            count++;
        }
        return count;
    }
}

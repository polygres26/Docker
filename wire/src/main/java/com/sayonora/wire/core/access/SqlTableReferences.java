package com.sayonora.wire.core.access;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SqlTableReferences {

    private SqlTableReferences() {
    }

    private static final Pattern FROM_OR_JOIN = Pattern.compile(
            "\\b(?:from|join)\\s+([a-zA-Z_][\\w$]*(?:\\.[a-zA-Z_][\\w$]*)*)", Pattern.CASE_INSENSITIVE);

    public static Set<String> extract(String sqlText) {
        Set<String> tables = new LinkedHashSet<>();
        if (sqlText == null) {
            return tables;
        }
        Matcher matcher = FROM_OR_JOIN.matcher(sqlText);
        while (matcher.find()) {
            tables.add(matcher.group(1));
        }
        return tables;
    }

    public static boolean anyMatches(String sqlText, Pattern tablePattern) {
        for (String table : extract(sqlText)) {
            if (tablePattern.matcher(table).find()) {
                return true;
            }
        }
        return false;
    }
}

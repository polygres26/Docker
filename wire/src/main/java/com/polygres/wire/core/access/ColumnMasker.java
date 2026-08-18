package com.polygres.wire.core.access;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ColumnMasker {

    private ColumnMasker() {
    }

    public static String mask(String sqlText, String column) {
        Pattern reference = Pattern.compile("(?:\\b[\\w$]+\\.)?\\b" + Pattern.quote(column) + "\\b", Pattern.CASE_INSENSITIVE);
        Matcher matcher = reference.matcher(sqlText);
        StringBuilder result = new StringBuilder();
        int last = 0;
        while (matcher.find()) {
            result.append(sqlText, last, matcher.start()).append("NULL");
            last = matcher.end();
        }
        result.append(sqlText.substring(last));
        return result.toString();
    }
}

package com.sayonora.wire.core;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DbLinkStage implements PipelineStage {

    private static final Pattern DB_LINK_REF =
            Pattern.compile("\\b([A-Za-z][\\w$#]*)@([A-Za-z][\\w$#]*)\\b");

    private final BackendRegistry registry;

    public DbLinkStage(BackendRegistry registry) {
        this.registry = registry;
    }

    @Override
    public ExecutionResult handle(Statement statement, PipelineChain next) throws SQLException {
        if (registry.isEmpty()) {
            return next.proceed(statement);
        }
        String sql = statement.sqlText();
        Matcher matcher = DB_LINK_REF.matcher(sql);
        int searchFrom = 0;
        while (matcher.find(searchFrom)) {
            if (!isInsideStringLiteral(sql, matcher.start())) {
                String linkName = matcher.group(2);
                BackendTarget target = registry.get(linkName);
                if (target == null) {
                    
                    throw ErrorCatalog.sqlException("ERR_DBLINK_NOT_FOUND", linkName);
                }
                
                String rewritten = sql.substring(0, matcher.start(2) - 1) + sql.substring(matcher.end(2));
                Statement resolved = statement.withSqlText(rewritten).withRouting(statement.workloadClass(), linkName);
                return next.proceed(resolved);
            }
            searchFrom = matcher.end();
        }
        return next.proceed(statement);
    }

    private static boolean isInsideStringLiteral(String sql, int position) {
        boolean inString = false;
        int i = 0;
        while (i < position) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inString && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i += 2;
                    continue;
                }
                inString = !inString;
            }
            i++;
        }
        return inString;
    }
}

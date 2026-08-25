package com.polygres.wire.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renders a real Postgres backend error's message TEXT in the calling client's own dialect
 * vocabulary -- e.g. an orawire client sees {@code ORA-00942: table or view does not exist}
 * instead of Postgres's {@code relation "foo" does not exist}. This is the message-text
 * counterpart to {@link SqlStateErrorMapper}, which already assigns the correct dialect-native
 * error NUMBER for the same SQLSTATE; together they mean a real Oracle/MySQL/SQL Server client
 * sees both the right code AND the right wording, not just a foreign-sounding Postgres sentence
 * behind a native-looking error number.
 *
 * <p>Every template lives in {@code errors/{dialect}_en.properties} on the classpath, keyed by
 * Postgres SQLSTATE, using the same {@link MessageFormat} convention as {@link ErrorCatalog}.
 * Templates are English-only today ({@code _en} suffix) -- the file-per-locale shape is already
 * in place for a future {@code errors/oracle_es.properties} etc. to be a drop-in addition, not a
 * rewrite; there is intentionally no locale-selection wiring yet (see the class doc on Phase 3's
 * scope -- that's a deliberate follow-up, not an oversight).
 *
 * <p>A placeholder's value is never invented -- it's extracted from the REAL Postgres message via
 * a per-SQLSTATE regex in {@link #EXTRACTORS} (e.g. pulling {@code foo} out of Postgres's own
 * {@code relation "foo" does not exist}), so the identifier in the rendered message is always the
 * genuine one from the actual failure, not a placeholder Postgres never sent. A SQLSTATE with no
 * template, or whose extractor doesn't match the real message (a different Postgres version
 * phrased it differently than expected), falls back to forwarding Postgres's own message text
 * completely unchanged -- this class only ever upgrades a message, it never degrades one into
 * something broken or missing.
 */
public final class DialectErrorMessages {

    // Identifier-extraction regex, one per SQLSTATE, applied to the real Postgres message text --
    // shared across all three dialects, since the SOURCE text (Postgres's own wording) is the
    // same regardless of which wire protocol the client is speaking; only the rendered template
    // differs per dialect. Only present for SQLSTATEs whose dialect-native template (in at least
    // one of the three properties files) actually has a {0}/{1} placeholder to fill -- a SQLSTATE
    // whose real dialect-native wording is fixed text in every dialect (e.g. deadlock_detected)
    // needs no extractor at all.
    private static final Map<String, Pattern> EXTRACTORS = Map.ofEntries(
            Map.entry("42P01", Pattern.compile("relation \"(.+?)\" does not exist")),
            Map.entry("42703", Pattern.compile("column \"(.+?)\" does not exist")),
            Map.entry("23505", Pattern.compile("duplicate key value violates unique constraint \"(.+?)\"")),
            Map.entry("23502", Pattern.compile(
                    "null value in column \"(.+?)\" of relation \"(.+?)\" violates not-null constraint")),
            Map.entry("23503", Pattern.compile(
                    "insert or update on table \"(.+?)\" violates foreign key constraint \"(.+?)\"")),
            Map.entry("42P07", Pattern.compile("relation \"(.+?)\" already exists")),
            Map.entry("23514", Pattern.compile(
                    "new row for relation \"(.+?)\" violates check constraint \"(.+?)\"")),
            Map.entry("22P02", Pattern.compile("invalid input syntax for type (\\w+): \"(.*?)\"")),
            Map.entry("42501", Pattern.compile("permission denied for \\w+ (.+)")),
            Map.entry("42883", Pattern.compile("function (.+?) does not exist")));

    private static final Properties ORACLE_TEMPLATES = load("errors/oracle_en.properties");
    private static final Properties MYSQL_TEMPLATES = load("errors/mysql_en.properties");
    private static final Properties SQL_SERVER_TEMPLATES = load("errors/sqlserver_en.properties");

    private static Properties load(String resourcePath) {
        Properties props = new Properties();
        try (InputStream in = DialectErrorMessages.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("missing dialect error message resource: " + resourcePath);
            }
            props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load dialect error messages: " + resourcePath, e);
        }
        return props;
    }

    private static Properties templatesFor(SourceDialect dialect) {
        return switch (dialect) {
            case ORACLE -> ORACLE_TEMPLATES;
            case MYSQL -> MYSQL_TEMPLATES;
            case SQL_SERVER -> SQL_SERVER_TEMPLATES;
            default -> throw new IllegalArgumentException(
                    "DialectErrorMessages has no dialect-native templates for " + dialect
                            + " -- only ORACLE/MYSQL/SQL_SERVER emulate a foreign error vocabulary; "
                            + "every other dialect's client already expects real Postgres wording");
        };
    }

    /** Renders {@code postgresMessage} (a real Postgres backend error's own text, exactly as
     * caught from {@code SQLException.getMessage()}) in {@code dialect}'s native wording for
     * {@code sqlState}, if a template and (when needed) a matching identifier exist -- otherwise
     * returns {@code postgresMessage} completely unchanged. Never throws over a rendering
     * problem: a missing template, a missing extractor, or an extractor that doesn't match the
     * real message are all just reasons to pass the original message through, not failures. */
    public static String render(SourceDialect dialect, String sqlState, String postgresMessage) {
        if (sqlState == null || postgresMessage == null) {
            return postgresMessage;
        }
        String template = templatesFor(dialect).getProperty(sqlState);
        if (template == null) {
            return postgresMessage;
        }
        Pattern extractor = EXTRACTORS.get(sqlState);
        if (extractor == null) {
            // A dialect-native template exists with no placeholder to fill (e.g. "ORA-00060:
            // deadlock detected while waiting for resource") -- return it as-is.
            return template;
        }
        Matcher matcher = extractor.matcher(postgresMessage);
        if (!matcher.find()) {
            return postgresMessage;
        }
        Object[] groups = new Object[matcher.groupCount()];
        for (int i = 0; i < groups.length; i++) {
            groups[i] = matcher.group(i + 1);
        }
        return MessageFormat.format(template, groups);
    }

    private DialectErrorMessages() {
    }
}

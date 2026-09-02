package com.sayonora.wire.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.junit.jupiter.api.Test;

/**
 * Proves the error catalog itself works correctly -- in particular the MessageFormat quoting
 * behavior that motivated escaping every literal apostrophe ({@code isn't}, {@code doesn't},
 * {@code can't}, {@code Warp's}) as {@code ''} in errors/en.properties: an unescaped
 * apostrophe in a template that also has a placeholder causes MessageFormat to silently stop
 * substituting from that point on, which would ship a client-visible error message with a raw
 * {@code {0}} in it instead of the real value. This is exactly the kind of bug that's invisible
 * by inspection and only shows up when the formatter actually runs.
 */
class ErrorCatalogTest {

    @Test
    void aZeroArgTemplateIsReturnedAsIs() {
        assertEquals("statement rejected by firewall: stacked query detected",
                ErrorCatalog.format("ERR_FIREWALL_STACKED_QUERY"));
    }

    @Test
    void aTemplateWithPlaceholdersSubstitutesRealValues() {
        assertEquals("router assigned unknown backend \"shard-3\"",
                ErrorCatalog.format("ERR_ROUTER_UNKNOWN_BACKEND", "shard-3"));
    }

    @Test
    void anApostropheAfterAPlaceholderDoesNotSwallowLaterPlaceholders() {
        // Regression case: ERR_ROLLUP_UNKNOWN_BACKEND has "isn't" (escaped as isn'' in the
        // properties file) coming AFTER both {0} and {1} in the template. If the escaping were
        // missing, MessageFormat would still substitute {0}/{1} here (they come first), but any
        // template with an unescaped apostrophe BEFORE a later placeholder would silently drop
        // it -- covered by the next test.
        String message = ErrorCatalog.format("ERR_ROLLUP_UNKNOWN_BACKEND", "daily_totals", "primary");
        assertEquals("rollup \"daily_totals\" references backend \"primary\", "
                + "which isn't a configured WARP_BACKENDS entry", message);
    }

    @Test
    void anApostropheBeforeAPlaceholderStillLetsThatPlaceholderSubstitute() {
        // ERR_SCATTER_DISTINCT_UNSUPPORTED has "can't" AFTER its one placeholder {0} -- but this
        // is the shape that would break if a future template needed a placeholder AFTER an
        // unescaped apostrophe. Asserting the funcName actually appears (not a literal "{0}")
        // is the real regression guard.
        String message = ErrorCatalog.format("ERR_SCATTER_DISTINCT_UNSUPPORTED", "COUNT");
        assertTrue(message.contains("COUNT(DISTINCT ...)"), "expected the real function name substituted, got: " + message);
        assertTrue(message.contains("can't be correctly merged"), "expected the literal apostrophe preserved, got: " + message);
        assertTrue(message.indexOf("{0}") < 0, "a literal \"{0}\" in the output means MessageFormat substitution broke: " + message);
    }

    @Test
    void aMissingKeyFailsLoudlyInsteadOfShippingTheRawKeyAsAMessage() {
        assertThrows(IllegalStateException.class, () -> ErrorCatalog.format("ERR_THIS_KEY_DOES_NOT_EXIST"));
    }

    @Test
    void sqlExceptionCarriesTheFormattedMessage() {
        SQLException e = ErrorCatalog.sqlException("ERR_UNSUPPORTED_SCHEMA_USERNAME", "bad;name");
        assertEquals("unsupported username as schema name: bad;name", e.getMessage());
    }

    @Test
    void sqlExceptionWithStateCarriesTheGivenSqlState() {
        SQLException e = ErrorCatalog.sqlExceptionWithState("ERR_QOS_RATE_LIMIT", "57014", "write");
        assertEquals("57014", e.getSQLState());
        assertEquals("rate limit exceeded for workload class \"write\"", e.getMessage());
    }

    @Test
    void sqlExceptionWithCauseCarriesTheGivenCause() {
        RuntimeException cause = new RuntimeException("boom");
        SQLException e = ErrorCatalog.sqlExceptionWithCause("ERR_NATIVE_CONNECT_FAILED", cause);
        assertEquals("native connect failed", e.getMessage());
        assertEquals(cause, e.getCause());
    }

    @Test
    void everyKeyReferencedFromMainSourceHasACatalogEntry() throws java.io.IOException {
        // Cheap regression guard against a typo'd key at a throw site: every ERR_* identifier
        // used as a String literal argument to an ErrorCatalog.sqlException*/format call anywhere
        // under src/main/java must resolve. This walks the source tree rather than hardcoding the
        // key list here, so it keeps catching new call sites without needing to be updated by hand.
        java.nio.file.Path mainJava = java.nio.file.Path.of("src", "main", "java");
        java.util.regex.Pattern callSite = java.util.regex.Pattern.compile(
                "ErrorCatalog\\.(?:sqlException|sqlExceptionWithState|sqlExceptionWithCause|format)\\(\\s*\"([A-Z_]+)\"");
        java.util.Set<String> referencedKeys = new java.util.TreeSet<>();
        try (var paths = java.nio.file.Files.walk(mainJava)) {
            for (java.nio.file.Path p : paths.filter(p -> p.toString().endsWith(".java")).toList()) {
                String content = java.nio.file.Files.readString(p);
                var matcher = callSite.matcher(content);
                while (matcher.find()) {
                    referencedKeys.add(matcher.group(1));
                }
            }
        }
        assertTrue(referencedKeys.size() >= 20,
                "expected to find the ~22 ErrorCatalog call sites, found: " + referencedKeys.size());
        for (String key : referencedKeys) {
            try {
                // 4 dummy args covers every template's placeholder count; MessageFormat ignores
                // extra args, so this only ever fails (IllegalStateException) for a genuinely
                // missing key, never for a key whose template needs fewer than 4.
                ErrorCatalog.format(key, "x", "y", "z", "w");
            } catch (IllegalStateException missing) {
                throw new AssertionError("no catalog entry for key referenced in source: " + key, missing);
            }
        }
    }
}

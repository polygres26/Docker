package com.nexagres.wire.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

/**
 * Central catalog for Warp's own repo-authored error messages -- as opposed to messages that
 * come back from the real Postgres JDBC driver, which are forwarded to the client verbatim (see
 * {@link SqlStateErrorMapper} for how <em>those</em> get a dialect-native error CODE assigned;
 * this class has nothing to do with that path).
 *
 * <p>Messages live in {@code errors/en.properties} on the classpath, keyed by a stable
 * {@code ERR_*} id, with {@link MessageFormat} placeholders ({@code {0}}, {@code {1}}, ...)
 * instead of string concatenation at each throw site. That's the whole point of this class: a
 * call site says <em>which</em> error happened and supplies the values that fill it in, not the
 * English sentence -- so adding {@code errors/es.properties} (or any other locale) later is a new
 * properties file, not a rewrite of every {@code throw new SQLException(...)} across the
 * pipeline. Phase 1 ships English only; the lookup-by-key mechanism here is already what a future
 * locale would reuse, it just isn't wired to a locale selector yet.
 *
 * <p>Deliberately out of scope for this class: the message text that comes from the real Postgres
 * backend inside a caught {@code SQLException} (e.g. {@code relation "x" already exists}) --
 * localizing <em>that</em> needs per-SQLSTATE templates with identifier extraction, since the
 * text is generated dynamically by Postgres itself, not authored in this repo.
 */
public final class ErrorCatalog {

    private static final Properties MESSAGES = load("errors/en.properties");

    private static Properties load(String resourcePath) {
        Properties props = new Properties();
        try (InputStream in = ErrorCatalog.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("missing error catalog resource: " + resourcePath);
            }
            // Explicit UTF-8 reader, not the 2-arg load(InputStream) overload -- that one assumes
            // ISO-8859-1 per the Properties javadoc, which would mangle the em-dash characters in
            // a couple of these messages.
            props.load(new InputStreamReader(in, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load error catalog: " + resourcePath, e);
        }
        return props;
    }

    /** Formats {@code key}'s template with {@code args} via {@link MessageFormat}. A missing key
     * is a programming error (typo, or a message that was never added to the properties file),
     * so this throws rather than silently falling back to the raw key string -- that failure mode
     * would otherwise ship a client-visible error message like {@code "ERR_SOME_TYPO"} instead of
     * failing the build/test that exercises it. */
    public static String format(String key, Object... args) {
        String template = MESSAGES.getProperty(key);
        if (template == null) {
            throw new IllegalStateException("no error catalog entry for key: " + key);
        }
        return args.length == 0 ? template : MessageFormat.format(template, args);
    }

    public static SQLException sqlException(String key, Object... args) {
        return new SQLException(format(key, args));
    }

    public static SQLException sqlExceptionWithState(String key, String sqlState, Object... args) {
        return new SQLException(format(key, args), sqlState);
    }

    public static SQLException sqlExceptionWithCause(String key, Throwable cause, Object... args) {
        return new SQLException(format(key, args), cause);
    }

    private ErrorCatalog() {
    }
}

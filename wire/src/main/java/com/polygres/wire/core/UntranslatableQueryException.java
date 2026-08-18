package com.polygres.wire.core;

import java.sql.SQLException;

/**
 * Thrown by {@link DialectTranslationStage} when neither {@link DialectTranslations}' deterministic
 * rules nor the {@link TranslationLlmClient} fallback can turn a statement into valid SQL for its
 * target dialect — this needs manual migration work, not a guess. Deliberately distinct from a
 * generic {@link SQLException} so callers (and operators reading logs) can tell "we don't have a
 * rule for this and the LLM fallback also failed" apart from an ordinary backend error, and so no
 * caller is tempted to catch {@link SQLException} broadly and retry with the untranslated SQL —
 * see this project's port notes: untranslatable queries get rejected clearly and flagged, never
 * silently passed through unchanged or best-effort translated.
 */
public class UntranslatableQueryException extends SQLException {

    public UntranslatableQueryException(String sqlText, SourceDialect from, SourceDialect to, String reason) {
        super("statement cannot be translated from " + from + " to " + to + " (needs manual migration): "
                + reason + " -- original SQL: " + sqlText, "0A000");
    }

    public UntranslatableQueryException(String sqlText, SourceDialect from, SourceDialect to, String reason, Throwable cause) {
        super("statement cannot be translated from " + from + " to " + to + " (needs manual migration): "
                + reason + " -- original SQL: " + sqlText, "0A000", cause);
    }
}

package com.sayonora.warp.core;

/**
 * Thrown from inside {@link DialectTranslations}'s normalizers (a plain {@code
 * Function<String, String>}, which cannot declare a checked exception) when a statement genuinely
 * requires a Shim extension (pg_mysql/pg_sqlserver/pg_oracle) that this backend doesn't have
 * installed, and -- unlike most of this file's Shim-dependent rewrites -- has no working,
 * plain-Postgres degraded fallback to fall back to (see {@code
 * DialectTranslations.normalizeMysql()}'s SHOW COLUMNS/DESCRIBE/SHOW INDEX/SHOW VARIABLES/SHOW
 * CREATE TABLE rewrites: none of those map onto anything plain Postgres can do natively). Caught by
 * {@link DialectTranslationStage#translateWithFallback} and converted into a real {@link
 * UntranslatableQueryException} there, since that's the checked-exception boundary a caller
 * actually expects.
 *
 * <p>Real gap this exists to close (2026-09-29, raised directly: "Emulate should not always assume
 * pg_* modules are linked because Warp can be run against Supabase or RDS Postgres"): before this,
 * these rewrites unconditionally targeted {@code mysql_catalog.*}/{@code sys.*} functions with no
 * availability check at all, so against a real managed Postgres with no Shim installed (Supabase,
 * RDS, Cloud SQL, Azure Database for PostgreSQL -- none of which allow installing a third-party C
 * extension), the rewritten SQL referenced a schema that doesn't exist, surfacing a raw, confusing
 * "schema mysql_catalog does not exist" error instead of a clear, actionable one naming the real
 * cause.
 */
final class ShimUnavailableException extends RuntimeException {

    ShimUnavailableException(String message) {
        super(message);
    }
}

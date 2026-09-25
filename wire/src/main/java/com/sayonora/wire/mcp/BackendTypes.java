package com.sayonora.wire.mcp;

import com.sayonora.wire.core.BackendTarget;
import com.sayonora.wire.core.SourceDialect;
import java.util.Locale;

/**
 * Automatic backend TYPE derivation. The type is never configured: it is read off the backend's own
 * definition (the JDBC URL prefix / connector pseudo-URL scheme in {@code WARP_BACKENDS}) via
 * {@link BackendTarget#dialect()}. Examples: {@code jdbc:postgresql:} -> {@code postgres},
 * {@code jdbc:mysql:} -> {@code mysql}, {@code jdbc:sqlserver:} -> {@code sqlserver},
 * {@code jdbc:oracle:} -> {@code oracle}, {@code jdbc:snowflake:} -> {@code snowflake} (every other
 * bundled JDBC dialect is its lower-cased dialect name), {@code mongodb://} -> {@code mongodb},
 * {@code dynamodb://} -> {@code dynamodb}, {@code s3://} -> {@code s3}, {@code kafka://},
 * {@code cassandra://}, {@code splunk://}; an unrecognised {@code jdbc:} prefix is {@code jdbc}.
 */
public final class BackendTypes {

    private BackendTypes() {
    }

    public static String typeOf(BackendTarget target) {
        SourceDialect d = target.dialect();
        String url = target.jdbcUrl() == null ? "" : target.jdbcUrl().toLowerCase(Locale.ROOT);
        if (d == null) {
            return url.startsWith("jdbc:") ? "jdbc" : "unknown";
        }
        if (url.startsWith("jdbc:mariadb:")) {
            return "mariadb";
        }
        return switch (d) {
            case POSTGRES -> "postgres";
            case SQL_SERVER -> "sqlserver";
            default -> d.name().toLowerCase(Locale.ROOT);
        };
    }

    public static BackendKind kindOf(BackendTarget target) {
        SourceDialect d = target.dialect();
        if (d == null) {
            return BackendKind.RELATIONAL;
        }
        return switch (d) {
            case DYNAMODB -> BackendKind.DYNAMODB;
            case MONGODB -> BackendKind.MONGODB;
            case S3 -> BackendKind.S3;
            case KAFKA -> BackendKind.KAFKA;
            case CASSANDRA -> BackendKind.CASSANDRA;
            case SPLUNK -> BackendKind.SPLUNK;
            default -> BackendKind.RELATIONAL;
        };
    }
}

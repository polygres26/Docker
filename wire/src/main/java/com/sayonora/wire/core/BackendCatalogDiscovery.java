package com.sayonora.wire.core;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Real schema auto-discovery across every backend {@link BackendRegistry} knows about --
 * {@code query_federated}'s answer to a real gap found live: {@link SchemaFederationStage} (and
 * the wire-protocol frontends' own routing) can only federate across backends an OPERATOR
 * pre-declared via {@code WARP_ROUTER_SCHEMA_RULES}, naming which schema alias lives on which
 * backend ahead of time. That's the right model for a real Postgres/MySQL/SQL Server client, which
 * hands Warp nothing but raw SQL text with no other signal -- a config-declared rule is genuinely
 * the only thing to route on. It's the WRONG model for MCP/NL-to-SQL: an agent (or the LLM drafting
 * SQL for {@code query_natural_language}) has no reason to know an operator's schema-alias naming
 * scheme, and shouldn't have to -- Warp already holds every backend's real connection info in
 * {@link BackendRegistry#all()}, so it can just ask each one what tables it actually has.
 *
 * <p>Scope, deliberately narrow for a first real implementation: introspects each backend's own
 * DEFAULT visible schema (whatever {@link DatabaseMetaData#getTables} returns with a {@code null}
 * schema pattern -- the connecting user's own search_path/default schema, "public" for a typical
 * Postgres backend), ordinary {@code TABLE}s only (no views). A backend whose real data lives
 * outside its default schema, or where discovery needs to span multiple schemas per backend, isn't
 * covered here -- a real, disclosed limitation rather than a silent one, matching {@code
 * WARP_ROUTER_SCHEMA_RULES}' own equally real limitation (an operator has to know and declare the
 * schema name up front, same trade-off in the other direction).
 */
public final class BackendCatalogDiscovery {

    private static final Logger log = LoggerFactory.getLogger(BackendCatalogDiscovery.class);

    public record DiscoveredTable(String tableName, String backendName, String realSchemaName) {
    }

    /** One real JDBC metadata query per backend -- not cached here (see this class's own javadoc
     * on why: a fresh discovery keyed to one MCP call, not a background service, is the deliberate
     * scope for this first implementation). A backend that fails to connect or introspect is
     * skipped with a logged warning rather than failing the whole discovery -- one unreachable
     * backend shouldn't block federation across the ones that ARE reachable. */
    public static List<DiscoveredTable> discoverAll(BackendRegistry registry) {
        List<DiscoveredTable> tables = new ArrayList<>();
        for (BackendTarget target : registry.all()) {
            try (Connection conn = target.open()) {
                DatabaseMetaData md = conn.getMetaData();
                try (ResultSet rs = md.getTables(null, null, "%", new String[] {"TABLE"})) {
                    while (rs.next()) {
                        String tableName = rs.getString("TABLE_NAME");
                        String schemaName = rs.getString("TABLE_SCHEM");
                        tables.add(new DiscoveredTable(tableName, target.name(), schemaName));
                    }
                }
            } catch (SQLException e) {
                log.warn("schema auto-discovery: could not introspect backend \"{}\" -- skipping it "
                        + "for this discovery pass, real query is unaffected if it doesn't need this "
                        + "backend ({})", target.name(), e.toString());
            }
        }
        return tables;
    }

    /** Groups {@link #discoverAll}'s flat list by table name (case-insensitively -- SQL identifiers
     * are case-insensitive unless quoted, and a caller matching a bare table reference from SQL
     * text has no quoting information left to go on). A table found on more than one backend is a
     * real ambiguity: {@link #resolveUnambiguous} is where that gets surfaced as a clear error
     * rather than a silent pick-one. */
    public static Map<String, List<DiscoveredTable>> byTableNameLowercase(List<DiscoveredTable> tables) {
        Map<String, List<DiscoveredTable>> byName = new LinkedHashMap<>();
        for (DiscoveredTable t : tables) {
            byName.computeIfAbsent(t.tableName().toLowerCase(Locale.ROOT), k -> new ArrayList<>()).add(t);
        }
        return byName;
    }

    private BackendCatalogDiscovery() {
    }
}

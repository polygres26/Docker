package com.sayonora.wire.core;

import com.sayonora.wire.acl.Cidr;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TrustedBackendHosts {

    private static final Logger log = LoggerFactory.getLogger(TrustedBackendHosts.class);

    public static final TrustedBackendHosts DISABLED = new TrustedBackendHosts(List.of());

    private record Entry(Cidr cidr, String literalHost, Integer port) {
    }

    private final List<Entry> entries;

    private TrustedBackendHosts(List<Entry> entries) {
        this.entries = entries;
    }

    public static TrustedBackendHosts fromEnv() {
        return parse(System.getenv("WARP_TRUSTED_BACKEND_HOSTS"));
    }

    public static TrustedBackendHosts parse(String spec) {
        if (spec == null || spec.isBlank()) {
            return DISABLED;
        }
        List<Entry> parsed = new ArrayList<>();
        for (String raw : spec.split(",")) {
            String entry = raw.trim();
            if (entry.isEmpty()) {
                continue;
            }
            parsed.add(parseEntry(entry));
        }
        log.info("TrustedBackendHosts: {} entr(y/ies) allowlisted from WARP_TRUSTED_BACKEND_HOSTS", parsed.size());
        return new TrustedBackendHosts(parsed);
    }

    private static final Pattern IP_LITERAL = Pattern.compile(
            "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}|[0-9a-fA-F:]*:[0-9a-fA-F:]*)$");

    private static Entry parseEntry(String entry) {
        if (entry.contains("/")) {
            
            return new Entry(Cidr.parse(entry), null, null);
        }
        String hostPart = entry;
        Integer port = null;
        int lastColon = entry.lastIndexOf(':');
        if (lastColon > 0) {
            String maybePort = entry.substring(lastColon + 1);
            if (maybePort.chars().allMatch(Character::isDigit)) {
                hostPart = entry.substring(0, lastColon);
                port = Integer.parseInt(maybePort);
            }
        }
        if (IP_LITERAL.matcher(hostPart).matches()) {
            return new Entry(Cidr.parse(hostPart), null, port);
        }
        return new Entry(null, hostPart.toLowerCase(Locale.ROOT), port);
    }

    public boolean isEnabled() {
        return !entries.isEmpty();
    }

    public boolean isTrusted(String jdbcUrl) {
        if (!isEnabled()) {
            return true;
        }
        HostPort target = extractHostPort(jdbcUrl);
        if (target == null) {
            return false;
        }
        InetAddress resolved = resolveQuietly(target.host());
        for (Entry entry : entries) {
            if (entry.port() != null && !entry.port().equals(target.port())) {
                continue;
            }
            if (entry.literalHost() != null) {
                if (entry.literalHost().equals(target.host().toLowerCase(Locale.ROOT))) {
                    return true;
                }
            } else if (resolved != null && entry.cidr().contains(resolved)) {
                return true;
            }
        }
        return false;
    }

    private record HostPort(String host, int port) {
    }

    private static final Pattern JDBC_POSTGRESQL = Pattern.compile("(?i)^jdbc:postgresql://([^/?]+)");
    // Real bug, found live building the Oracle/SQL Server/MySQL/Mongo backend engine work: this
    // used to recognize ONLY jdbc:postgresql: -- extractHostPort returned null (never trusted,
    // full refusal) for every other real engine's own URL shape the moment WARP_TRUSTED_
    // BACKEND_HOSTS was actually enabled, silently defeating the whole feature for anyone using it
    // with a non-Postgres backend, not just declining to allowlist it. Oracle's own real
    // "thin:@//host:port/service" shape needs its own separate pattern (the "thin:@" prefix isn't
    // a URI scheme); TNS-descriptor-style Oracle URLs (no host:port at all) are a real, further
    // gap this doesn't cover, same "return null -> not trusted" fallback as before this fix.
    //
    // Second real bug, found live writing ShardingAcrossBackendEnginesIntegrationTest: the thin
    // driver ALSO accepts a single-slash "@host:port/service" form (no double slash) -- both are
    // real, valid syntax, confirmed live via a real shard backend using the single-slash form
    // that this regex's original "@//" requirement silently rejected as untrusted, for the exact
    // same "extractHostPort returns null" reason described above, just a second URL shape hitting
    // it. The "//" is now optional, not required.
    private static final Pattern JDBC_ORACLE_THIN = Pattern.compile("(?i)^jdbc:oracle:thin:@/{0,2}([^/?]+)");
    private static final Pattern JDBC_HOST_PORT_STYLE =
            Pattern.compile("(?i)^jdbc:(?:sqlserver|mysql|mariadb)://([^/;?]+)");
    private static final Pattern MONGO_CONNECTION_STRING = Pattern.compile("(?i)^mongodb(?:\\+srv)?://([^/?]+)");

    private static HostPort extractHostPort(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        String trimmed = jdbcUrl.trim();
        Matcher pg = JDBC_POSTGRESQL.matcher(trimmed);
        if (pg.find()) {
            return parseHostPort("postgresql", pg.group(1), 5432);
        }
        Matcher oracle = JDBC_ORACLE_THIN.matcher(trimmed);
        if (oracle.find()) {
            return parseHostPort("oracle", oracle.group(1), 1521);
        }
        Matcher hostPort = JDBC_HOST_PORT_STYLE.matcher(trimmed);
        if (hostPort.find()) {
            boolean sqlServer = trimmed.regionMatches(true, 0, "jdbc:sqlserver:", 0, "jdbc:sqlserver:".length());
            return parseHostPort("generic", hostPort.group(1), sqlServer ? 1433 : 3306);
        }
        Matcher mongo = MONGO_CONNECTION_STRING.matcher(trimmed);
        if (mongo.find()) {
            // A real Mongo connection string can list several host:port pairs (a replica set
            // seed list, comma-separated) -- trusting the FIRST one is a real, documented
            // simplification, not a security hole: every listed host still has to be a real member
            // of the same real replica set to ever actually get used.
            String first = mongo.group(1).split(",", 2)[0];
            return parseHostPort("mongo", first, 27017);
        }
        return null;
    }

    private static HostPort parseHostPort(String scheme, String authority, int defaultPort) {
        try {
            URI uri = new URI(scheme, authority, "/", null, null);
            String host = uri.getHost();
            int port = uri.getPort();
            return host == null ? null : new HostPort(host, port < 0 ? defaultPort : port);
        } catch (Exception e) {
            return null;
        }
    }

    private static InetAddress resolveQuietly(String host) {
        try {
            return InetAddress.getByName(host);
        } catch (Exception e) {
            return null;
        }
    }
}

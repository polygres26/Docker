package com.polygres.wire.core;

import com.polygres.wire.acl.Cidr;
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
        return parse(System.getenv("POLYWIRE_TRUSTED_BACKEND_HOSTS"));
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
        log.info("TrustedBackendHosts: {} entr(y/ies) allowlisted from POLYWIRE_TRUSTED_BACKEND_HOSTS", parsed.size());
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

    private static HostPort extractHostPort(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        Matcher m = JDBC_POSTGRESQL.matcher(jdbcUrl.trim());
        if (!m.find()) {
            return null;
        }
        try {
            URI uri = new URI("postgresql", m.group(1), "/", null, null);
            String host = uri.getHost();
            int port = uri.getPort();
            return host == null ? null : new HostPort(host, port < 0 ? 5432 : port);
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

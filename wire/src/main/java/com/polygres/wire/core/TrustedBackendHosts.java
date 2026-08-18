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

/**
 * The explicit "only invited Postgres backends can be registered" allowlist for {@link
 * BackendRegistry}'s {@code POLYWIRE_BACKENDS}/{@code POLYWIRE_SHARD_BACKENDS} entries -- closes a
 * real backend-poisoning gap: those two settings live in {@code polywire_config.backends}/{@code
 * shardBackends}, which is hot-reloadable straight from a Postgres table (see {@code
 * PolyWireConfig}'s javadoc). Anyone with write access to that table could otherwise register an
 * arbitrary host and have real client traffic routed to it -- a classic SSRF-via-config vector,
 * the same class of risk egress allowlists/IAM resource policies/{@code NetworkPolicy} all exist
 * to close.
 *
 * <p><b>Deliberately {@code POLYWIRE_TRUSTED_BACKEND_HOSTS} — env var only, never also settable
 * via {@code polywire_config}</b>: this is a real trust boundary, not ordinary tunable config. If
 * the allowlist itself lived in the same DB-writable table as the thing it's supposed to gate, the
 * whole protection would be circular -- anyone who could poison {@code backends} could just as
 * easily poison the allowlist to include their own rogue host first. Requiring actual deploy-time/
 * infrastructure access (an env var, set by whoever controls the container/process) to change the
 * allowlist, versus SQL write access to a table, is the entire point.
 *
 * <p>Unset means every host is trusted -- zero behavior change from before this feature existed,
 * same "opt-in, default unchanged" convention as every other feature added this session. The
 * {@code ORAPG_PG_*} config-primary backend ({@link BackendRegistry#DEFAULT_BACKEND_NAME}) is
 * never checked against this list at all -- see {@link BackendRegistry#fromConfig}'s call site --
 * it's the operator's own env-var-configured connection, already inherently trusted since it's how
 * they told PolyWire to reach its own backend at deploy time; only the DB-writable {@code
 * POLYWIRE_BACKENDS}-style surface needs gating.
 *
 * <p><b>Entries</b> ({@code ,}-separated), reusing {@link Cidr} (the same IP/CIDR matcher {@code
 * ClientAcl} already uses) rather than a second implementation:
 * <ul>
 *   <li>{@code 10.0.0.0/8} — a CIDR block, any port. Matched against the target's resolved IP
 *   address (a real DNS lookup if the backend's own host is itself a hostname, not a literal IP).</li>
 *   <li>{@code 10.0.1.5:5432} — a single IP with a port restriction.</li>
 *   <li>{@code backend-primary.internal:5432} — a literal hostname with a port restriction,
 *   matched as an exact case-insensitive string against the backend's own configured host, no DNS
 *   resolution involved (the common case for docker-compose service names/internal DNS records
 *   that a CIDR block can't express).</li>
 *   <li>{@code backend-primary.internal} — same, any port.</li>
 * </ul>
 *
 * <p><b>Narrow-slice, stated plainly</b>: an IPv6 literal with a port restriction (bracket
 * notation, {@code [::1]:5432}) isn't supported -- IPv6 addresses contain colons themselves, so a
 * naive last-colon split can't distinguish a real port suffix from part of the address. Give an
 * IPv6 entry as a bare address/CIDR (any port) instead; the practical cases this feature targets
 * (docker-compose service names, IPv4 CIDR ranges) don't need bracket notation.
 */
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

    // Deliberately checked before ever calling Cidr.parse (which calls InetAddress.getByName --
    // for a *hostname* string, that performs a real DNS lookup, not just format validation; javadoc
    // confirms only a literal numeric address is checked locally with no network round-trip). A
    // hostname entry that happens to currently resolve would otherwise silently become a pinned-IP
    // CIDR match instead of the intended literal-string match against the backend's own configured
    // host -- this regex keeps parsing this allowlist itself free of any DNS dependency.
    private static final Pattern IP_LITERAL = Pattern.compile(
            "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}|[0-9a-fA-F:]*:[0-9a-fA-F:]*)$");

    private static Entry parseEntry(String entry) {
        if (entry.contains("/")) {
            // CIDR block -- never carries a port restriction in this grammar (kept simple/
            // unambiguous rather than inventing a "CIDR:port" notation nothing else here uses).
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

    /**
     * True if {@code jdbcUrl}'s {@code host:port} is allowlisted (or the allowlist is disabled).
     * A URL this can't parse as a {@code jdbc:postgresql://host:port/...} shape is treated as
     * <b>untrusted</b> when the allowlist is enabled -- fail closed, matching this class's own
     * purpose, rather than silently letting a malformed entry through unexamined.
     */
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
            return null; // a literal-hostname allowlist entry can still match without this
        }
    }
}

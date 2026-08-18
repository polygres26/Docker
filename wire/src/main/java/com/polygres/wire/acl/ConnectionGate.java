package com.polygres.wire.acl;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The one place every accept loop (TCP) and HTTP handler (dynamowire, {@code /metrics}) asks
 * "should this connection/request be let through" -- resolves the real client address (raw socket
 * peer, or PPv2/X-Forwarded-For when this listener is fronted by a trusted load balancer) and
 * evaluates it against {@link ClientAcl}, rejecting as early as possible: before any protocol
 * handshake, any auth check, any backend connection -- same "reject without touching anything
 * downstream" posture {@code PgRoleAuthCache}/{@code CredentialStore} already established for bad
 * credentials.
 *
 * <p><b>Trusting PPv2/X-Forwarded-For is itself a security decision</b> -- both are values the
 * immediate peer *claims*, not something PolyWire independently verifies. Trusted-proxies (a CIDR
 * list, blank/unset = trust any immediate peer) gates both: PPv2 is only parsed (TCP), and
 * {@code X-Forwarded-For} is only honored (HTTP), when the raw socket's own immediate peer is
 * itself inside this list -- otherwise the raw peer address is used directly and any claimed
 * header is ignored, the same "only trust a header from a known-good upstream" posture as nginx's
 * {@code set_real_ip_from} / Envoy's trusted-hops config. Left unset, a listener with PPv2 enabled
 * trusts whichever peer actually connects to it (matches enabling PPv2 being an explicit opt-in
 * already assuming the operator has firewalled the port to only the real load balancer) --
 * setting it adds a second, defense-in-depth check.
 *
 * <p><b>Config source</b>: {@code POLYWIRE_ACL_PPV2_ENABLED}/{@code POLYWIRE_ACL_TRUSTED_PROXIES}
 * (bootstrap default) or {@code polywire_config.aclPpv2Enabled}/{@code aclTrustedProxies}
 * (hot-reloadable -- see {@link #reload}, called from {@code Main}'s config-apply callback). The
 * underlying {@link ClientAcl} reloads independently through its own {@link ClientAcl#reload} --
 * this class just holds a reference to it, so an ACL-rules-only change doesn't need to touch this
 * class at all.
 */
public final class ConnectionGate {

    private static final Logger log = LoggerFactory.getLogger(ConnectionGate.class);

    /** No ACL rules, no PPv2 -- every connection allowed, zero behavior change from before this feature existed. A plain default value, never itself reloaded -- see {@link ClientAcl}'s class javadoc for the same reasoning. */
    public static final ConnectionGate DISABLED = new ConnectionGate(ClientAcl.DISABLED, false, List.of());

    private final ClientAcl acl;
    // volatile, not final -- see ClientAcl's identical "rules" field javadoc for why: a reload
    // (from Main's polywire_config LISTEN callback) and a concurrent connection's own read both
    // need to see consistent values, never a torn combination, without a lock on the hot path.
    private volatile boolean ppv2Enabled;
    private volatile List<Cidr> trustedProxies;

    private ConnectionGate(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        this.acl = acl;
        this.ppv2Enabled = ppv2Enabled;
        this.trustedProxies = trustedProxies;
    }

    /**
     * Builds a real, independently-reloadable instance -- unlike {@link #fromEnv}, never returns
     * the shared {@link #DISABLED} constant, even when {@code acl} has no rules and PPv2 is off;
     * {@code Main} uses this (not {@link #fromEnv}) for the one instance it shares across every
     * frontend and later reloads.
     */
    public static ConnectionGate create(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        return new ConnectionGate(acl, ppv2Enabled, List.copyOf(trustedProxies));
    }

    /** Exposed for {@code com.polygres.wire.grpc.PolyWireGrpcServer} to build its own PPv2-capable protocol negotiator from the same config, without re-parsing the env vars separately. */
    public ClientAcl acl() {
        return acl;
    }

    public boolean ppv2Enabled() {
        return ppv2Enabled;
    }

    public List<Cidr> trustedProxies() {
        return trustedProxies;
    }

    /** Swaps in freshly-parsed ppv2Enabled/trustedProxies -- the {@link #acl()} reference itself reloads independently via {@link ClientAcl#reload}. */
    public void reload(boolean ppv2Enabled, List<Cidr> trustedProxies) {
        this.ppv2Enabled = ppv2Enabled;
        this.trustedProxies = List.copyOf(trustedProxies);
        log.info("ConnectionGate: reloaded ppv2Enabled={}, trustedProxies={} entries", ppv2Enabled, trustedProxies.size());
    }

    public static ConnectionGate fromEnv() {
        ClientAcl acl = ClientAcl.fromEnv();
        boolean ppv2Enabled = "true".equalsIgnoreCase(System.getenv("POLYWIRE_ACL_PPV2_ENABLED"));
        if (!acl.hasRules() && !ppv2Enabled) {
            return DISABLED;
        }
        List<Cidr> trustedProxies = parseTrustedProxies(System.getenv("POLYWIRE_ACL_TRUSTED_PROXIES"));
        log.info("ConnectionGate: acl={}, ppv2Enabled={}, trustedProxies={} entries",
                acl.hasRules() ? "enabled" : "disabled", ppv2Enabled, trustedProxies.size());
        return new ConnectionGate(acl, ppv2Enabled, trustedProxies);
    }

    public static List<Cidr> parseTrustedProxies(String spec) {
        List<Cidr> trustedProxies = new ArrayList<>();
        if (spec != null && !spec.isBlank()) {
            for (String entry : spec.split(",")) {
                if (!entry.isBlank()) {
                    trustedProxies.add(Cidr.parse(entry.trim()));
                }
            }
        }
        return trustedProxies;
    }

    /**
     * TCP accept-loop entry point: reads PPv2 (if enabled), evaluates the ACL, and closes+returns
     * false if rejected. Callers must not proceed to construct a session handler when this returns
     * false -- the socket is already closed.
     */
    public boolean acceptTcp(Socket socket) {
        if (this == DISABLED) {
            return true;
        }
        boolean currentPpv2Enabled = ppv2Enabled; // one volatile read each -- see field javadoc
        List<Cidr> currentTrustedProxies = trustedProxies;
        InetAddress rawPeer = socket.getInetAddress();
        InetAddress effectiveClient = rawPeer;
        try {
            if (currentPpv2Enabled) {
                if (!currentTrustedProxies.isEmpty() && !matchesAny(rawPeer, currentTrustedProxies)) {
                    log.warn("ACL: rejecting connection from {} -- PPv2 is enabled on this listener but this peer "
                            + "is not in POLYWIRE_ACL_TRUSTED_PROXIES", rawPeer);
                    closeQuietly(socket);
                    return false;
                }
                ProxyProtocolV2.Result header = ProxyProtocolV2.readHeader(socket.getInputStream());
                // LOCAL command (health check) or an unresolvable family -- no real client to
                // extract, fall back to the raw peer exactly as if PPv2 were disabled for this one
                // connection, same as ConnectionGate would do without PPv2 at all.
                effectiveClient = header.sourceAddress().orElse(rawPeer);
            }
        } catch (IOException e) {
            log.warn("ACL: rejecting connection from {} -- {}", rawPeer, e.getMessage());
            closeQuietly(socket);
            return false;
        }
        if (!acl.isAllowed(effectiveClient)) {
            log.warn("ACL: rejecting connection from {}", effectiveClient);
            closeQuietly(socket);
            return false;
        }
        return true;
    }

    /** HTTP handler entry point (dynamowire, {@code /metrics}) -- no side effects, caller writes its own 403. */
    public boolean acceptHttp(HttpServletRequest request) {
        if (this == DISABLED) {
            return true;
        }
        List<Cidr> currentTrustedProxies = trustedProxies; // one volatile read -- see field javadoc
        InetAddress rawPeer = parseQuietly(request.getRemoteAddr());
        InetAddress effectiveClient = rawPeer;
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank() && rawPeer != null
                && (currentTrustedProxies.isEmpty() || matchesAny(rawPeer, currentTrustedProxies))) {
            // Leftmost entry is the original client in the conventional (single-trusted-hop)
            // X-Forwarded-For chain shape -- a narrower reading than nginx's full trusted-hops
            // walk, adequate for one load balancer in front, not a chain of several.
            InetAddress claimed = parseQuietly(forwardedFor.split(",")[0].trim());
            if (claimed != null) {
                effectiveClient = claimed;
            }
        }
        if (effectiveClient == null) {
            log.warn("ACL: rejecting request -- could not resolve a client address to evaluate (remoteAddr={})",
                    request.getRemoteAddr());
            return false; // fail closed -- an ACL is configured and we can't confidently evaluate it
        }
        boolean allowed = acl.isAllowed(effectiveClient);
        if (!allowed) {
            log.warn("ACL: rejecting request from {}", effectiveClient);
        }
        return allowed;
    }

    private static boolean matchesAny(InetAddress address, List<Cidr> cidrs) {
        for (Cidr cidr : cidrs) {
            if (cidr.contains(address)) {
                return true;
            }
        }
        return false;
    }

    private static InetAddress parseQuietly(String address) {
        if (address == null || address.isBlank()) {
            return null;
        }
        try {
            return InetAddress.getByName(address);
        } catch (IOException e) {
            return null;
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // already rejecting the connection -- nothing more to do
        }
    }
}

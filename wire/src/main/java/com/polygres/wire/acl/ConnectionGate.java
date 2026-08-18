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
 * immediate peer *claims*, not something PolyWire independently verifies. {@code
 * POLYWIRE_ACL_TRUSTED_PROXIES} (a CIDR list, blank/unset = trust any immediate peer) gates both:
 * PPv2 is only parsed (TCP), and {@code X-Forwarded-For} is only honored (HTTP), when the raw
 * socket's own immediate peer is itself inside this list -- otherwise the raw peer address is used
 * directly and any claimed header is ignored, the same "only trust a header from a known-good
 * upstream" posture as nginx's {@code set_real_ip_from} / Envoy's trusted-hops config. Left unset,
 * a listener with PPv2 enabled trusts whichever peer actually connects to it (matches enabling
 * PPv2 being an explicit opt-in already assuming the operator has firewalled the port to only the
 * real load balancer) -- setting it adds a second, defense-in-depth check.
 */
public final class ConnectionGate {

    private static final Logger log = LoggerFactory.getLogger(ConnectionGate.class);

    /** No ACL rules, no PPv2 -- every connection allowed, zero behavior change from before this feature existed. */
    public static final ConnectionGate DISABLED = new ConnectionGate(ClientAcl.DISABLED, false, List.of());

    private final ClientAcl acl;
    private final boolean ppv2Enabled;
    private final List<Cidr> trustedProxies;

    private ConnectionGate(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        this.acl = acl;
        this.ppv2Enabled = ppv2Enabled;
        this.trustedProxies = trustedProxies;
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

    public static ConnectionGate fromEnv() {
        ClientAcl acl = ClientAcl.fromEnv();
        boolean ppv2Enabled = "true".equalsIgnoreCase(System.getenv("POLYWIRE_ACL_PPV2_ENABLED"));
        if (acl == ClientAcl.DISABLED && !ppv2Enabled) {
            return DISABLED;
        }
        List<Cidr> trustedProxies = new ArrayList<>();
        String trustedSpec = System.getenv("POLYWIRE_ACL_TRUSTED_PROXIES");
        if (trustedSpec != null && !trustedSpec.isBlank()) {
            for (String entry : trustedSpec.split(",")) {
                if (!entry.isBlank()) {
                    trustedProxies.add(Cidr.parse(entry.trim()));
                }
            }
        }
        log.info("ConnectionGate: acl={}, ppv2Enabled={}, trustedProxies={} entries",
                acl == ClientAcl.DISABLED ? "disabled" : "enabled", ppv2Enabled, trustedProxies.size());
        return new ConnectionGate(acl, ppv2Enabled, trustedProxies);
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
        InetAddress rawPeer = socket.getInetAddress();
        InetAddress effectiveClient = rawPeer;
        try {
            if (ppv2Enabled) {
                if (!trustedProxies.isEmpty() && !matchesAny(rawPeer, trustedProxies)) {
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
        InetAddress rawPeer = parseQuietly(request.getRemoteAddr());
        InetAddress effectiveClient = rawPeer;
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank() && rawPeer != null
                && (trustedProxies.isEmpty() || matchesAny(rawPeer, trustedProxies))) {
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

package com.nexagres.wire.acl;

import com.nexagres.wire.license.License;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectionGate {

    private static final Logger log = LoggerFactory.getLogger(ConnectionGate.class);

    public static final ConnectionGate DISABLED = new ConnectionGate(ClientAcl.DISABLED, false, List.of());

    private final ClientAcl acl;

    private volatile boolean ppv2Enabled;
    private volatile List<Cidr> trustedProxies;

    // License-tier connection cap -- deliberately independent of the `this == DISABLED` shortcut
    // both acceptTcp/acceptHttp start with: a Developer-tier instance with no ACL rules configured
    // at all must still be capped, so this check can't live behind "ACL is disabled, allow
    // everything." Scoped to acceptTcp only (pgwire/mywire/mssqlwire/orawire/mongowire -- the real
    // persistent, session-oriented connections) -- deliberately NOT applied in acceptHttp, since
    // dynamowire/sqswire/oswire/the admin API/MCP are stateless HTTP request/response, where
    // "concurrent connections" isn't the meaningful unit the pricing plan's cap describes. Every
    // accepted TCP session must eventually call release() exactly once (Main.java wraps each
    // submitted session Runnable in try/finally to guarantee this) or the count only ever grows.
    private final AtomicInteger liveConnections = new AtomicInteger(0);

    private ConnectionGate(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        this.acl = acl;
        this.ppv2Enabled = ppv2Enabled;
        this.trustedProxies = trustedProxies;
    }

    public static ConnectionGate create(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        return new ConnectionGate(acl, ppv2Enabled, List.copyOf(trustedProxies));
    }

    /** Call exactly once for every {@link #acceptTcp} that returned {@code true}, once that
     * session's socket has actually closed -- Main.java does this via a try/finally around each
     * submitted session {@code Runnable}, not inside any individual session handler class, so no
     * per-protocol session handler needed to change to get this enforced uniformly. */
    public void release() {
        liveConnections.decrementAndGet();
    }

    public ClientAcl acl() {
        return acl;
    }

    public boolean ppv2Enabled() {
        return ppv2Enabled;
    }

    public List<Cidr> trustedProxies() {
        return trustedProxies;
    }

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

    public boolean acceptTcp(Socket socket) {
        int max = License.current().maxConnectionsPerInstance();
        // Optimistic increment-then-check (not check-then-increment) so two concurrent accepts
        // racing the last free slot can't both read "24, still room" and both proceed -- whichever
        // loses the race sees its own increment push the count over max and backs out via
        // decrementAndGet(), not the other thread's.
        int afterIncrement = liveConnections.incrementAndGet();
        if (afterIncrement > max) {
            log.warn("license: rejecting connection from {} -- Developer edition is capped at {} "
                    + "concurrent connections per instance (see the Pricing section of the docs "
                    + "for Enterprise, which has no connection limit)", socket.getInetAddress(), max);
            return rejectTcp(socket);
        }
        if (this == DISABLED) {
            return true;
        }
        boolean currentPpv2Enabled = ppv2Enabled;
        List<Cidr> currentTrustedProxies = trustedProxies;
        InetAddress rawPeer = socket.getInetAddress();
        InetAddress effectiveClient = rawPeer;
        try {
            if (currentPpv2Enabled) {
                if (!currentTrustedProxies.isEmpty() && !matchesAny(rawPeer, currentTrustedProxies)) {
                    log.warn("ACL: rejecting connection from {} -- PPv2 is enabled on this listener but this peer "
                            + "is not in POLYWIRE_ACL_TRUSTED_PROXIES", rawPeer);
                    return rejectTcp(socket);
                }
                ProxyProtocolV2.Result header = ProxyProtocolV2.readHeader(socket.getInputStream());

                effectiveClient = header.sourceAddress().orElse(rawPeer);
            }
        } catch (IOException e) {
            log.warn("ACL: rejecting connection from {} -- {}", rawPeer, e.getMessage());
            return rejectTcp(socket);
        }
        if (!acl.isAllowed(effectiveClient)) {
            log.warn("ACL: rejecting connection from {}", effectiveClient);
            return rejectTcp(socket);
        }
        return true;
    }

    public boolean acceptHttp(HttpServletRequest request) {
        if (this == DISABLED) {
            return true;
        }
        List<Cidr> currentTrustedProxies = trustedProxies;
        InetAddress rawPeer = parseQuietly(request.getRemoteAddr());
        InetAddress effectiveClient = rawPeer;
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if (forwardedFor != null && !forwardedFor.isBlank() && rawPeer != null
                && (currentTrustedProxies.isEmpty() || matchesAny(rawPeer, currentTrustedProxies))) {
            
            InetAddress claimed = parseQuietly(forwardedFor.split(",")[0].trim());
            if (claimed != null) {
                effectiveClient = claimed;
            }
        }
        if (effectiveClient == null) {
            log.warn("ACL: rejecting request -- could not resolve a client address to evaluate (remoteAddr={})",
                    request.getRemoteAddr());
            return false;
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

    /** Every rejection path in {@link #acceptTcp} routes through here -- a socket that's rejected
     * never gets a session, so it must never hold a slot in {@link #liveConnections} either.
     * Always returns {@code false}, so a caller can just {@code return rejectTcp(socket);}. */
    private boolean rejectTcp(Socket socket) {
        liveConnections.decrementAndGet();
        closeQuietly(socket);
        return false;
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            
        }
    }
}

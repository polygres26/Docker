package com.polygres.wire.acl;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ConnectionGate {

    private static final Logger log = LoggerFactory.getLogger(ConnectionGate.class);

    public static final ConnectionGate DISABLED = new ConnectionGate(ClientAcl.DISABLED, false, List.of());

    private final ClientAcl acl;
    
    private volatile boolean ppv2Enabled;
    private volatile List<Cidr> trustedProxies;

    private ConnectionGate(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        this.acl = acl;
        this.ppv2Enabled = ppv2Enabled;
        this.trustedProxies = trustedProxies;
    }

    public static ConnectionGate create(ClientAcl acl, boolean ppv2Enabled, List<Cidr> trustedProxies) {
        return new ConnectionGate(acl, ppv2Enabled, List.copyOf(trustedProxies));
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
                    closeQuietly(socket);
                    return false;
                }
                ProxyProtocolV2.Result header = ProxyProtocolV2.readHeader(socket.getInputStream());
                
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

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            
        }
    }
}

package com.sayonora.warp.tls;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a Jetty {@link Server} with a plaintext connector and (when TLS is configured) an additional HTTPS connector.
 * Follow-up HTTP frontends use it as a one-line replacement for {@code new Server(port)}:
 * <pre>{@code Server s = TlsListeners.jetty("A2A", port, 18444, System.getenv()).server();}</pre>
 *
 * <p>Environment for listener {@code NAME}: {@code WARP_<NAME>_HTTPS_PORT} (default given by the caller),
 * {@code WARP_<NAME>_HTTP_DISABLED=true} (no plaintext connector), and the TLS material variables described on
 * {@link TlsSettings}. A TLS misconfiguration never stops Warp: it is logged loudly, HTTPS stays off, and the plaintext
 * connector keeps working (unless it was explicitly disabled, in which case startup fails: refusing to silently serve
 * nothing is safer than silently serving plaintext against the operator's stated policy).
 */
public final class TlsListeners {

    private static final Logger log = LoggerFactory.getLogger(TlsListeners.class);
    private static final Map<String, Info> INFOS = new ConcurrentHashMap<>();

    private TlsListeners() {
    }

    /** What a listener ended up serving; {@code httpPort}/{@code httpsPort} are -1 when that scheme is off. */
    public record Info(String name, int httpPort, int httpsPort, boolean tlsEnabled, boolean selfSigned,
            boolean clientAuth, String tlsError, String certSubject, String certNotAfter) {
    }

    public record Listener(Server server, Info info) {
    }

    public static Info info(String name) {
        return INFOS.get(name.toUpperCase(Locale.ROOT));
    }

    public static Listener jetty(String name, int httpPort, int defaultHttpsPort, Map<String, String> env) {
        String n = name.toUpperCase(Locale.ROOT);
        boolean httpDisabled = "true".equalsIgnoreCase(env.get("WARP_" + n + "_HTTP_DISABLED"));
        int httpsPort = defaultHttpsPort;
        String hp = env.get("WARP_" + n + "_HTTPS_PORT");
        if (hp != null && !hp.isBlank()) {
            try {
                httpsPort = Integer.parseInt(hp.trim());
            } catch (NumberFormatException e) {
                throw new TlsException("WARP_" + n + "_HTTPS_PORT must be an integer (got \"" + hp + "\")");
            }
        }
        TlsProvider provider = null;
        String error = null;
        try {
            TlsSettings settings = TlsSettings.resolve(n, env);
            if (settings != null) {
                provider = TlsProvider.get(settings);
            }
        } catch (RuntimeException e) {
            error = e instanceof TlsException ? e.getMessage() : e.toString();
            log.error("{} HTTPS is NOT enabled: {}. {}", n, error, httpDisabled
                    ? "Plaintext is disabled by request, so this listener cannot start."
                    : "Plaintext HTTP keeps working on port " + httpPort + ".");
            if (httpDisabled) {
                throw new TlsException(n + ": WARP_" + n + "_HTTP_DISABLED=true but TLS could not be set up: " + error, e);
            }
        }
        Server server = new Server();
        if (!httpDisabled) {
            ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(new HttpConfiguration()));
            http.setPort(httpPort);
            server.addConnector(http);
        } else if (provider == null) {
            throw new TlsException(n + ": WARP_" + n + "_HTTP_DISABLED=true but no TLS material is configured "
                    + "(set WARP_" + n + "_TLS_CERT/KEY, WARP_" + n + "_TLS_KEYSTORE or WARP_TLS_*)");
        }
        String subject = null;
        String notAfter = null;
        if (provider != null) {
            HttpConfiguration cfg = new HttpConfiguration();
            cfg.addCustomizer(new SecureRequestCustomizer(false)); // sets scheme=https; no SNI/Host cert matching
            ServerConnector https = new ServerConnector(server,
                    new SslConnectionFactory(provider.jettyFactory(), HttpVersion.HTTP_1_1.asString()),
                    new HttpConnectionFactory(cfg));
            https.setPort(httpsPort);
            server.addConnector(https);
            subject = provider.material().leaf().getSubjectX500Principal().getName();
            notAfter = provider.material().leaf().getNotAfter().toInstant().toString();
            log.info("{} HTTPS enabled on port {} ({}{}) {}", n, httpsPort, provider.settings().origin(),
                    provider.selfSigned() ? ", SELF-SIGNED dev certificate -- clients will not trust it" : "",
                    provider.material().describe());
        }
        Info info = new Info(n, httpDisabled ? -1 : httpPort, provider == null ? -1 : httpsPort, provider != null,
                provider != null && provider.selfSigned(),
                provider != null && provider.settings().clientAuth() != TlsSettings.ClientAuth.NONE, error, subject,
                notAfter);
        INFOS.put(n, info);
        return new Listener(server, info);
    }
}

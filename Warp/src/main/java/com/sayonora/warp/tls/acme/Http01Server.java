package com.sayonora.warp.tls.acme;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The tiny dedicated http-01 listener. It serves ONLY {@code /.well-known/acme-challenge/<token>} for tokens of orders that are
 * currently in flight; everything else is 404 (or, with {@code WARP_ACME_HTTP_REDIRECT=true}, a 308 to the same path on https).
 * It stays up for the whole life of the process while ACME is enabled (a renewal is unattended and a daily check must not
 * depend on operators opening a port); it exposes nothing but random challenge tokens that exist for a few seconds.
 */
public final class Http01Server implements AcmeClient.Responder {

    private static final Logger log = LoggerFactory.getLogger(Http01Server.class);
    private static final String PREFIX = "/.well-known/acme-challenge/";
    private final Map<String, String> tokens = new ConcurrentHashMap<>();
    private final boolean redirect;
    private final String redirectPort;
    private HttpServer server;
    private volatile String bindError;

    public Http01Server(boolean redirect, String redirectPort) {
        this.redirect = redirect;
        this.redirectPort = redirectPort;
    }

    public void start(String bind, int port) {
        try {
            server = HttpServer.create(new InetSocketAddress(bind, port), 32);
            server.createContext("/", this::handle);
            server.setExecutor(Executors.newFixedThreadPool(2, r -> {
                Thread t = new Thread(r, "warp-acme-http01");
                t.setDaemon(true);
                return t;
            }));
            server.start();
            log.info("ACME http-01 challenge listener on {}:{} (serves only {}<token>)", bind, server.getAddress().getPort(), PREFIX);
        } catch (IOException | RuntimeException e) {
            bindError = "cannot listen on " + bind + ":" + port + " for http-01 (" + e.getMessage()
                    + "); use WARP_ACME_HTTP_PORT with a port mapping, or WARP_ACME_CHALLENGE=dns-01";
            log.error("ACME: {}", bindError);
        }
    }

    public int port() {
        return server == null ? -1 : server.getAddress().getPort();
    }

    /** Non-null when the listener could not start. */
    public String bindError() {
        return bindError;
    }

    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void handle(HttpExchange ex) throws IOException {
        try (ex) {
            String path = ex.getRequestURI().getRawPath();
            String method = ex.getRequestMethod();
            if (path.startsWith(PREFIX) && (method.equals("GET") || method.equals("HEAD"))) {
                String ka = tokens.get(path.substring(PREFIX.length()));
                if (ka != null) {
                    byte[] b = ka.getBytes(StandardCharsets.US_ASCII);
                    ex.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    ex.sendResponseHeaders(200, method.equals("HEAD") ? -1 : b.length);
                    if (!method.equals("HEAD")) {
                        ex.getResponseBody().write(b);
                    }
                    return;
                }
            } else if (redirect) {
                String host = ex.getRequestHeaders().getFirst("Host");
                if (host != null && host.matches("[A-Za-z0-9.\\-:\\[\\]]+")) {
                    String h = host.startsWith("[") ? host.substring(0, host.indexOf(']') + 1) : host.replaceAll(":\\d+$", "");
                    ex.getResponseHeaders().add("Location", "https://" + h + (redirectPort == null || redirectPort.equals("443") ? "" : ":" + redirectPort) + path);
                    ex.sendResponseHeaders(308, -1);
                    return;
                }
            }
            byte[] nf = "not found\n".getBytes(StandardCharsets.US_ASCII);
            ex.sendResponseHeaders(404, method.equals("HEAD") ? -1 : nf.length);
            if (!method.equals("HEAD")) {
                ex.getResponseBody().write(nf);
            }
        }
    }

    @Override
    public void present(String domain, String type, String token, String keyAuthorization) {
        if (bindError != null) {
            throw new IllegalStateException(bindError);
        }
        tokens.put(token, keyAuthorization);
    }

    @Override
    public void cleanup(String domain, String type, String token, String keyAuthorization) {
        tokens.remove(token);
    }
}

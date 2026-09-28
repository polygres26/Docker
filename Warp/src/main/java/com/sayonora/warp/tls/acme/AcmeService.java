package com.sayonora.warp.tls.acme;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sayonora.warp.server.ServerOptions;
import com.sayonora.warp.tls.TlsProvider;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in ACME (RFC 8555) certificate management. {@link #start} reads {@code WARP_ACME_*}, makes sure the state directory holds
 * a certificate + key (a short-lived self-signed placeholder until the first issuance, so every HTTPS listener can bind at
 * startup), starts the http-01 listener and a scheduler that checks daily, renews at {@code WARP_ACME_RENEW_DAYS} remaining,
 * and backs off exponentially (60 s .. 6 h, longer on rateLimited) after failures. Renewed material is written to
 * {@code fullchain.pem} / {@code privkey.pem}, which the shared {@link TlsProvider} hot-reloads: no restart. A failed renewal
 * keeps the old certificate. With a shared Postgres control plane and SAYONORA_ENCRYPTION_KEY the account, certificate and
 * key are stored encrypted in {@code warp_acme_state} and one node at a time (pg advisory lock) talks to the CA.
 */
public final class AcmeService {

    private static final Logger log = LoggerFactory.getLogger(AcmeService.class);
    private static volatile AcmeService instance;


    private final AcmeConfig cfg;
    private final String disabledReason;
    private final AcmeStore store;
    private final AcmeDb db;
    private final Http01Server http01;
    private final ScheduledExecutorService exec;
    private final ReentrantLock runLock = new ReentrantLock();
    private final HttpClient httpClient;
    private final long retryBaseSeconds;
    /** Automatic re-issue floor (a renew-days larger than the certificate lifetime must not re-order daily). */
    private final long minIssueSeconds;
    /** A manual renewal closely following another attempt is refused (ACME servers rate-limit; do not hammer them). */
    private final long manualMinSeconds;
    private final Object stateLock = new Object();

    private volatile boolean issuing;
    private volatile Instant lastRenewal;
    private volatile Instant lastAttempt;
    private volatile String lastError;
    private volatile Instant lastErrorAt;
    private volatile int failures;
    private volatile Instant backoffUntil;
    private volatile Instant nextCheck;
    private volatile String lastOutcome;
    private volatile boolean lastRateLimited;
    private volatile Instant lastManual;
    private volatile int ordersPlaced;
    private volatile String adoptedFingerprint;

    private AcmeService(AcmeConfig cfg, String disabledReason, AcmeStore store, AcmeDb db, Http01Server http01, HttpClient http) {
        this.cfg = cfg;
        this.disabledReason = disabledReason;
        this.store = store;
        this.db = db;
        this.http01 = http01;
        this.httpClient = http;
        this.exec = cfg == null ? null : Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "warp-acme");
            t.setDaemon(true);
            return t;
        });
        String rs = cfg == null ? null : cfg.env().get("WARP_ACME_RETRY_SECONDS");
        this.retryBaseSeconds = rs == null || rs.isBlank() ? 60 : Long.parseLong(rs.trim());
        this.minIssueSeconds = cfg == null ? 3600 : envLong(cfg.env(), "WARP_ACME_MIN_INTERVAL_SECONDS", 3600);
        this.manualMinSeconds = cfg == null ? 300 : envLong(cfg.env(), "WARP_ACME_MANUAL_MIN_SECONDS", 300);
    }

    private static long envLong(Map<String, String> env, String k, long def) {
        String v = env.get(k);
        return v == null || v.isBlank() ? def : Long.parseLong(v.trim());
    }

    // ------------------------------------------------------------------ static wiring

    public static AcmeService instance() {
        return instance;
    }

    /** True when ACME is enabled and manages the certificate files. */
    public static boolean managing() {
        AcmeService s = instance;
        return s != null && s.cfg != null;
    }

    /** {@code [fullchain, privkey]} of the managed material, or null. */
    public static Path[] managedPaths() {
        AcmeService s = instance;
        return s == null || s.cfg == null ? null : new Path[] {s.cfg.fullchain(), s.cfg.privkey()};
    }

    /** Never throws: any problem is logged as "ACME is NOT enabled: reason" and Warp continues. */
    public static AcmeService start(ServerOptions options, Map<String, String> env) {
        AcmeConfig cfg;
        try {
            cfg = AcmeConfig.fromEnv(env, Path.of("").toAbsolutePath());
        } catch (AcmeConfig.ConfigException e) {
            return disabled(e.getMessage());
        } catch (RuntimeException e) {
            return disabled(e.toString());
        }
        if (cfg == null) {
            return null;
        }
        try {
            AcmeStore store = new AcmeStore(cfg.dir());
            store.init();
            HttpClient http = buildHttp(cfg);
            AcmeDb db = null;
            if (cfg.shared() && options != null) {
                if (env.get("SAYONORA_ENCRYPTION_KEY") == null || env.get("SAYONORA_ENCRYPTION_KEY").isBlank()) {
                    log.warn("ACME: SAYONORA_ENCRYPTION_KEY is not set, so the certificate is NOT shared through the control plane "
                            + "(each node keeps its own copy in WARP_ACME_DIR and would order its own). Set it to share one certificate.");
                } else {
                    try {
                        db = new AcmeDb(options, cfg.domains().get(0));
                        db.ensureSchema();
                    } catch (Exception e) {
                        log.warn("ACME: control-plane sharing unavailable ({}); using WARP_ACME_DIR only", e.toString());
                        db = null;
                    }
                }
            }
            Http01Server h = null;
            if (cfg.challenge() == AcmeConfig.Challenge.HTTP_01) {
                h = new Http01Server(cfg.httpRedirect(), cfg.env().get("WARP_ACME_REDIRECT_HTTPS_PORT"));
                if (cfg.httpPort() > 0) {
                    h.start(cfg.httpBind(), cfg.httpPort());
                }
            }
            AcmeService s = new AcmeService(cfg, null, store, db, h, http);
            instance = s;
            s.bootstrap();
            s.schedule(0);
            return s;
        } catch (Exception e) {
            return disabled(e.toString());
        }
    }

    private static AcmeService disabled(String reason) {
        log.error("ACME is NOT enabled: {}", reason);
        AcmeService s = new AcmeService(null, reason, null, null, null, null);
        instance = s;
        return s;
    }

    private static HttpClient buildHttp(AcmeConfig cfg) throws Exception {
        HttpClient.Builder b = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(15));
        if (cfg.caBundle() != null) {
            java.security.KeyStore ks = java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
            ks.load(null, null);
            int i = 0;
            for (X509Certificate c : com.sayonora.warp.tls.PemLoader.readCertificates(cfg.caBundle())) {
                ks.setCertificateEntry("ca" + i++, c);
            }
            javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory.getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
            ctx.init(null, tmf.getTrustManagers(), null);
            b.sslContext(ctx);
        }
        return b.build();
    }

    // ------------------------------------------------------------------ pure logic (unit tested)

    /** Does this certificate need (re)issuing for {@code domains}? */
    public static boolean needsIssuance(CertInfo cert, List<String> domains, int renewDays, Instant now) {
        if (cert == null || cert.placeholder()) {
            return true;
        }
        Set<String> want = new TreeSet<>();
        for (String d : domains) {
            want.add(d.toLowerCase(java.util.Locale.ROOT));
        }
        if (!cert.names().equals(want)) {
            return true;
        }
        return !now.plus(Duration.ofDays(renewDays)).isBefore(cert.notAfter());
    }

    /** Exponential backoff: base, 2x, 4x ... capped at 6 h; a rateLimited answer waits at least 1 h (or its Retry-After, max 24 h). */
    public static Duration backoff(int failures, long baseSeconds, boolean rateLimited, Duration retryAfter) {
        long secs = baseSeconds;
        for (int i = 1; i < failures && secs < 6 * 3600; i++) {
            secs *= 2;
        }
        secs = Math.min(secs, 6 * 3600);
        if (rateLimited) {
            long floor = retryAfter != null ? Math.min(retryAfter.getSeconds(), 24 * 3600) : 3600;
            secs = Math.max(secs, floor);
        }
        return Duration.ofSeconds(secs);
    }

    // ------------------------------------------------------------------ lifecycle

    private CertInfo currentCert() {
        String pem = store.readFullchain();
        if (pem == null) {
            return null;
        }
        try {
            List<X509Certificate> chain = AcmeCrypto.parseCertChain(pem);
            return chain.isEmpty() ? null : CertInfo.of(chain.get(0));
        } catch (RuntimeException e) {
            return null;
        }
    }

    private String certRowName() {
        return "certificate:" + cfg.domains().get(0);
    }

    private String accountRowName() {
        return "account:" + AcmeCrypto.b64url(AcmeCrypto.sha256(cfg.directory().getBytes(java.nio.charset.StandardCharsets.UTF_8))).substring(0, 16);
    }

    /** First start: restore persisted state, adopt a shared certificate, or write the placeholder so listeners can bind. */
    private void bootstrap() throws IOException {
        JsonObject st = store.readState();
        if (st.has("lastRenewal")) {
            lastRenewal = Instant.parse(st.get("lastRenewal").getAsString());
        }
        if (st.has("lastError") && !st.get("lastError").isJsonNull()) {
            lastError = st.get("lastError").getAsString();
            lastErrorAt = st.has("lastErrorAt") ? Instant.parse(st.get("lastErrorAt").getAsString()) : null;
        }
        if (st.has("failures")) {
            failures = st.get("failures").getAsInt();
        }
        if (st.has("backoffUntil") && !st.get("backoffUntil").isJsonNull()) {
            backoffUntil = Instant.parse(st.get("backoffUntil").getAsString());
        }
        adoptFromDb();
        CertInfo c = currentCert();
        if (c == null || store.readPrivkey() == null) {
            KeyPair k = AcmeCrypto.newKey(cfg.keyType());
            byte[] der = AcmeCrypto.placeholderCertificate(k, cfg.domains(), Instant.now());
            store.writeCertificate(AcmeCrypto.pem("CERTIFICATE", der), AcmeCrypto.pem("PRIVATE KEY", k.getPrivate().getEncoded()));
            log.warn("ACME: no certificate yet for {}; HTTPS serves a temporary self-signed placeholder until issuance succeeds", cfg.domains());
        } else if (c.placeholder()) {
            log.warn("ACME: still on the self-signed placeholder for {}", cfg.domains());
        } else {
            log.info("ACME: existing certificate for {} valid until {} ({} days left)", c.names(), c.notAfter(), c.daysLeft(Instant.now()));
        }
        if (db != null) {
            exec.scheduleWithFixedDelay(this::syncSafely, cfg.syncSeconds(), cfg.syncSeconds(), TimeUnit.SECONDS);
        }
    }

    private void syncSafely() {
        try {
            if (!issuing) {
                adoptFromDb();
            }
        } catch (RuntimeException e) {
            log.warn("ACME: control-plane sync failed: {}", e.toString());
        }
    }

    /** Copies a certificate another node stored in the control plane into the local files (hot reload picks it up). */
    private boolean adoptFromDb() {
        if (db == null) {
            return false;
        }
        try {
            String v = db.get(certRowName());
            if (v == null) {
                return false;
            }
            JsonObject j = JsonParser.parseString(v).getAsJsonObject();
            String chain = j.get("fullchain").getAsString();
            String key = j.get("privkey").getAsString();
            List<X509Certificate> parsed = AcmeCrypto.parseCertChain(chain);
            if (parsed.isEmpty()) {
                return false;
            }
            CertInfo remote = CertInfo.of(parsed.get(0));
            if (remote.sha256().equals(adoptedFingerprint) || Instant.now().isAfter(remote.notAfter())) {
                return false;
            }
            CertInfo local = currentCert();
            if (local != null && local.sha256().equals(remote.sha256())) {
                adoptedFingerprint = remote.sha256();
                return false;
            }
            // only adopt a certificate for our domain set that is newer than (or replaces a placeholder for) ours
            Set<String> want = new TreeSet<>(cfg.domains());
            if (!remote.names().equals(want)) {
                return false;
            }
            if (local != null && !local.placeholder() && local.names().equals(want) && !remote.notBefore().isAfter(local.notBefore())) {
                return false;
            }
            store.writeCertificate(chain, key);
            adoptedFingerprint = remote.sha256();
            TlsProvider.reloadAll();
            lastRenewal = remote.notBefore();
            log.info("ACME: adopted the shared certificate from the control plane ({} valid until {})", remote.names(), remote.notAfter());
            persistState();
            return true;
        } catch (Exception e) {
            log.warn("ACME: cannot read the shared certificate: {}", e.toString());
            return false;
        }
    }

    private void schedule(long delaySeconds) {
        nextCheck = Instant.now().plusSeconds(delaySeconds);
        exec.schedule(() -> runCheck(false, false), delaySeconds, TimeUnit.SECONDS);
    }

    /** Result of a manual renewal request. */
    public record RenewResult(boolean accepted, int httpStatus, String message, Instant retryAt) {
    }

    /** POST /api/tls/renew: force a renewal now, guarded against hammering the CA. */
    public RenewResult requestRenewal() {
        if (cfg == null) {
            return new RenewResult(false, 409, "ACME is not enabled: " + disabledReason, null);
        }
        Instant now = Instant.now();
        if (issuing) {
            return new RenewResult(false, 409, "an issuance is already in progress", null);
        }
        Instant bo = backoffUntil;
        if (bo != null && bo.isAfter(now) && lastRateLimited) {
            return new RenewResult(false, 429, "the CA rate-limited the last attempt; not retrying before " + bo, bo);
        }
        Instant lm = lastManual;
        Instant la = lastAttempt;
        Instant latest = lm == null ? la : (la == null || lm.isAfter(la) ? lm : la);
        if (latest != null && latest.plusSeconds(manualMinSeconds).isAfter(now)) {
            Instant retry = latest.plusSeconds(manualMinSeconds);
            return new RenewResult(false, 429, "the last attempt was less than " + Math.max(1, manualMinSeconds / 60)
                    + " minute(s) ago; try again after " + retry, retry);
        }
        lastManual = now;
        exec.execute(() -> runCheck(true, true));
        return new RenewResult(true, 202, "renewal started", null);
    }

    void runCheck(boolean force, boolean manual) {
        if (!runLock.tryLock()) {
            return;
        }
        long next = cfg.checkIntervalSeconds();
        try {
            adoptFromDb();
            CertInfo cur = currentCert();
            boolean need = force || needsIssuance(cur, cfg.domains(), cfg.renewDays(), Instant.now());
            if (!need) {
                lastOutcome = "certificate is current";
                next = Math.min(next, Math.max(60, Duration.between(Instant.now(), cur.notAfter().minus(Duration.ofDays(cfg.renewDays()))).getSeconds()));
                return;
            }
            if (need && !force && cur != null && !cur.placeholder() && lastRenewal != null
                    && cur.names().equals(new TreeSet<>(cfg.domains())) && Instant.now().isBefore(cur.notAfter())
                    && lastRenewal.plusSeconds(minIssueSeconds).isAfter(Instant.now())) {
                next = Math.max(5, Duration.between(Instant.now(), lastRenewal.plusSeconds(minIssueSeconds)).getSeconds());
                lastOutcome = "renewal due, but the last issuance was under " + minIssueSeconds + "s ago";
                return;
            }
            Instant bo = backoffUntil;
            if (!force && bo != null && bo.isAfter(Instant.now())) {
                next = Math.max(5, Duration.between(Instant.now(), bo).getSeconds());
                lastOutcome = "waiting out a backoff until " + bo;
                return;
            }
            if (cfg.challenge() == AcmeConfig.Challenge.HTTP_01 && http01 != null && http01.bindError() != null) {
                fail(new AcmeException(http01.bindError()), false);
                next = nextDelayAfterFailure(null);
                return;
            }
            AcmeDb.Lock lock = null;
            if (db != null) {
                try {
                    lock = db.tryLock();
                } catch (Exception e) {
                    log.warn("ACME: cannot take the control-plane lock: {}", e.toString());
                    next = 60;
                    lastOutcome = "control plane unavailable";
                    return;
                }
                if (lock == null) {
                    lastOutcome = "another node is issuing";
                    next = 30;
                    return;
                }
            }
            try {
                if (db != null && adoptFromDb() && !force) {
                    lastOutcome = "adopted the certificate another node issued";
                    return;
                }
                if (db != null && !force && !needsIssuance(currentCert(), cfg.domains(), cfg.renewDays(), Instant.now())) {
                    return;
                }
                issuing = true;
                lastAttempt = Instant.now();
                try {
                    issue();
                    failures = 0;
                    backoffUntil = null;
                    lastError = null;
                    lastErrorAt = null;
                    lastOutcome = "issued";
                    persistState();
                    CertInfo c = currentCert();
                    next = Math.min(next, Math.max(60, Duration.between(Instant.now(), c.notAfter().minus(Duration.ofDays(cfg.renewDays()))).getSeconds()));
                } catch (AcmeException e) {
                    fail(e, e.isRateLimited());
                    next = nextDelayAfterFailure(e);
                } catch (Exception e) {
                    fail(new AcmeException(e.toString(), e), false);
                    next = nextDelayAfterFailure(null);
                } finally {
                    issuing = false;
                }
            } finally {
                if (lock != null) {
                    lock.close();
                }
            }
        } catch (RuntimeException e) {
            log.error("ACME check failed unexpectedly: {}", e.toString());
        } finally {
            runLock.unlock();
            if (!exec.isShutdown()) {
                schedule(next);
            }
        }
    }

    private long nextDelayAfterFailure(AcmeException e) {
        Instant bo = backoffUntil;
        return bo == null ? 60 : Math.max(5, Duration.between(Instant.now(), bo).getSeconds());
    }

    private void fail(AcmeException e, boolean rateLimited) {
        failures++;
        lastRateLimited = rateLimited;
        lastError = e.getMessage();
        lastErrorAt = Instant.now();
        Duration d = backoff(failures, retryBaseSeconds, rateLimited, e.retryAfter());
        backoffUntil = Instant.now().plus(d);
        lastOutcome = "failed";
        log.error("ACME issuance for {} failed (attempt {}): {}. Keeping the current certificate; next attempt in {}s.", cfg.domains(),
                failures, e.getMessage(), d.getSeconds());
        persistState();
    }

    private void persistState() {
        JsonObject o = new JsonObject();
        if (lastRenewal != null) {
            o.addProperty("lastRenewal", lastRenewal.toString());
        }
        if (lastError != null) {
            o.addProperty("lastError", lastError);
            o.addProperty("lastErrorAt", lastErrorAt.toString());
        }
        o.addProperty("failures", failures);
        if (backoffUntil != null) {
            o.addProperty("backoffUntil", backoffUntil.toString());
        }
        store.writeState(o);
    }

    // ------------------------------------------------------------------ issuance

    private AcmeStore.Account loadAccount() throws Exception {
        if (db != null) {
            String v = db.get(accountRowName());
            if (v != null) {
                JsonObject j = JsonParser.parseString(v).getAsJsonObject();
                return new AcmeStore.Account(AcmeCrypto.keyPairFromPem(j.get("key").getAsString()), j.has("kid") ? j.get("kid").getAsString() : null);
            }
        }
        return store.readAccount(cfg.directory());
    }

    private void saveAccount(AcmeStore.Account a) throws Exception {
        store.writeAccount(cfg.directory(), a);
        if (db != null) {
            JsonObject j = new JsonObject();
            j.addProperty("key", AcmeCrypto.keyPairPem(a.key()));
            if (a.kid() != null) {
                j.addProperty("kid", a.kid());
            }
            db.put(accountRowName(), j.toString());
        }
    }

    private AcmeClient.Responder responder() {
        if (cfg.challenge() == AcmeConfig.Challenge.HTTP_01) {
            return http01;
        }
        Map<String, String> env = cfg.env();
        DnsProvider p = switch (cfg.dnsProvider()) {
            case "hook" -> new HookDnsProvider(cfg.dnsHook(), 120);
            case "cloudflare" -> new CloudflareDnsProvider(env.getOrDefault("WARP_ACME_CLOUDFLARE_API", "https://api.cloudflare.com/client/v4"),
                    env.get("CLOUDFLARE_API_TOKEN").trim(), httpClient);
            default -> new Route53DnsProvider(env.getOrDefault("WARP_ACME_ROUTE53_ENDPOINT", "https://route53.amazonaws.com"),
                    env.get("AWS_ACCESS_KEY_ID").trim(), env.get("AWS_SECRET_ACCESS_KEY").trim(),
                    env.get("AWS_SESSION_TOKEN"), env.get("WARP_ACME_ROUTE53_ZONE_ID"), httpClient, 120);
        };
        return new DnsChallengeResponder(p, new DnsChecker(cfg.dnsCheck(), cfg.dohUrl(), httpClient), cfg.dnsWaitSeconds());
    }

    private void issue() throws Exception {
        AcmeClient client = new AcmeClient(cfg.directory(), httpClient, cfg.pollMillis(), Duration.ofSeconds(cfg.pollTimeoutSeconds()));
        AcmeStore.Account acct = loadAccount();
        if (acct == null) {
            acct = new AcmeStore.Account(AcmeCrypto.newEcKey(), null);
        }
        String chalType = cfg.challenge() == AcmeConfig.Challenge.HTTP_01 ? "http-01" : "dns-01";
        KeyPair certKey = AcmeCrypto.newKey(cfg.keyType());
        AcmeClient.Issued issued = null;
        for (int attempt = 0; attempt < 2 && issued == null; attempt++) {
            if (acct.kid() == null) {
                String kid = client.newAccount(acct.key(), cfg.email(), true, cfg.eabKid(), cfg.eabHmacKey());
                acct = new AcmeStore.Account(acct.key(), kid);
                saveAccount(acct);
                log.info("ACME: registered account {}", kid);
            }
            try {
                ordersPlaced++;
                issued = client.obtain(acct.key(), acct.kid(), cfg.domains(), chalType, certKey, responder());
            } catch (AcmeException e) {
                if (attempt == 0 && e.is("accountDoesNotExist")) {
                    log.warn("ACME: the CA does not know account {}; registering a new one", acct.kid());
                    acct = new AcmeStore.Account(acct.key(), null);
                    continue;
                }
                throw e;
            }
        }
        List<X509Certificate> chain = AcmeCrypto.parseCertChain(issued.fullchainPem());
        if (chain.isEmpty()) {
            throw new AcmeException("the CA returned no certificate");
        }
        CertInfo info = CertInfo.of(chain.get(0));
        if (!info.names().containsAll(cfg.domains().stream().map(d -> d.toLowerCase(java.util.Locale.ROOT)).toList())) {
            throw new AcmeException("the issued certificate does not cover all requested names: " + info.names());
        }
        if (!chain.get(0).getPublicKey().equals(certKey.getPublic())) {
            throw new AcmeException("the issued certificate does not match the generated key");
        }
        String keyPem = AcmeCrypto.pem("PRIVATE KEY", certKey.getPrivate().getEncoded());
        if (db != null) {
            JsonObject j = new JsonObject();
            j.addProperty("fullchain", issued.fullchainPem());
            j.addProperty("privkey", keyPem);
            j.addProperty("issuedAt", Instant.now().toString());
            db.put(certRowName(), j.toString());
            adoptedFingerprint = info.sha256();
        }
        store.writeCertificate(issued.fullchainPem(), keyPem);
        lastRenewal = Instant.now();
        TlsProvider.reloadAll();
        log.info("ACME: issued certificate for {} (valid until {}); serving it on all HTTPS listeners", info.names(), info.notAfter());
    }

    // ------------------------------------------------------------------ status

    public boolean enabled() {
        return cfg != null;
    }

    public AcmeConfig config() {
        return cfg;
    }

    public int ordersPlaced() {
        return ordersPlaced;
    }

    /** The certificate currently in the managed files (placeholder included), or null. */
    public CertInfo certificate() {
        return cfg == null ? null : currentCert();
    }

    public JsonObject statusJson() {
        JsonObject o = new JsonObject();
        o.addProperty("enabled", cfg != null);
        if (cfg == null) {
            o.addProperty("reason", disabledReason);
            return o;
        }
        Instant now = Instant.now();
        o.add("domains", new com.google.gson.Gson().toJsonTree(cfg.domains()));
        o.addProperty("directory", cfg.directory());
        o.addProperty("staging", cfg.staging() || cfg.directory().contains("staging"));
        o.addProperty("challenge", cfg.challenge() == AcmeConfig.Challenge.HTTP_01 ? "http-01" : "dns-01");
        o.addProperty("dnsProvider", cfg.dnsProvider());
        o.addProperty("renewDays", cfg.renewDays());
        o.addProperty("keyType", cfg.keyType());
        o.addProperty("shared", db != null);
        o.addProperty("issuing", issuing);
        o.addProperty("lastRenewal", lastRenewal == null ? null : lastRenewal.toString());
        o.addProperty("lastAttempt", lastAttempt == null ? null : lastAttempt.toString());
        o.addProperty("lastError", lastError);
        o.addProperty("lastErrorAt", lastErrorAt == null ? null : lastErrorAt.toString());
        o.addProperty("lastOutcome", lastOutcome);
        o.addProperty("failures", failures);
        o.addProperty("nextCheck", nextCheck == null ? null : nextCheck.toString());
        o.addProperty("backoffUntil", backoffUntil != null && backoffUntil.isAfter(now) ? backoffUntil.toString() : null);
        o.addProperty("ordersPlaced", ordersPlaced);
        if (http01 != null) {
            o.addProperty("httpPort", http01.port());
            o.addProperty("httpError", http01.bindError());
        }
        CertInfo c = currentCert();
        if (c != null) {
            o.add("certificate", certJson(c, now));
            o.addProperty("renewalDue", !c.placeholder() ? c.notAfter().minus(Duration.ofDays(cfg.renewDays())).toString() : null);
        }
        return o;
    }

    public static JsonObject certJson(CertInfo c, Instant now) {
        JsonObject j = new JsonObject();
        j.addProperty("subject", c.subject());
        j.addProperty("issuer", c.issuer());
        j.add("domainNames", new com.google.gson.Gson().toJsonTree(new ArrayList<>(c.names())));
        j.addProperty("notBefore", c.notBefore().toString());
        j.addProperty("notAfter", c.notAfter().toString());
        j.addProperty("daysLeft", c.daysLeft(now));
        j.addProperty("placeholder", c.placeholder());
        j.addProperty("sha256", c.sha256());
        j.addProperty("serial", c.serial());
        return j;
    }

    /** For tests / shutdown. */
    public void stop() {
        if (exec != null) {
            exec.shutdownNow();
        }
        if (http01 != null) {
            http01.stop();
        }
    }

    static Set<String> namesOf(X509Certificate c) {
        return new HashSet<>(CertInfo.of(c).names());
    }

    static X509Certificate parseOne(byte[] der) throws Exception {
        return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(new java.io.ByteArrayInputStream(der));
    }
}

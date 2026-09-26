package com.sayonora.warp.ab;

import com.google.gson.JsonObject;
import com.sayonora.warp.ab.AbAuthProvider.AuthException;
import com.sayonora.warp.ab.AbAuthProvider.AwsCreds;
import com.sayonora.warp.ab.AbAuthProvider.CloudCredential;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.xml.parsers.DocumentBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;

/**
 * AWS auth providers for cloud targets: static keys, assume-role (STS AssumeRole with automatic refresh),
 * web identity (IRSA, STS AssumeRoleWithWebIdentity) and the default credential chain (web identity env ->
 * AWS SDK v2 {@code DefaultCredentialsProvider}: env, system properties, profile, container, instance role).
 * Azure and Google ids are accepted by {@link #create} only to answer with {@link AbAuthProvider.Unimplemented}.
 * A per-request role override (the policy's client-to-role map) assumes that role using the provider's own
 * credentials as the base, with its own refreshed cache entry.
 */
public final class AbAwsAuth {

    private static final Logger log = LoggerFactory.getLogger(AbAwsAuth.class);

    /** Injectable clock for tests. */
    static volatile Supplier<Instant> clock = Instant::now;

    private AbAwsAuth() {
    }

    public static AbAuthProvider create(JsonObject auth, String region) {
        String type = auth.has("type") ? auth.get("type").getAsString() : "";
        switch (type) {
            case "static":
                return new StaticProvider(auth, region);
            case "assume-role":
                return new AssumeRoleProvider(auth, region);
            case "web-identity":
                return new WebIdentityProvider(auth, region);
            case "default-chain":
                return new DefaultChainProvider(auth, region);
            case "azure-service-principal":
            case "azure-managed-identity":
            case "google-service-account":
            case "google-workload-identity":
            case "google-impersonation":
                return new StubProvider(type);
            default:
                throw new IllegalArgumentException("unknown auth type '" + type + "' (static, assume-role, web-identity, "
                        + "default-chain; azure-*/google-* are UNIMPLEMENTED stubs)");
        }
    }

    /** True for provider ids that are stubs (rejected at configuration time with a clear message). */
    public static boolean isStub(String type) {
        return type.startsWith("azure-") || type.startsWith("google-");
    }

    private static String s(JsonObject o, String k) {
        return o.has(k) && !o.get(k).isJsonNull() && !o.get(k).getAsString().isBlank() ? o.get(k).getAsString() : null;
    }

    static final class StubProvider implements AbAuthProvider {
        private final String type;

        StubProvider(String type) {
            this.type = type;
        }

        @Override
        public CloudCredential resolve(String roleOverride) throws AuthException {
            throw new Unimplemented(type);
        }

        @Override
        public String type() {
            return type;
        }
    }

    /** Base providers can also be the source of an assume-role. */
    interface AwsBase {
        AwsCreds base() throws AuthException;
    }

    /** Caches assumed-role credentials per role ARN and refreshes them before they expire. */
    static final class RoleCache {
        private record Entry(AwsCreds creds) {
        }

        private final ConcurrentHashMap<String, Entry> cache = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
        final long skewSeconds;
        volatile int refreshCount;

        RoleCache(long skewSeconds) {
            this.skewSeconds = skewSeconds;
        }

        AwsCreds get(String key, java.util.concurrent.Callable<AwsCreds> loader) throws AuthException {
            Entry e = cache.get(key);
            if (fresh(e)) {
                return e.creds;
            }
            synchronized (locks.computeIfAbsent(key, k -> new Object())) {
                e = cache.get(key);
                if (fresh(e)) {
                    return e.creds;
                }
                try {
                    AwsCreds c = loader.call();
                    refreshCount++;
                    cache.put(key, new Entry(c));
                    return c;
                } catch (AuthException ex) {
                    if (e != null && e.creds.expiresAt() != null && clock.get().isBefore(e.creds.expiresAt())) {
                        log.warn("cloud auth: refresh failed, continuing on the still-valid previous credentials: {}", ex.getMessage());
                        return e.creds;
                    }
                    throw ex;
                } catch (Exception ex) {
                    throw new AuthException("credential refresh failed: " + ex.getMessage(), ex);
                }
            }
        }

        private boolean fresh(Entry e) {
            return e != null && (e.creds.expiresAt() == null
                    || clock.get().isBefore(e.creds.expiresAt().minusSeconds(skewSeconds)));
        }
    }

    static final class StaticProvider implements AbAuthProvider, AwsBase {
        private final AwsCreds creds;
        private final Sts sts;
        private final RoleCache roles;

        StaticProvider(JsonObject a, String region) {
            String ak = s(a, "accessKeyId");
            String sk = s(a, "secretAccessKey");
            if (ak == null || sk == null) {
                throw new IllegalArgumentException("static auth needs accessKeyId and secretAccessKey");
            }
            this.creds = new AwsCreds(ak, sk, s(a, "sessionToken"), null);
            this.sts = new Sts(s(a, "stsEndpoint"), region);
            this.roles = new RoleCache(skew(a));
        }

        @Override
        public AwsCreds base() {
            return creds;
        }

        @Override
        public CloudCredential resolve(String roleOverride) throws AuthException {
            return roleOverride == null ? creds : assumeOverride(roles, sts, this, roleOverride, null);
        }

        @Override
        public String type() {
            return "static";
        }
    }

    static long skew(JsonObject a) {
        return a.has("refreshSkewSeconds") ? Math.max(0, a.get("refreshSkewSeconds").getAsLong()) : 300;
    }

    static AwsCreds assumeOverride(RoleCache roles, Sts sts, AwsBase base, String role, String externalId) throws AuthException {
        return roles.get("role:" + role, () -> sts.assumeRole(base.base(), role, "warp-ab", 3600, externalId));
    }

    static final class AssumeRoleProvider implements AbAuthProvider, AwsBase {
        private final AbAuthProvider source;
        private final String roleArn;
        private final String externalId;
        private final String sessionName;
        private final int duration;
        private final Sts sts;
        private final RoleCache roles;

        AssumeRoleProvider(JsonObject a, String region) {
            this.roleArn = s(a, "roleArn");
            if (roleArn == null) {
                throw new IllegalArgumentException("assume-role auth needs roleArn");
            }
            this.externalId = s(a, "externalId");
            this.sessionName = s(a, "sessionName") == null ? "warp-ab" : s(a, "sessionName");
            this.duration = a.has("durationSeconds") ? a.get("durationSeconds").getAsInt() : 3600;
            this.sts = new Sts(s(a, "stsEndpoint"), region);
            this.roles = new RoleCache(skew(a));
            JsonObject src = a.has("source") && a.get("source").isJsonObject() ? a.getAsJsonObject("source") : null;
            if (src == null) {
                JsonObject d = new JsonObject();
                d.addProperty("type", "default-chain");
                src = d;
            }
            if (src.has("type") && src.get("type").getAsString().equals("assume-role")) {
                throw new IllegalArgumentException("assume-role source must be static, web-identity or default-chain");
            }
            this.source = create(src, region);
        }

        @Override
        public AwsCreds base() throws AuthException {
            return roles.get("self", () -> sts.assumeRole(sourceCreds(), roleArn, sessionName, duration, externalId));
        }

        private AwsCreds sourceCreds() throws AuthException {
            CloudCredential c = source.resolve(null);
            return (AwsCreds) c;
        }

        @Override
        public CloudCredential resolve(String roleOverride) throws AuthException {
            if (roleOverride == null) {
                return base();
            }
            // narrow: assume the override role using this target's own (assumed or source) identity as the base
            return roles.get("role:" + roleOverride, () -> sts.assumeRole(sourceCreds(), roleOverride, sessionName, duration, externalId));
        }

        int refreshes() {
            return roles.refreshCount;
        }

        @Override
        public String type() {
            return "assume-role";
        }
    }

    static final class WebIdentityProvider implements AbAuthProvider, AwsBase {
        private final String roleArn;
        private final String tokenFile;
        private final String sessionName;
        private final Sts sts;
        private final RoleCache roles;

        WebIdentityProvider(JsonObject a, String region) {
            this.roleArn = s(a, "roleArn") != null ? s(a, "roleArn") : System.getenv("AWS_ROLE_ARN");
            this.tokenFile = s(a, "tokenFile") != null ? s(a, "tokenFile") : System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
            if (roleArn == null || tokenFile == null) {
                throw new IllegalArgumentException("web-identity auth needs roleArn and tokenFile (or AWS_ROLE_ARN / AWS_WEB_IDENTITY_TOKEN_FILE)");
            }
            this.sessionName = s(a, "sessionName") == null ? "warp-ab" : s(a, "sessionName");
            this.sts = new Sts(s(a, "stsEndpoint"), region);
            this.roles = new RoleCache(skew(a));
        }

        @Override
        public AwsCreds base() throws AuthException {
            // the token file is re-read on every refresh (kubelet rotates it)
            return roles.get("self", () -> {
                String token = Files.readString(Path.of(tokenFile), StandardCharsets.UTF_8).trim();
                return sts.assumeRoleWithWebIdentity(roleArn, sessionName, token);
            });
        }

        @Override
        public CloudCredential resolve(String roleOverride) throws AuthException {
            return roleOverride == null ? base() : assumeOverride(roles, sts, this, roleOverride, null);
        }

        @Override
        public String type() {
            return "web-identity";
        }
    }

    /** Env web identity (IRSA) if configured, else the AWS SDK v2 default provider chain. */
    static final class DefaultChainProvider implements AbAuthProvider, AwsBase {
        private final Sts sts;
        private final RoleCache roles;
        private final AbAuthProvider irsa;
        private volatile software.amazon.awssdk.auth.credentials.AwsCredentialsProvider sdk;

        DefaultChainProvider(JsonObject a, String region) {
            this.sts = new Sts(s(a, "stsEndpoint"), region);
            this.roles = new RoleCache(skew(a));
            String rn = System.getenv("AWS_ROLE_ARN");
            String tf = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
            JsonObject w = new JsonObject();
            if (rn != null && tf != null) {
                w.addProperty("type", "web-identity");
                w.addProperty("roleArn", rn);
                w.addProperty("tokenFile", tf);
                w.addProperty("stsEndpoint", s(a, "stsEndpoint"));
                this.irsa = new WebIdentityProvider(w, region);
            } else {
                this.irsa = null;
            }
        }

        @Override
        public AwsCreds base() throws AuthException {
            if (irsa != null) {
                return (AwsCreds) irsa.resolve(null);
            }
            try {
                if (sdk == null) {
                    sdk = software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.builder().build();
                }
                var c = sdk.resolveCredentials();
                String token = c instanceof software.amazon.awssdk.auth.credentials.AwsSessionCredentials sc ? sc.sessionToken() : null;
                Instant exp = c.expirationTime().orElse(null);
                return new AwsCreds(c.accessKeyId(), c.secretAccessKey(), token, exp);
            } catch (RuntimeException e) {
                throw new AuthException("default credential chain found no credentials: " + e.getMessage(), e);
            }
        }

        @Override
        public CloudCredential resolve(String roleOverride) throws AuthException {
            return roleOverride == null ? base() : assumeOverride(roles, sts, this, roleOverride, null);
        }

        @Override
        public String type() {
            return "default-chain";
        }
    }

    /** Minimal STS client: AssumeRole (SigV4-signed with the base credentials) and AssumeRoleWithWebIdentity (unsigned). */
    static final class Sts {
        private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        private final String endpoint;
        private final String region;

        Sts(String endpoint, String region) {
            this.region = region == null ? "us-east-1" : region;
            this.endpoint = endpoint != null ? endpoint : "https://sts." + this.region + ".amazonaws.com";
        }

        AwsCreds assumeRole(AwsCreds base, String roleArn, String sessionName, int duration, String externalId) throws AuthException {
            Map<String, String> p = new LinkedHashMap<>();
            p.put("Action", "AssumeRole");
            p.put("Version", "2011-06-15");
            p.put("RoleArn", roleArn);
            p.put("RoleSessionName", sessionName);
            p.put("DurationSeconds", Integer.toString(duration));
            if (externalId != null) {
                p.put("ExternalId", externalId);
            }
            return call(p, base);
        }

        AwsCreds assumeRoleWithWebIdentity(String roleArn, String sessionName, String token) throws AuthException {
            Map<String, String> p = new LinkedHashMap<>();
            p.put("Action", "AssumeRoleWithWebIdentity");
            p.put("Version", "2011-06-15");
            p.put("RoleArn", roleArn);
            p.put("RoleSessionName", sessionName);
            p.put("WebIdentityToken", token);
            return call(p, null);
        }

        private AwsCreds call(Map<String, String> params, AwsCreds signWith) throws AuthException {
            StringBuilder form = new StringBuilder();
            params.forEach((k, v) -> {
                if (form.length() > 0) {
                    form.append('&');
                }
                form.append(AbSigV4.enc(k, false)).append('=').append(AbSigV4.enc(v, false));
            });
            byte[] body = form.toString().getBytes(StandardCharsets.UTF_8);
            URI uri = URI.create(endpoint);
            HttpRequest.Builder rb = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(30));
            Map<String, String> h = new LinkedHashMap<>();
            h.put("content-type", "application/x-www-form-urlencoded; charset=utf-8");
            if (signWith != null) {
                String host = uri.getPort() > 0 ? uri.getHost() + ":" + uri.getPort() : uri.getHost();
                h.put("host", host);
                String path = uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath();
                h = AbSigV4.sign("POST", path, "", h, AbSigV4.hex(AbSigV4.sha256(body)), signWith, region, "sts", clock.get(), false);
                h.remove("host");
            }
            h.forEach(rb::header);
            try {
                HttpResponse<String> r = HTTP.send(rb.POST(HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                        HttpResponse.BodyHandlers.ofString());
                if (r.statusCode() / 100 != 2) {
                    // STS error bodies contain a Code/Message, never our secrets; keep it short
                    throw new AuthException("STS " + params.get("Action") + " failed: HTTP " + r.statusCode() + " " + extract(r.body(), "Code"));
                }
                Document d = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                        .parse(new java.io.ByteArrayInputStream(r.body().getBytes(StandardCharsets.UTF_8)));
                String ak = text(d, "AccessKeyId");
                String sk = text(d, "SecretAccessKey");
                String tok = text(d, "SessionToken");
                String exp = text(d, "Expiration");
                if (ak == null || sk == null) {
                    throw new AuthException("STS response carried no credentials");
                }
                return new AwsCreds(ak, sk, tok, exp == null ? null : Instant.parse(exp));
            } catch (AuthException e) {
                throw e;
            } catch (java.io.IOException | InterruptedException | javax.xml.parsers.ParserConfigurationException
                    | org.xml.sax.SAXException | java.time.format.DateTimeParseException e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new AuthException("STS " + params.get("Action") + " failed: " + e.getClass().getSimpleName() + ": " + e.getMessage(), e);
            }
        }

        private static String text(Document d, String tag) {
            var n = d.getElementsByTagName(tag);
            return n.getLength() == 0 ? null : n.item(0).getTextContent().trim();
        }

        private static String extract(String xml, String tag) {
            int i = xml == null ? -1 : xml.indexOf("<" + tag + ">");
            int j = i < 0 ? -1 : xml.indexOf("</" + tag + ">", i);
            return i < 0 || j < 0 ? "" : xml.substring(i + tag.length() + 2, j);
        }
    }
}

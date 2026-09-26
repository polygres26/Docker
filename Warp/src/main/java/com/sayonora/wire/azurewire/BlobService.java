package com.sayonora.wire.azurewire;

import com.sayonora.wire.azurewire.BlobModel.Blob;
import com.sayonora.wire.azurewire.BlobModel.Cont;
import com.sayonora.wire.azurewire.BlobModel.Lease;
import com.sayonora.wire.azurewire.BlobModel.Seg;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The Azure Blob service REST API on top of {@link BlobStore}: containers (properties, metadata, ACL, leases, listing),
 * block / append / page blobs, snapshots, tags, copy, tiers, leases and conditional headers, service properties (with
 * CORS), account information, and blob batch (delete / set tier). Error codes, statuses and messages follow Azure's.
 */
final class BlobService {

    static final String MAX_VERSION = "2025-01-05";
    private static final long MAX_PUT_BLOB = 5000L * 1024 * 1024;
    private static final long MAX_BLOCK = 4000L * 1024 * 1024;
    private static final long MAX_APPEND_BLOCK = 4L * 1024 * 1024;
    private static final long MAX_PAGE_WRITE = 4L * 1024 * 1024;
    private static final Pattern CONTAINER_NAME = Pattern.compile("^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}$");
    private static final Pattern META_NAME = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
    private static final Set<String> BLOCK_TIERS = Set.of("Hot", "Cool", "Cold", "Archive");

    final BlobStore store;
    final AzureConfig cfg;
    final AzureAuth authn;

    BlobService(BlobStore store, AzureConfig cfg, AzureAuth authn) {
        this.store = store;
        this.cfg = cfg;
        this.authn = authn;
    }

    // ------------------------------------------------------------------------------------------ dispatch

    void handle(AzReq r, HttpServletResponse resp) throws IOException {
        String container = r.first();
        String blob = r.rest();
        if (blob != null && blob.isEmpty()) {
            blob = null;
        }
        if ("OPTIONS".equals(r.method)) {
            AzCors.preflight(r, resp, corsRules(r.account));
            return;
        }
        Cont[] contHolder = new Cont[1];
        AzureAuth.Result auth = authn.authenticate(r, id -> {
            if (container == null) {
                return null;
            }
            Cont k = contHolder[0] != null ? contHolder[0] : (contHolder[0] = store.getContainer(r.account, container));
            if (k == null) {
                return null;
            }
            for (AzureAuth.Policy p : k.acl) {
                if (p.id().equals(id)) {
                    return p;
                }
            }
            return null;
        }, container, blob);
        AzCors.applyActual(r, resp, corsRules(r.account));
        dispatch(r, resp, auth, container, blob);
    }

    void dispatch(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String blob) throws IOException {
        String comp = r.q("comp");
        String restype = r.q("restype");
        if (container == null) {
            serviceLevel(r, resp, auth, comp, restype);
        } else if (blob == null) {
            containerLevel(r, resp, auth, container, comp, restype);
        } else {
            blobLevel(r, resp, auth, container, blob, comp);
        }
    }

    // ------------------------------------------------------------------------------------------ helpers

    static void requireMethod(AzReq r, String... allowed) {
        for (String m : allowed) {
            if (m.equals(r.method)) {
                return;
            }
        }
        throw new AzureException(405, "UnsupportedHttpVerb", "The resource doesn't support specified Http Verb.");
    }

    static Map<String, String> metadata(AzReq r) {
        Map<String, String> m = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        for (String n : Collections.list(r.raw.getHeaderNames())) {
            if (n.toLowerCase(Locale.ROOT).startsWith("x-ms-meta-")) {
                String key = n.substring("x-ms-meta-".length());
                if (!META_NAME.matcher(key).matches()) {
                    throw new AzureException(400, "InvalidMetadata", "The metadata specified is invalid. It has characters "
                            + "that are not permitted.");
                }
                if (!seen.add(key.toLowerCase(Locale.ROOT))) {
                    throw new AzureException(400, "InvalidMetadata", "The metadata specified is invalid. It has characters "
                            + "that are not permitted.");
                }
                String v = r.raw.getHeader(n);
                for (int i = 0; i < v.length(); i++) {
                    if (v.charAt(i) > 127) {
                        throw new AzureException(400, "InvalidMetadata", "The metadata specified is invalid. It has "
                                + "characters that are not permitted.");
                    }
                }
                m.put(key, v);
            }
        }
        return m;
    }

    static void writeMetadata(HttpServletResponse resp, Map<String, String> m) {
        m.forEach((k, v) -> resp.setHeader("x-ms-meta-" + k, v));
    }

    static void writeLease(HttpServletResponse resp, Lease l, Instant now) {
        String e = l.effective(now);
        boolean locked = e.equals("leased") || e.equals("breaking");
        resp.setHeader("x-ms-lease-status", locked ? "locked" : "unlocked");
        resp.setHeader("x-ms-lease-state", e);
        if (e.equals("leased")) {
            resp.setHeader("x-ms-lease-duration", l.duration < 0 ? "infinite" : "fixed");
        }
    }

    static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    static String leaseHeader(AzReq r) {
        String v = r.header("x-ms-lease-id");
        if (v != null && !isGuid(v)) {
            throw AzErrors.invalidHeader("x-ms-lease-id", v);
        }
        return v;
    }

    static boolean isGuid(String s) {
        try {
            UUID.fromString(s);
            return s.length() == 36;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** Lease gate for a write (and for reads that carry a lease id). */
    static void checkLease(AzReq r, Lease l, boolean write, String what) {
        String given = leaseHeader(r);
        Instant now = Instant.now();
        boolean locked = l.locked(now);
        String cap = what.equals("blob") ? "Blob" : "Container";
        if (locked) {
            if (given == null) {
                if (write) {
                    throw new AzureException(412, "LeaseIdMissing", "There is currently a lease on the " + what
                            + " and no lease ID was specified in the request.");
                }
                return;
            }
            if (!given.equalsIgnoreCase(l.id)) {
                throw new AzureException(412, "LeaseIdMismatchWith" + cap + "Operation",
                        "The lease ID specified did not match the lease ID for the " + what + ".");
            }
        } else if (given != null) {
            throw new AzureException(412, "LeaseNotPresentWith" + cap + "Operation",
                    "There is currently no lease on the " + what + ".");
        }
    }

    // ---- conditional headers

    static boolean etagMatches(String header, String etag) {
        if (header == null) {
            return false;
        }
        for (String part : header.split(",")) {
            String p = part.trim();
            if (p.equals("*") && etag != null) {
                return true;
            }
            if (p.startsWith("W/")) {
                p = p.substring(2);
            }
            if (etag != null && (p.equals(etag) || p.replace("\"", "").equals(etag.replace("\"", "")))) {
                return true;
            }
        }
        return false;
    }

    private static Instant date(AzReq r, String h) {
        String v = r.header(h);
        if (v == null) {
            return null;
        }
        Instant t = AzHttp.parseHttpDate(v);
        if (t == null) {
            throw AzErrors.invalidHeader(h, v);
        }
        return t;
    }

    /**
     * Evaluates If-Match / If-None-Match / If-Modified-Since / If-Unmodified-Since (and x-ms-if-tags for blobs).
     * @param exists whether the resource exists (a missing resource with If-Match fails the condition)
     * @return true when a read must answer 304 Not Modified
     */
    static boolean conditions(AzReq r, boolean exists, String etag, Instant lastModified, Map<String, String> tags,
            boolean read) {
        String ifMatch = r.header("If-Match");
        String ifNone = r.header("If-None-Match");
        Instant ims = date(r, "If-Modified-Since");
        Instant ius = date(r, "If-Unmodified-Since");
        String ifTags = r.header("x-ms-if-tags");
        if (!exists) {
            if (ifMatch != null) {
                throw AzErrors.conditionNotMet();
            }
            return false;
        }
        Instant lm = lastModified == null ? null : lastModified.truncatedTo(ChronoUnit.SECONDS);
        if (ifMatch != null && !etagMatches(ifMatch, etag)) {
            throw AzErrors.conditionNotMet();
        }
        if (ius != null && lm != null && lm.isAfter(ius)) {
            throw AzErrors.conditionNotMet();
        }
        if (ifTags != null && tags != null) {
            boolean ok;
            try {
                ok = BlobTags.matches(ifTags, tags);
            } catch (IllegalArgumentException e) {
                throw new AzureException(400, "InvalidHeaderValue", "The value for one of the HTTP headers is not in the "
                        + "correct format.").extra("HeaderName", "x-ms-if-tags").extra("HeaderValue", ifTags);
            }
            if (!ok) {
                throw AzErrors.conditionNotMet();
            }
        }
        if (ifNone != null && etagMatches(ifNone, etag)) {
            if (read) {
                return true;
            }
            if (ifNone.trim().equals("*")) {
                throw new AzureException(409, "BlobAlreadyExists", "The specified blob already exists.");
            }
            throw AzErrors.conditionNotMet();
        }
        if (ims != null && lm != null && !lm.isAfter(ims)) {
            if (read) {
                return true;
            }
            throw AzErrors.conditionNotMet();
        }
        return false;
    }

    static void validateContainerName(String name) {
        if (!("$root".equals(name) || "$logs".equals(name) || "$web".equals(name)) && (name.length() < 3 || name.length() > 63)) {
            throw new AzureException(400, "OutOfRangeInput", "The specified resource name length is not within the permissible limits.");
        }
        if (!("$root".equals(name) || "$logs".equals(name) || "$web".equals(name) || CONTAINER_NAME.matcher(name).matches())) {
            throw new AzureException(400, "InvalidResourceName", "The specified resource name contains invalid characters.");
        }
    }

    static String pubAccess(AzReq r) {
        String v = r.header("x-ms-blob-public-access");
        if (v == null) {
            return "";
        }
        if (!v.equals("blob") && !v.equals("container")) {
            throw AzErrors.invalidHeader("x-ms-blob-public-access", v);
        }
        return v;
    }

    private Cont requireContainer(String account, String container) {
        Cont k = store.getContainer(account, container);
        if (k == null) {
            throw AzErrors.containerNotFound();
        }
        return k;
    }

    static int maxResults(AzReq r) {
        String mr = r.q("maxresults");
        if (mr == null) {
            return 5000;
        }
        try {
            int n = Integer.parseInt(mr);
            if (n <= 0) {
                throw AzErrors.outOfRange("maxresults", mr, "1", "2147483647");
            }
            return Math.min(n, 5000);
        } catch (NumberFormatException e) {
            throw AzErrors.invalidQuery("maxresults", mr);
        }
    }

    static Set<String> include(AzReq r) {
        String v = r.q("include");
        Set<String> s = new HashSet<>();
        if (v != null && !v.isEmpty()) {
            for (String p : v.split(",")) {
                s.add(p.trim().toLowerCase(Locale.ROOT));
            }
        }
        return s;
    }

    String endpoint(AzReq r) {
        String host = r.header("Host");
        String scheme = r.raw.getScheme();
        if (r.hostStyle) {
            return scheme + "://" + host + "/";
        }
        return scheme + "://" + host + "/" + r.account;
    }

    // ------------------------------------------------------------------------------------------ service level

    private void serviceLevel(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String comp, String restype)
            throws IOException {
        if ("service".equals(restype) && "properties".equals(comp)) {
            if ("GET".equals(r.method)) {
                r.op = "GetServiceProperties";
                auth.authorize('s', "r");
                String xml = servicePropertiesXml(r.account);
                AzHttp.xml(resp, 200, xml);
                return;
            }
            if ("PUT".equals(r.method)) {
                r.op = "SetServiceProperties";
                r.write = true;
                auth.authorize('s', "w");
                setServiceProperties(r);
                resp.setStatus(202);
                return;
            }
            requireMethod(r);
        }
        if ("service".equals(restype) && "stats".equals(comp)) {
            r.op = "GetServiceStats";
            requireMethod(r, "GET");
            auth.authorize('s', "r");
            AzXml x = new AzXml().open("StorageServiceStats").open("GeoReplication").text("Status", "live")
                    .text("LastSyncTime", AzHttp.httpDate(Instant.now())).close().close();
            AzHttp.xml(resp, 200, x.toString());
            return;
        }
        if ("account".equals(restype) && "properties".equals(comp)) {
            r.op = "GetAccountInformation";
            requireMethod(r, "GET", "HEAD");
            auth.authorize('s', "r");
            resp.setHeader("x-ms-sku-name", "Standard_RAGRS");
            resp.setHeader("x-ms-account-kind", "StorageV2");
            resp.setHeader("x-ms-is-hns-enabled", "false");
            resp.setStatus(200);
            return;
        }
        if ("userdelegationkey".equals(comp)) {
            r.op = "GetUserDelegationKey";
            throw new AzureException(501, "NotImplemented", "Warp azurewire does not issue user delegation keys "
                    + "(Entra ID authentication is not supported); use SharedKey or SAS.");
        }
        if ("blobs".equals(comp) && "GET".equals(r.method)) {
            r.op = "FindBlobsByTags";
            auth.authorize('s', "f");
            findByTags(r, resp, null);
            return;
        }
        if ("batch".equals(comp) && "POST".equals(r.method)) {
            r.op = "BlobBatch";
            r.write = true;
            BlobBatch.run(this, r, resp, auth);
            return;
        }
        if ("list".equals(comp)) {
            r.op = "ListContainers";
            requireMethod(r, "GET");
            auth.authorize('s', "l");
            listContainers(r, resp);
            return;
        }
        if (comp == null && restype == null) {
            throw AzErrors.invalidUri();
        }
        throw new AzureException(400, "InvalidQueryParameterValue", "Value for one of the query parameters specified in "
                + "the request URI is invalid.").extra("QueryParameterName", comp != null ? "comp" : "restype")
                .extra("QueryParameterValue", comp != null ? comp : restype);
    }

    private List<AzCors.Rule> corsRules(String account) {
        try {
            return AzCors.parse(AzServiceProps.cors(store.serviceProperties(account)));
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    private String servicePropertiesXml(String account) {
        return AzServiceProps.render(store.serviceProperties(account), true);
    }

    private void setServiceProperties(AzReq r) throws IOException {
        byte[] body = r.raw.getInputStream().readAllBytes();
        String merged = AzServiceProps.merge(store.serviceProperties(r.account), body, true);
        store.setServiceProperties(r.account, merged);
    }

    private void listContainers(AzReq r, HttpServletResponse resp) throws IOException {
        String prefix = r.q("prefix") == null ? "" : r.q("prefix");
        String marker = r.q("marker") == null ? "" : r.q("marker");
        int max = maxResults(r);
        boolean meta = include(r).contains("metadata");
        List<Cont> l = store.listContainers(r.account, prefix, marker, max + 1);
        AzXml x = new AzXml().openAttrs("EnumerationResults", "ServiceEndpoint", endpoint(r));
        x.textOrEmpty("Prefix", prefix).text("MaxResults", String.valueOf(max));
        x.open("Containers");
        Instant now = Instant.now();
        for (int i = 0; i < Math.min(max, l.size()); i++) {
            Cont k = l.get(i);
            x.open("Container").text("Name", k.name).open("Properties");
            x.text("Last-Modified", AzHttp.httpDate(k.lastModified)).text("Etag", k.etag);
            String e = k.lease.effective(now);
            x.text("LeaseStatus", e.equals("leased") || e.equals("breaking") ? "locked" : "unlocked");
            x.text("LeaseState", e);
            if (e.equals("leased")) {
                x.text("LeaseDuration", k.lease.duration < 0 ? "infinite" : "fixed");
            }
            if (!k.publicAccess.isEmpty()) {
                x.text("PublicAccess", k.publicAccess);
            }
            x.text("HasImmutabilityPolicy", "false").text("HasLegalHold", "false").close();
            if (meta && !k.metadata.isEmpty()) {
                x.open("Metadata");
                k.metadata.forEach(x::text);
                x.close();
            }
            x.close();
        }
        x.close();
        x.textOrEmpty("NextMarker", l.size() > max ? l.get(max).name : "");
        x.close();
        AzHttp.xml(resp, 200, x.toString());
    }

    private void findByTags(AzReq r, HttpServletResponse resp, String container) throws IOException {
        String where = r.q("where");
        if (where == null) {
            throw AzErrors.missingHeader("where");
        }
        int max = maxResults(r);
        String marker = r.q("marker");
        List<Blob> hits = new ArrayList<>();
        try {
            for (Blob b : store.taggedBlobs(r.account, container)) {
                Map<String, String> tv = new LinkedHashMap<>(b.tags);
                tv.put("@container", b.container);
                if (BlobTags.matches(where, tv)) {
                    hits.add(b);
                }
            }
        } catch (IllegalArgumentException e) {
            throw new AzureException(400, "InvalidQueryParameterValue", "Value for one of the query parameters specified in "
                    + "the request URI is invalid.").extra("QueryParameterName", "where").extra("QueryParameterValue", where)
                    .extra("Reason", e.getMessage());
        }
        int from = 0;
        if (marker != null && !marker.isEmpty()) {
            try {
                from = Integer.parseInt(new String(Base64.getDecoder().decode(marker), StandardCharsets.UTF_8));
            } catch (RuntimeException e) {
                throw AzErrors.invalidQuery("marker", marker);
            }
        }
        AzXml x = new AzXml().openAttrs("EnumerationResults", "ServiceEndpoint", endpoint(r));
        x.text("Where", where).open("Blobs");
        int to = Math.min(hits.size(), from + max);
        for (int i = from; i < to; i++) {
            Blob b = hits.get(i);
            x.open("Blob").text("Name", b.name).text("ContainerName", b.container).open("Tags").open("TagSet");
            b.tags.forEach((k, v) -> x.open("Tag").text("Key", k).text("Value", v).close());
            x.close().close().close();
        }
        x.close();
        x.textOrEmpty("NextMarker", to < hits.size() ? Base64.getEncoder().encodeToString(String.valueOf(to)
                .getBytes(StandardCharsets.UTF_8)) : "");
        x.close();
        AzHttp.xml(resp, 200, x.toString());
    }

    // ------------------------------------------------------------------------------------------ container level

    private void containerLevel(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String comp,
            String restype) throws IOException {
        String m = r.method;
        if ("batch".equals(comp) && "POST".equals(m)) {
            r.op = "BlobBatch";
            r.write = true;
            BlobBatch.run(this, r, resp, auth);
            return;
        }
        if ("blobs".equals(comp) && "GET".equals(m)) {
            r.op = "FindBlobsByTags";
            auth.authorize('c', "f");
            requireContainer(r.account, container);
            findByTags(r, resp, container);
            return;
        }
        if (!"container".equals(restype)) {
            if ("PUT".equals(m) && "undelete".equals(comp)) {
                throw AzErrors.unsupported("Restore Container");
            }
            throw AzErrors.invalidUri();
        }
        if (comp == null) {
            switch (m) {
                case "PUT" -> createContainer(r, resp, auth, container);
                case "GET", "HEAD" -> containerProperties(r, resp, auth, container);
                case "DELETE" -> deleteContainer(r, resp, auth, container);
                default -> requireMethod(r);
            }
            return;
        }
        switch (comp) {
            case "metadata" -> {
                if ("PUT".equals(m)) {
                    r.op = "SetContainerMetadata";
                    r.write = true;
                    auth.authorize('c', "w");
                    Map<String, String> md = metadata(r);
                    Cont k = requireContainer(r.account, container);
                    checkLease(r, k.lease, true, "container");
                    Instant ims = date(r, "If-Modified-Since");
                    if (ims != null && !k.lastModified.truncatedTo(ChronoUnit.SECONDS).isAfter(ims)) {
                        throw AzErrors.conditionNotMet();
                    }
                    Cont out = store.mutateContainer(r.account, container, kk -> {
                        kk.metadata = md;
                        kk.lastModified = now();
                        kk.etag = AzHttp.newEtag();
                        return kk;
                    });
                    resp.setHeader("ETag", out.etag);
                    resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
                    resp.setStatus(200);
                } else if ("GET".equals(m) || "HEAD".equals(m)) {
                    r.op = "GetContainerMetadata";
                    containerProperties(r, resp, auth, container);
                } else {
                    requireMethod(r);
                }
            }
            case "acl" -> containerAcl(r, resp, auth, container);
            case "lease" -> {
                requireMethod(r, "PUT");
                r.op = "ContainerLease";
                r.write = true;
                auth.authorize('c', "w");
                leaseContainer(r, resp, container);
            }
            case "list" -> {
                requireMethod(r, "GET");
                r.op = "ListBlobs";
                Cont k = requireContainer(r.account, container);
                authorizeContainerRead(r, auth, k, 'l', "container");
                listBlobs(r, resp, container);
            }
            default -> throw new AzureException(400, "InvalidQueryParameterValue", "Value for one of the query parameters "
                    + "specified in the request URI is invalid.").extra("QueryParameterName", "comp")
                    .extra("QueryParameterValue", comp);
        }
    }

    /** Anonymous access is allowed on a container whose public access is 'container' (list / properties). */
    private void authorizeContainerRead(AzReq r, AzureAuth.Result auth, Cont k, char perm, String need) {
        if (auth.anonymous()) {
            if ("container".equals(k.publicAccess)) {
                return;
            }
            throw noAuth();
        }
        auth.authorize('c', String.valueOf(perm));
    }

    static AzureException noAuth() {
        return new AzureException(403, "AuthorizationFailure", "Server failed to authenticate the request. Make sure the "
                + "value of the Authorization header is formed correctly including the signature.");
    }

    /** Anonymous access to Queue / Table: Azure answers 403 AuthenticationFailed. */
    static AzureException noAuthKeyed() {
        return new AzureException(403, "AuthenticationFailed", "Server failed to authenticate the request. Make sure the "
                + "value of the Authorization header is formed correctly including the signature.");
    }

    private void createContainer(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container) {
        r.op = "CreateContainer";
        r.write = true;
        auth.authorize('c', "c");
        validateContainerName(container);
        Cont k = new Cont();
        k.account = r.account;
        k.name = container;
        k.metadata = metadata(r);
        k.publicAccess = pubAccess(r);
        k.etag = AzHttp.newEtag();
        k.createdAt = now();
        k.lastModified = k.createdAt;
        store.createContainer(k);
        resp.setHeader("ETag", k.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(k.lastModified));
        resp.setStatus(201);
    }

    private void containerProperties(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container) {
        if (r.op == null || r.op.equals("Unknown")) {
            r.op = "GetContainerProperties";
        }
        requireMethod(r, "GET", "HEAD");
        Cont k = requireContainer(r.account, container);
        if (auth.anonymous()) {
            if ("container".equals(k.publicAccess)) {
                // allowed: anonymous container properties
            } else {
                throw noAuth();
            }
        } else {
            auth.authorize('c', "rl");
        }
        checkLease(r, k.lease, false, "container");
        resp.setHeader("ETag", k.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(k.lastModified));
        writeMetadata(resp, k.metadata);
        writeLease(resp, k.lease, Instant.now());
        if (!k.publicAccess.isEmpty()) {
            resp.setHeader("x-ms-blob-public-access", k.publicAccess);
        }
        resp.setHeader("x-ms-has-immutability-policy", "false");
        resp.setHeader("x-ms-has-legal-hold", "false");
        resp.setHeader("x-ms-default-encryption-scope", "$account-encryption-key");
        resp.setHeader("x-ms-deny-encryption-scope-override", "false");
        resp.setHeader("x-ms-immutable-storage-with-versioning-enabled", "false");
        resp.setStatus(200);
    }

    private void deleteContainer(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container) {
        r.op = "DeleteContainer";
        r.write = true;
        auth.authorize('c', "d");
        Cont k = requireContainer(r.account, container);
        checkLease(r, k.lease, true, "container");
        conditions(r, true, k.etag, k.lastModified, null, false);
        store.deleteContainer(r.account, container);
        resp.setStatus(202);
    }

    private void containerAcl(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container) throws IOException {
        if ("PUT".equals(r.method)) {
            r.op = "SetContainerAcl";
            r.write = true;
            if (auth.kind != AzureAuth.Kind.KEY && auth.kind != AzureAuth.Kind.BEARER) {
                throw new AzureException(403, "AuthorizationFailure", "This request is not authorized to perform this operation.");
            }
            String access = pubAccess(r);
            byte[] body = r.raw.getInputStream().readAllBytes();
            List<AzureAuth.Policy> policies = AzServiceProps.parseSignedIdentifiers(body);
            Cont k = requireContainer(r.account, container);
            checkLease(r, k.lease, true, "container");
            Cont out = store.mutateContainer(r.account, container, kk -> {
                kk.acl = policies;
                kk.publicAccess = access;
                kk.lastModified = now();
                kk.etag = AzHttp.newEtag();
                return kk;
            });
            resp.setHeader("ETag", out.etag);
            resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
            resp.setStatus(200);
            return;
        }
        requireMethod(r, "GET", "HEAD");
        r.op = "GetContainerAcl";
        if (auth.kind != AzureAuth.Kind.KEY && auth.kind != AzureAuth.Kind.BEARER) {
            throw new AzureException(403, "AuthorizationFailure", "This request is not authorized to perform this operation.");
        }
        Cont k = requireContainer(r.account, container);
        checkLease(r, k.lease, false, "container");
        resp.setHeader("ETag", k.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(k.lastModified));
        if (!k.publicAccess.isEmpty()) {
            resp.setHeader("x-ms-blob-public-access", k.publicAccess);
        }
        AzXml x = new AzXml().open("SignedIdentifiers");
        for (AzureAuth.Policy p : k.acl) {
            x.open("SignedIdentifier").text("Id", p.id()).open("AccessPolicy");
            if (p.start() != null) {
                x.text("Start", p.start());
            }
            if (p.expiry() != null) {
                x.text("Expiry", p.expiry());
            }
            if (p.permission() != null) {
                x.text("Permission", p.permission());
            }
            x.close().close();
        }
        x.close();
        if ("HEAD".equals(r.method)) {
            resp.setStatus(200);
        } else {
            AzHttp.xml(resp, 200, x.toString());
        }
    }

    /** Lease state machine shared by containers and blobs. Returns the http status; sets response headers. */
    static int leaseAction(AzReq r, HttpServletResponse resp, Lease l, String what) {
        String action = r.header("x-ms-lease-action");
        if (action == null) {
            throw AzErrors.missingHeader("x-ms-lease-action");
        }
        Instant now = Instant.now();
        String eff = l.effective(now);
        String given = r.header("x-ms-lease-id");
        String proposed = r.header("x-ms-proposed-lease-id");
        String cap = what.equals("blob") ? "Blob" : "Container";
        if (given != null && !isGuid(given)) {
            throw AzErrors.invalidHeader("x-ms-lease-id", given);
        }
        if (proposed != null && !isGuid(proposed)) {
            throw AzErrors.invalidHeader("x-ms-proposed-lease-id", proposed);
        }
        switch (action) {
            case "acquire" -> {
                String dur = r.header("x-ms-lease-duration");
                if (dur == null) {
                    throw AzErrors.missingHeader("x-ms-lease-duration");
                }
                int d;
                try {
                    d = Integer.parseInt(dur);
                } catch (NumberFormatException e) {
                    throw AzErrors.invalidHeader("x-ms-lease-duration", dur);
                }
                if (d != -1 && (d < 15 || d > 60)) {
                    throw AzErrors.invalidHeader("x-ms-lease-duration", dur);
                }
                String id = proposed != null ? proposed : UUID.randomUUID().toString();
                if (eff.equals("leased") || eff.equals("breaking")) {
                    if (proposed != null && proposed.equalsIgnoreCase(l.id) && eff.equals("leased")) {
                        // re-acquiring with the same id renews it with the new duration
                    } else {
                        throw new AzureException(409, "LeaseAlreadyPresent", "There is already a lease present.");
                    }
                }
                l.state = "leased";
                l.id = id;
                l.duration = d;
                l.expiry = d > 0 ? now.plusSeconds(d) : null;
                l.breakAt = null;
                resp.setHeader("x-ms-lease-id", id);
                return 201;
            }
            case "renew" -> {
                if (given == null) {
                    throw AzErrors.missingHeader("x-ms-lease-id");
                }
                if (eff.equals("available") || l.id == null) {
                    throw new AzureException(409, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the "
                            + what + ".");
                }
                if (!given.equalsIgnoreCase(l.id)) {
                    throw new AzureException(409, "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the " + what + ".");
                }
                if (eff.equals("broken") || eff.equals("breaking")) {
                    throw new AzureException(409, "LeaseIsBrokenAndCannotBeRenewed", "The lease ID matched, but the lease has "
                            + "been broken explicitly and cannot be renewed.");
                }
                l.state = "leased";
                l.expiry = l.duration > 0 ? now.plusSeconds(l.duration) : null;
                resp.setHeader("x-ms-lease-id", l.id);
                return 200;
            }
            case "release" -> {
                if (given == null) {
                    throw AzErrors.missingHeader("x-ms-lease-id");
                }
                if (eff.equals("available") || l.id == null) {
                    throw new AzureException(409, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the "
                            + what + ".");
                }
                if (!given.equalsIgnoreCase(l.id)) {
                    throw new AzureException(409, "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the " + what + ".");
                }
                l.state = "available";
                l.id = null;
                l.duration = 0;
                l.expiry = null;
                l.breakAt = null;
                return 200;
            }
            case "change" -> {
                if (given == null) {
                    throw AzErrors.missingHeader("x-ms-lease-id");
                }
                if (proposed == null) {
                    throw AzErrors.missingHeader("x-ms-proposed-lease-id");
                }
                if (eff.equals("available") || l.id == null || eff.equals("expired") || eff.equals("broken")) {
                    throw new AzureException(409, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the "
                            + what + ".");
                }
                if (!given.equalsIgnoreCase(l.id) && !given.equalsIgnoreCase(proposed)) {
                    throw new AzureException(409, "LeaseIdMismatchWithLeaseOperation",
                            "The lease ID specified did not match the lease ID for the " + what + ".");
                }
                l.id = proposed;
                resp.setHeader("x-ms-lease-id", proposed);
                return 200;
            }
            case "break" -> {
                String bp = r.header("x-ms-lease-break-period");
                Integer period = null;
                if (bp != null) {
                    try {
                        period = Integer.parseInt(bp);
                    } catch (NumberFormatException e) {
                        throw AzErrors.invalidHeader("x-ms-lease-break-period", bp);
                    }
                    if (period < 0 || period > 60) {
                        throw AzErrors.invalidHeader("x-ms-lease-break-period", bp);
                    }
                }
                if (eff.equals("available") || l.id == null && !eff.equals("breaking")) {
                    throw new AzureException(409, "LeaseNotPresentWithLeaseOperation", "There is currently no lease on the "
                            + what + ".");
                }
                if (eff.equals("leased") || eff.equals("expired")) {
                    long remaining = l.duration > 0 && l.expiry != null ? Math.max(0, ChronoUnit.SECONDS.between(now, l.expiry))
                            : Long.MAX_VALUE;
                    long p = period == null ? (l.duration < 0 ? 0 : remaining) : Math.min(period, remaining);
                    if (eff.equals("expired")) {
                        p = 0;
                    }
                    l.state = "breaking";
                    l.breakAt = now.plusSeconds(p);
                    resp.setHeader("x-ms-lease-time", String.valueOf(p));
                    return 202;
                }
                if (eff.equals("breaking")) {
                    long remaining = Math.max(0, ChronoUnit.SECONDS.between(now, l.breakAt));
                    long p = period == null ? remaining : Math.min(period, remaining);
                    l.breakAt = now.plusSeconds(p);
                    resp.setHeader("x-ms-lease-time", String.valueOf(p));
                    return 202;
                }
                resp.setHeader("x-ms-lease-time", "0");
                return 202;
            }
            default -> throw AzErrors.invalidHeader("x-ms-lease-action", action);
        }
    }

    private void leaseContainer(AzReq r, HttpServletResponse resp, String container) {
        Cont probe = requireContainer(r.account, container);
        int[] status = new int[1];
        Map<String, String> hdr = new LinkedHashMap<>();
        Cont out = store.mutateContainer(r.account, container, k -> {
            HttpServletResponse cap = new HeaderCapture(resp, hdr);
            status[0] = leaseAction(r, cap, k.lease, "container");
            return k;
        });
        hdr.forEach(resp::setHeader);
        resp.setHeader("ETag", out.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
        resp.setStatus(status[0]);
    }

    /** Collects headers set on a response wrapper so a transaction can be retried/rolled back without side effects. */
    static final class HeaderCapture extends jakarta.servlet.http.HttpServletResponseWrapper {
        private final Map<String, String> headers;

        HeaderCapture(HttpServletResponse r, Map<String, String> headers) {
            super(r);
            this.headers = headers;
        }

        @Override
        public void setHeader(String name, String value) {
            headers.put(name, value);
        }
    }

    // ------------------------------------------------------------------------------------------ list blobs

    private static String encodeMarker(String name, String snapshot) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(("n:" + name + "\u0001" + snapshot)
                .getBytes(StandardCharsets.UTF_8));
    }

    private void listBlobs(AzReq r, HttpServletResponse resp, String container) throws IOException {
        String prefix = r.q("prefix") == null ? "" : r.q("prefix");
        String delimiter = r.q("delimiter") == null ? "" : r.q("delimiter");
        String marker = r.q("marker") == null ? "" : r.q("marker");
        int max = maxResults(r);
        Set<String> inc = include(r);
        for (String i : inc) {
            if (!Set.of("snapshots", "metadata", "uncommittedblobs", "copy", "deleted", "tags", "versions",
                    "deletedwithversions", "immutabilitypolicy", "legalhold", "permissions").contains(i)) {
                throw AzErrors.invalidQuery("include", r.q("include"));
            }
        }
        String fromName = null;
        String fromSnap = "";
        if (!marker.isEmpty()) {
            try {
                String dec = new String(Base64.getUrlDecoder().decode(marker), StandardCharsets.UTF_8);
                if (!dec.startsWith("n:")) {
                    throw new IllegalArgumentException();
                }
                int sep = dec.indexOf('\u0001');
                fromName = dec.substring(2, sep);
                fromSnap = dec.substring(sep + 1);
            } catch (RuntimeException e) {
                throw AzErrors.invalidQuery("marker", marker);
            }
        }
        BlobStore.MergedCursor cur = store.new MergedCursor(r.account, container, prefix, inc.contains("snapshots"),
                fromName, fromSnap);
        AzXml x = new AzXml().openAttrs("EnumerationResults", "ServiceEndpoint", endpoint(r), "ContainerName", container);
        x.textOrEmpty("Prefix", prefix).textOrEmpty("Marker", marker).text("MaxResults", String.valueOf(max));
        if (!delimiter.isEmpty()) {
            x.text("Delimiter", delimiter);
        }
        x.open("Blobs");
        int count = 0;
        Instant now = Instant.now();
        while (count < max) {
            Blob b = cur.peek();
            if (b == null) {
                break;
            }
            if (!delimiter.isEmpty()) {
                String rest = b.name.substring(prefix.length());
                int idx = rest.indexOf(delimiter);
                if (idx >= 0) {
                    String pfx = prefix + rest.substring(0, idx + delimiter.length());
                    x.open("BlobPrefix").text("Name", pfx).close();
                    count++;
                    cur.skipPrefix(pfx);
                    continue;
                }
            }
            cur.next();
            count++;
            x.open("Blob").text("Name", b.name);
            if (!b.snapshot.isEmpty()) {
                x.text("Snapshot", b.snapshot);
            }
            x.open("Properties");
            blobProperties(x, b, now);
            x.close();
            if (inc.contains("metadata") && !b.metadata.isEmpty()) {
                x.open("Metadata");
                b.metadata.forEach(x::text);
                x.close();
            }
            if (inc.contains("tags") && !b.tags.isEmpty()) {
                x.open("Tags").open("TagSet");
                b.tags.forEach((k, v) -> x.open("Tag").text("Key", k).text("Value", v).close());
                x.close().close();
            }
            x.close();
        }
        x.close();
        Blob nxt = cur.peek();
        x.textOrEmpty("NextMarker", nxt == null || count < max ? "" : encodeMarker(nxt.name, nxt.snapshot));
        x.close();
        AzHttp.xml(resp, 200, x.toString());
    }

    static void blobProperties(AzXml x, Blob b, Instant now) {
        x.text("Creation-Time", AzHttp.httpDate(b.createdAt));
        x.text("Last-Modified", AzHttp.httpDate(b.lastModified));
        x.text("Etag", b.etag.replace("\"", ""));
        x.text("Content-Length", String.valueOf(b.size));
        x.text("Content-Type", b.contentType == null ? "application/octet-stream" : b.contentType);
        x.text("Content-Encoding", b.contentEncoding);
        x.text("Content-Language", b.contentLanguage);
        x.text("Content-MD5", b.contentMd5);
        x.text("Cache-Control", b.cacheControl);
        x.text("Content-Disposition", b.contentDisposition);
        if ("PageBlob".equals(b.type)) {
            x.text("x-ms-blob-sequence-number", String.valueOf(b.seq));
        }
        x.text("BlobType", b.type);
        if (b.snapshot.isEmpty()) {
            String e = b.lease.effective(now);
            x.text("LeaseStatus", e.equals("leased") || e.equals("breaking") ? "locked" : "unlocked");
            x.text("LeaseState", e);
            if (e.equals("leased")) {
                x.text("LeaseDuration", b.lease.duration < 0 ? "infinite" : "fixed");
            }
        } else {
            x.text("LeaseStatus", "unlocked").text("LeaseState", "available");
        }
        if (b.copyId != null) {
            x.text("CopyId", b.copyId).text("CopyStatus", b.copyStatus).text("CopySource", b.copySource)
                    .text("CopyProgress", b.size + "/" + b.size);
            if (b.copyCompletion != null) {
                x.text("CopyCompletionTime", AzHttp.httpDate(b.copyCompletion));
            }
        }
        x.text("ServerEncrypted", "true");
        if ("BlockBlob".equals(b.type)) {
            x.text("AccessTier", b.tier == null ? "Hot" : b.tier);
            x.text("AccessTierInferred", String.valueOf(b.tierInferred));
            x.text("AccessTierChangeTime", AzHttp.httpDate(b.tierChanged != null ? b.tierChanged : b.createdAt));
        }
        if (!b.tags.isEmpty()) {
            x.text("TagCount", String.valueOf(b.tags.size()));
        }
        if ("AppendBlob".equals(b.type) && b.sealed) {
            x.text("Sealed", "true");
        }
    }

    // ------------------------------------------------------------------------------------------ blob level

    private void blobLevel(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String blob,
            String comp) throws IOException {
        String m = r.method;
        if (comp == null) {
            switch (m) {
                case "PUT" -> {
                    if (r.header("x-ms-copy-source") != null) {
                        copyBlob(r, resp, auth, container, blob);
                    } else {
                        putBlob(r, resp, auth, container, blob);
                    }
                }
                case "GET", "HEAD" -> getBlob(r, resp, auth, container, blob);
                case "DELETE" -> deleteBlob(r, resp, auth, container, blob);
                default -> requireMethod(r);
            }
            return;
        }
        switch (comp) {
            case "block" -> {
                requireMethod(r, "PUT");
                putBlock(r, resp, auth, container, blob);
            }
            case "blocklist" -> {
                if ("PUT".equals(m)) {
                    putBlockList(r, resp, auth, container, blob);
                } else {
                    requireMethod(r, "GET");
                    getBlockList(r, resp, auth, container, blob);
                }
            }
            case "appendblock" -> {
                requireMethod(r, "PUT");
                appendBlock(r, resp, auth, container, blob);
            }
            case "seal" -> {
                requireMethod(r, "PUT");
                seal(r, resp, auth, container, blob);
            }
            case "page" -> {
                requireMethod(r, "PUT");
                putPage(r, resp, auth, container, blob);
            }
            case "pagelist" -> {
                requireMethod(r, "GET");
                getPageRanges(r, resp, auth, container, blob);
            }
            case "properties" -> {
                requireMethod(r, "PUT");
                setProperties(r, resp, auth, container, blob);
            }
            case "metadata" -> {
                if ("PUT".equals(m)) {
                    setMetadata(r, resp, auth, container, blob);
                } else {
                    requireMethod(r, "GET", "HEAD");
                    r.op = "GetBlobMetadata";
                    getBlob(r, resp, auth, container, blob);
                }
            }
            case "lease" -> {
                requireMethod(r, "PUT");
                leaseBlob(r, resp, auth, container, blob);
            }
            case "snapshot" -> {
                requireMethod(r, "PUT");
                snapshot(r, resp, auth, container, blob);
            }
            case "tier" -> {
                requireMethod(r, "PUT");
                setTier(r, resp, auth, container, blob);
            }
            case "copy" -> {
                requireMethod(r, "PUT");
                r.op = "AbortCopyBlob";
                auth.authorize('o', "w");
                requireContainer(r.account, container);
                if (r.q("copyid") == null) {
                    throw AzErrors.missingHeader("copyid");
                }
                Blob b = store.getBlob(r.account, container, blob, "");
                if (b == null) {
                    throw AzErrors.blobNotFound();
                }
                throw new AzureException(409, "NoPendingCopyOperation", "There is currently no pending copy operation.");
            }
            case "tags" -> {
                if ("PUT".equals(m)) {
                    setTags(r, resp, auth, container, blob);
                } else {
                    requireMethod(r, "GET");
                    getTags(r, resp, auth, container, blob);
                }
            }
            case "undelete" -> throw AzErrors.unsupported("Undelete Blob (soft delete is not implemented)");
            case "expiry" -> throw AzErrors.unsupported("Set Blob Expiry");
            case "query" -> throw AzErrors.unsupported("Query Blob Contents");
            default -> throw new AzureException(400, "InvalidQueryParameterValue", "Value for one of the query parameters "
                    + "specified in the request URI is invalid.").extra("QueryParameterName", "comp")
                    .extra("QueryParameterValue", comp);
        }
    }

    private String host(AzReq r, String container, String blob) {
        return store.ownerHost(r.account, container, blob);
    }

    /** Container must exist; returns it. */
    private Cont container(AzReq r, String container) {
        return requireContainer(r.account, container);
    }

    static String snap(AzReq r) {
        String s = r.q("snapshot");
        if (s == null) {
            return "";
        }
        return s; // an unparsable snapshot simply matches nothing (BlobNotFound), as Azurite does
    }

    // ---- properties from request headers

    private static String h(AzReq r, String primary, String alt) {
        String v = r.header(primary);
        return v != null ? v : (alt == null ? null : r.header(alt));
    }

    private static void applyProperties(AzReq r, Blob b, boolean putBlob) {
        b.contentType = putBlob ? h(r, "x-ms-blob-content-type", "Content-Type") : r.header("x-ms-blob-content-type");
        b.contentEncoding = putBlob ? h(r, "x-ms-blob-content-encoding", "Content-Encoding") : r.header("x-ms-blob-content-encoding");
        b.contentLanguage = putBlob ? h(r, "x-ms-blob-content-language", "Content-Language") : r.header("x-ms-blob-content-language");
        b.cacheControl = putBlob ? h(r, "x-ms-blob-cache-control", "Cache-Control") : r.header("x-ms-blob-cache-control");
        b.contentDisposition = putBlob ? h(r, "x-ms-blob-content-disposition", null) : r.header("x-ms-blob-content-disposition");
        b.contentMd5 = r.header("x-ms-blob-content-md5");
        if (b.contentMd5 != null) {
            validateMd5Format("x-ms-blob-content-md5", b.contentMd5);
        }
    }

    static void validateMd5Format(String header, String v) {
        try {
            if (Base64.getDecoder().decode(v).length != 16) {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException e) {
            throw AzErrors.invalidHeader(header, v);
        }
    }

    static Map<String, String> parseTagsHeader(String v) {
        Map<String, String> m = new LinkedHashMap<>();
        if (v == null || v.isEmpty()) {
            return m;
        }
        for (String pair : v.split("&")) {
            int eq = pair.indexOf('=');
            String k = AzReq.decode(eq < 0 ? pair : pair.substring(0, eq), true);
            String val = eq < 0 ? "" : AzReq.decode(pair.substring(eq + 1), true);
            m.put(k, val);
        }
        BlobTags.validate(m);
        return m;
    }

    private static void applyTier(AzReq r, Blob b) {
        String t = r.header("x-ms-access-tier");
        if (t == null) {
            return;
        }
        validateTier(b.type, t);
        b.tier = t;
        b.tierInferred = false;
        b.tierChanged = now();
    }

    static void validateTier(String type, String t) {
        boolean ok = "PageBlob".equals(type) ? t.matches("P(4|6|10|15|20|30|40|50|60|70|80)") : BLOCK_TIERS.contains(t);
        if ("AppendBlob".equals(type)) {
            throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
        }
        if (!ok) {
            throw AzErrors.invalidHeader("x-ms-access-tier", t);
        }
    }

    private void checkBlobRead(AzReq r, AzureAuth.Result auth, Cont k) {
        if (auth.anonymous()) {
            if ("blob".equals(k.publicAccess) || "container".equals(k.publicAccess)) {
                return;
            }
            throw noAuth();
        }
        auth.authorize('o', "r");
    }

    // ---- Get Blob / properties

    private void getBlob(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String blobName)
            throws IOException {
        boolean head = "HEAD".equals(r.method);
        if (r.op == null || r.op.equals("Unknown")) {
            r.op = head ? "GetBlobProperties" : "GetBlob";
        }
        Cont k = container(r, container);
        checkBlobRead(r, auth, k);
        String snapshot = snap(r);
        Blob b = store.getBlob(r.account, container, blobName, snapshot);
        if (b == null) {
            throw AzErrors.blobNotFound();
        }
        if (snapshot.isEmpty()) {
            checkLease(r, b.lease, false, "blob");
        }
        boolean notModified = conditions(r, true, b.etag, b.lastModified, b.tags, true);
        String rangeHeader = r.header("x-ms-range") != null ? r.header("x-ms-range") : r.header("Range");
        long start = 0;
        long end = b.size - 1;
        boolean ranged = false;
        if (rangeHeader != null && !head && !"metadata".equals(r.q("comp"))) {
            long[] rg = parseRange(rangeHeader, b.size);
            if (rg != null) {
                start = rg[0];
                end = rg[1];
                ranged = true;
            }
        }
        responseProps(resp, b, k);
        if (ranged) {
            resp.setHeader("Content-MD5", null);
        }
        if (notModified) {
            resp.setStatus(304);
            return;
        }
        long len = ranged ? end - start + 1 : b.size;
        boolean metaOnly = "metadata".equals(r.q("comp"));
        if (metaOnly) {
            resp.setStatus(200);
            resp.setHeader("Content-Length", null);
            return;
        }
        resp.setHeader("Content-Length", String.valueOf(b.size == 0 && !ranged ? 0 : len));
        if (ranged) {
            resp.setHeader("Content-Range", "bytes " + start + "-" + end + "/" + b.size);
            resp.setStatus(206);
            if ("true".equalsIgnoreCase(r.header("x-ms-range-get-content-md5"))) {
                if (len > 4L * 1024 * 1024) {
                    throw AzErrors.invalidHeader("x-ms-range-get-content-md5", "true");
                }
                byte[] data = store.readAll(b, start, end);
                resp.setHeader("Content-MD5", Base64.getEncoder().encodeToString(BlobStore.md5().digest(data)));
                if (!head) {
                    resp.getOutputStream().write(data);
                }
                return;
            }
        } else {
            resp.setStatus(200);
        }
        if (head) {
            return;
        }
        if (len > 0) {
            store.stream(b, start, end, resp.getOutputStream());
        }
    }

    /** @return the satisfiable range, or null when the header is malformed / multi-range (Azure then serves the whole blob) */
    static long[] parseRange(String header, long size) {
        String hd = header.trim();
        if (!hd.regionMatches(true, 0, "bytes=", 0, 6)) {
            return null;
        }
        String spec = hd.substring(6).trim();
        int dash = spec.indexOf('-');
        if (dash < 0 || spec.contains(",")) {
            return null;
        }
        try {
            String a = spec.substring(0, dash).trim();
            String bb = spec.substring(dash + 1).trim();
            if (a.isEmpty()) {
                long n = Long.parseLong(bb);
                if (n <= 0 || size == 0) {
                    throw invalidRange();
                }
                return new long[] {Math.max(0, size - n), size - 1};
            }
            long s = Long.parseLong(a);
            long e = bb.isEmpty() ? size - 1 : Long.parseLong(bb);
            if (s < 0 || !bb.isEmpty() && e < s) {
                return null;
            }
            if (s >= size) {
                throw invalidRange();
            }
            return new long[] {s, Math.min(e, size - 1)};
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    static AzureException invalidRange() {
        return new AzureException(416, "InvalidRange", "The range specified is invalid for the current size of the resource.");
    }

    private void responseProps(HttpServletResponse resp, Blob b, Cont k) {
        Instant now = Instant.now();
        resp.setHeader("Last-Modified", AzHttp.httpDate(b.lastModified));
        resp.setHeader("ETag", b.etag);
        resp.setHeader("Content-Type", b.contentType == null ? "application/octet-stream" : b.contentType);
        if (b.contentEncoding != null) {
            resp.setHeader("Content-Encoding", b.contentEncoding);
        }
        if (b.contentLanguage != null) {
            resp.setHeader("Content-Language", b.contentLanguage);
        }
        if (b.cacheControl != null) {
            resp.setHeader("Cache-Control", b.cacheControl);
        }
        if (b.contentDisposition != null) {
            resp.setHeader("Content-Disposition", b.contentDisposition);
        }
        if (b.contentMd5 != null) {
            resp.setHeader("Content-MD5", b.contentMd5);
        }
        resp.setHeader("Accept-Ranges", "bytes");
        resp.setHeader("x-ms-blob-type", b.type);
        resp.setHeader("x-ms-creation-time", AzHttp.httpDate(b.createdAt));
        writeMetadata(resp, b.metadata);
        if (b.snapshot.isEmpty()) {
            writeLease(resp, b.lease, now);
        } else {
            resp.setHeader("x-ms-lease-status", "unlocked");
            resp.setHeader("x-ms-lease-state", "available");
        }
        if (b.copyId != null) {
            resp.setHeader("x-ms-copy-id", b.copyId);
            resp.setHeader("x-ms-copy-source", b.copySource);
            resp.setHeader("x-ms-copy-status", b.copyStatus);
            resp.setHeader("x-ms-copy-progress", b.size + "/" + b.size);
            if (b.copyCompletion != null) {
                resp.setHeader("x-ms-copy-completion-time", AzHttp.httpDate(b.copyCompletion));
            }
        }
        resp.setHeader("x-ms-server-encrypted", "true");
        if ("AppendBlob".equals(b.type)) {
            resp.setHeader("x-ms-blob-committed-block-count", String.valueOf(b.segments.size()));
            if (b.sealed) {
                resp.setHeader("x-ms-blob-sealed", "true");
            }
        }
        if ("PageBlob".equals(b.type)) {
            resp.setHeader("x-ms-blob-sequence-number", String.valueOf(b.seq));
        }
        if ("BlockBlob".equals(b.type)) {
            resp.setHeader("x-ms-access-tier", b.tier == null ? "Hot" : b.tier);
            resp.setHeader("x-ms-access-tier-inferred", String.valueOf(b.tierInferred));
            if (b.tierChanged != null) {
                resp.setHeader("x-ms-access-tier-change-time", AzHttp.httpDate(b.tierChanged));
            }
        }
        if (!b.tags.isEmpty()) {
            resp.setHeader("x-ms-tag-count", String.valueOf(b.tags.size()));
        }
    }

    // ------------------------------------------------------------------------------------------ Put Blob

    static void requireContentLength(AzReq r) {
        if (r.header("Content-Length") == null && !"chunked".equalsIgnoreCase(r.header("Transfer-Encoding"))) {
            throw new AzureException(411, "MissingContentLengthHeader", "The Content-Length header was not specified.");
        }
    }

    private void verifyMd5(AzReq r, BlobStore.Ingested in, String host) {
        String want = r.header("Content-MD5");
        if (want != null && !want.equals(in.md5())) {
            store.dropData(host, List.of(in.dataId()));
            throw new AzureException(400, "Md5Mismatch", "The MD5 value specified in the request did not match with the MD5 "
                    + "value calculated by the server.").extra("UserSpecifiedMd5", want).extra("ServerCalculatedMd5", in.md5());
        }
    }

    private void putBlob(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "PutBlob";
        r.write = true;
        auth.authorize('o', "cw");
        String type = r.header("x-ms-blob-type");
        if (type == null) {
            throw AzErrors.missingHeader("x-ms-blob-type");
        }
        if (!type.equals("BlockBlob") && !type.equals("AppendBlob") && !type.equals("PageBlob")) {
            throw AzErrors.invalidHeader("x-ms-blob-type", type);
        }
        requireContentLength(r);
        container(r, container);
        Blob b = new Blob();
        b.account = r.account;
        b.container = container;
        b.name = name;
        b.type = type;
        applyProperties(r, b, true);
        b.metadata = metadata(r);
        b.tags = parseTagsHeader(r.header("x-ms-tags"));
        if ("BlockBlob".equals(type)) {
            applyTier(r, b);
        }
        long pageLen = 0;
        if ("PageBlob".equals(type)) {
            String cl = r.header("x-ms-blob-content-length");
            if (cl == null) {
                throw AzErrors.missingHeader("x-ms-blob-content-length");
            }
            try {
                pageLen = Long.parseLong(cl);
            } catch (NumberFormatException e) {
                throw AzErrors.invalidHeader("x-ms-blob-content-length", cl);
            }
            if (pageLen < 0 || pageLen % 512 != 0) {
                throw AzErrors.invalidHeader("x-ms-blob-content-length", cl);
            }
            String sn = r.header("x-ms-blob-sequence-number");
            if (sn != null) {
                try {
                    b.seq = Long.parseLong(sn);
                } catch (NumberFormatException e) {
                    throw AzErrors.invalidHeader("x-ms-blob-sequence-number", sn);
                }
            }
        }
        String host = host(r, container, name);
        // fail fast on the existing blob before streaming a large body
        Blob existing = store.getBlob(r.account, container, name, "");
        if (existing != null) {
            checkLease(r, existing.lease, true, "blob");
        }
        conditions(r, existing != null, existing == null ? null : existing.etag, existing == null ? null : existing.lastModified,
                existing == null ? null : existing.tags, false);
        BlobStore.Ingested in = null;
        if ("BlockBlob".equals(type)) {
            in = store.ingest(host, r.raw.getInputStream(), MAX_PUT_BLOB, r.account, container, name, "", "T", null);
            verifyMd5(r, in, host);
            b.size = in.size();
            if (b.contentMd5 == null) {
                b.contentMd5 = in.md5();
            }
            if (in.size() > 0) {
                Seg s = new Seg();
                s.d = in.dataId();
                s.n = in.size();
                b.segments.add(s);
            }
        } else {
            b.size = "PageBlob".equals(type) ? pageLen : 0;
            drain(r);
        }
        b.etag = AzHttp.newEtag();
        b.createdAt = now();
        b.lastModified = b.createdAt;
        final BlobStore.Ingested fin = in;
        try {
            store.shards.tx(host, c -> {
                Blob cur = BlobStore.selectBlob(c, r.account, container, name, "", true);
                if (cur != null) {
                    checkLease(r, cur.lease, true, "blob");
                    conditions(r, true, cur.etag, cur.lastModified, cur.tags, false);
                } else {
                    conditions(r, false, null, null, null, false);
                }
                BlobStore.dropOwned(c, r.account, container, name, "", fin == null || fin.size() == 0 ? null
                        : List.of(fin.dataId()));
                if (fin != null && fin.size() > 0) {
                    BlobStore.adopt(c, fin.dataId(), "C", "");
                } else if (fin != null) {
                    BlobStore.dropData(c, List.of(fin.dataId()));
                }
                if (cur != null) {
                    // an overwritten blob keeps its lease
                    b.lease = cur.lease;
                }
                BlobStore.upsertBlob(c, b);
                return null;
            });
        } catch (RuntimeException e) {
            if (fin != null) {
                store.dropData(host, List.of(fin.dataId()));
            }
            throw e;
        }
        resp.setHeader("ETag", b.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(b.lastModified));
        if ("BlockBlob".equals(type) && b.contentMd5 != null) {
            resp.setHeader("Content-MD5", in.md5());
        }
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(201);
    }

    private static void drain(AzReq r) throws IOException {
        InputStream in = r.raw.getInputStream();
        byte[] buf = new byte[8192];
        while (in.read(buf) >= 0) {
            // discard
        }
    }

    // ------------------------------------------------------------------------------------------ blocks

    private static String decodeBlockId(String id) {
        if (id == null) {
            throw AzErrors.missingHeader("blockid");
        }
        try {
            byte[] raw = Base64.getDecoder().decode(id);
            if (raw.length == 0 || raw.length > 64) {
                throw new IllegalArgumentException();
            }
            return id;
        } catch (IllegalArgumentException e) {
            throw AzErrors.invalidQuery("blockid", id);
        }
    }

    private void putBlock(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "PutBlock";
        r.write = true;
        auth.authorize('o', "cw");
        String blockId = decodeBlockId(r.q("blockid"));
        requireContentLength(r);
        container(r, container);
        String host = host(r, container, name);
        Blob existing = store.getBlob(r.account, container, name, "");
        if (existing != null) {
            if (!"BlockBlob".equals(existing.type)) {
                throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
            }
            checkLease(r, existing.lease, true, "blob");
        }
        BlobStore.Ingested in = store.ingest(host, r.raw.getInputStream(), MAX_BLOCK, r.account, container, name, "", "U", blockId);
        verifyMd5(r, in, host);
        try {
            store.shards.tx(host, c -> {
                List<BlobStore.BlockRow> all = BlobStore.blocks(c, r.account, container, name, "U,C");
                int len = Base64.getDecoder().decode(blockId).length;
                for (BlobStore.BlockRow br : all) {
                    if (br.blockId() != null && !br.dataId().equals(in.dataId())
                            && Base64.getDecoder().decode(br.blockId()).length != len) {
                        throw new AzureException(400, "InvalidBlobOrBlock", "The specified blob or block content is invalid.");
                    }
                }
                List<String> old = new ArrayList<>();
                for (BlobStore.BlockRow br : all) {
                    if ("U".equals(br.kind()) && blockId.equals(br.blockId()) && !br.dataId().equals(in.dataId())) {
                        old.add(br.dataId());
                    }
                }
                BlobStore.dropData(c, old);
                return null;
            });
        } catch (RuntimeException e) {
            store.dropData(host, List.of(in.dataId()));
            throw e;
        }
        resp.setHeader("Content-MD5", in.md5());
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(201);
    }

    private void putBlockList(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "PutBlockList";
        r.write = true;
        auth.authorize('o', "cw");
        container(r, container);
        byte[] listBody = r.raw.getInputStream().readAllBytes();
        List<String[]> list = AzServiceProps.parseBlockList(listBody);
        Blob nb = new Blob();
        nb.account = r.account;
        nb.container = container;
        nb.name = name;
        nb.type = "BlockBlob";
        applyProperties(r, nb, false);
        nb.metadata = metadata(r);
        nb.tags = parseTagsHeader(r.header("x-ms-tags"));
        applyTier(r, nb);
        String host = host(r, container, name);
        Blob result = store.shards.tx(host, c -> {
            Blob cur = BlobStore.selectBlob(c, r.account, container, name, "", true);
            if (cur != null) {
                if (!"BlockBlob".equals(cur.type)) {
                    throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
                }
                checkLease(r, cur.lease, true, "blob");
            }
            conditions(r, cur != null, cur == null ? null : cur.etag, cur == null ? null : cur.lastModified,
                    cur == null ? null : cur.tags, false);
            List<BlobStore.BlockRow> rows = BlobStore.blocks(c, r.account, container, name, "U,C");
            Map<String, BlobStore.BlockRow> uncommitted = new LinkedHashMap<>();
            Map<String, Seg> committed = new LinkedHashMap<>();
            for (BlobStore.BlockRow br : rows) {
                if ("U".equals(br.kind())) {
                    uncommitted.put(br.blockId(), br);
                }
            }
            if (cur != null) {
                for (Seg s : cur.segments) {
                    committed.put(s.id, s);
                }
            }
            if (list.size() > 50000) {
                throw new AzureException(400, "BlockCountExceedsLimit", "The committed block count cannot exceed the "
                        + "maximum limit of 50,000 blocks.");
            }
            List<Seg> segs = new ArrayList<>();
            Set<String> keep = new HashSet<>();
            long total = 0;
            for (String[] e : list) {
                String id = e[0];
                String kind = e[1];
                Seg s = new Seg();
                s.id = id;
                BlobStore.BlockRow u = uncommitted.get(id);
                Seg cs = committed.get(id);
                if ("Uncommitted".equals(kind)) {
                    if (u == null) {
                        throw invalidBlockList("Specified block or blocks are invalid: " + id);
                    }
                    s.d = u.dataId();
                    s.n = u.size();
                } else if ("Committed".equals(kind)) {
                    if (cs == null) {
                        throw invalidBlockList("Specified block or blocks are invalid: " + id);
                    }
                    s.d = cs.d;
                    s.n = cs.n;
                } else {
                    if (u != null) {
                        s.d = u.dataId();
                        s.n = u.size();
                    } else if (cs != null) {
                        s.d = cs.d;
                        s.n = cs.n;
                    } else {
                        throw invalidBlockList("Specified block or blocks are invalid: " + id);
                    }
                }
                keep.add(s.d);
                total += s.n;
                segs.add(s);
            }
            BlobStore.dropOwned(c, r.account, container, name, "", keep);
            for (String d : keep) {
                BlobStore.adopt(c, d, "C", "");
            }
            nb.segments = segs;
            nb.size = total;
            nb.etag = AzHttp.newEtag();
            nb.createdAt = now();
            nb.lastModified = nb.createdAt;
            if (cur != null) {
                nb.lease = cur.lease;
            }
            BlobStore.upsertBlob(c, nb);
            return nb;
        });
        resp.setHeader("ETag", result.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(result.lastModified));
        resp.setHeader("Content-MD5", Base64.getEncoder().encodeToString(BlobStore.md5().digest(listBody)));
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(201);
    }

    static AzureException invalidBlockList(String msg) {
        return new AzureException(400, "InvalidBlockList", "The specified block list is invalid.");
    }

    private void getBlockList(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "GetBlockList";
        Cont k = container(r, container);
        checkBlobRead(r, auth, k);
        String type0 = r.q("blocklisttype") == null ? "committed" : r.q("blocklisttype").toLowerCase(Locale.ROOT);
        String type = type0;
        if (!type.equals("committed") && !type.equals("uncommitted") && !type.equals("all")) {
            throw AzErrors.invalidQuery("blocklisttype", r.q("blocklisttype"));
        }
        String snapshot = snap(r);
        Blob b = store.getBlob(r.account, container, name, snapshot);
        List<BlobStore.BlockRow> unc = new ArrayList<>();
        if (type.equals("uncommitted") || type.equals("all")) {
            unc = store.shards.conn(host(r, container, name),
                    c -> BlobStore.blocks(c, r.account, container, name, "U"));
        }
        if (b == null) {
            List<BlobStore.BlockRow> staged = unc.isEmpty() ? store.shards.conn(host(r, container, name),
                    c -> BlobStore.blocks(c, r.account, container, name, "U")) : unc;
            if (staged.isEmpty()) {
                throw AzErrors.blobNotFound();
            }
        }
        if (b != null) {
            checkLease(r, b.lease, false, "blob");
            conditions(r, true, b.etag, b.lastModified, b.tags, true);
            if (!"BlockBlob".equals(b.type)) {
                throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
            }
            resp.setHeader("Last-Modified", AzHttp.httpDate(b.lastModified));
            resp.setHeader("ETag", b.etag);
            resp.setHeader("x-ms-blob-content-length", String.valueOf(b.size));
        }
        if (b == null) {
            resp.setHeader("ETag", AzHttp.newEtag());
            resp.setHeader("Last-Modified", AzHttp.httpDate(Instant.now()));
            resp.setHeader("x-ms-blob-content-length", "0");
            type = "all";
        }
        AzXml x = new AzXml().open("BlockList");
        if (!type.equals("uncommitted")) {
            x.open("CommittedBlocks");
            if (b != null) {
                for (Seg s : b.segments) {
                    x.open("Block").text("Name", s.id).text("Size", String.valueOf(s.n)).close();
                }
            }
            x.close();
        }
        if (!type.equals("committed")) {
            x.open("UncommittedBlocks");
            for (BlobStore.BlockRow u : unc) {
                x.open("Block").text("Name", u.blockId()).text("Size", String.valueOf(u.size())).close();
            }
            x.close();
        }
        x.close();
        AzHttp.xml(resp, 200, x.toString());
    }

    // ------------------------------------------------------------------------------------------ append blobs

    private void appendBlock(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "AppendBlock";
        r.write = true;
        auth.authorize('o', "aw");
        requireContentLength(r);
        container(r, container);
        String host = host(r, container, name);
        Blob pre = store.getBlob(r.account, container, name, "");
        if (pre == null) {
            throw AzErrors.blobNotFound();
        }
        if (!"AppendBlob".equals(pre.type)) {
            throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
        }
        checkLease(r, pre.lease, true, "blob");
        BlobStore.Ingested in = store.ingest(host, r.raw.getInputStream(), MAX_APPEND_BLOCK, r.account, container, name, "",
                "T", null);
        verifyMd5(r, in, host);
        long[] offset = new long[1];
        try {
            Blob out = store.shards.tx(host, c -> {
                Blob b = BlobStore.selectBlob(c, r.account, container, name, "", true);
                if (b == null) {
                    throw AzErrors.blobNotFound();
                }
                if (b.sealed) {
                    throw new AzureException(409, "BlobIsSealed", "The specified blob is sealed, and its contents can't be modified unless the blob is re-created after a delete.");
                }
                checkLease(r, b.lease, true, "blob");
                conditions(r, true, b.etag, b.lastModified, b.tags, false);
                String pos = r.header("x-ms-blob-condition-appendpos");
                if (pos != null && Long.parseLong(pos) != b.size) {
                    throw new AzureException(412, "AppendPositionConditionNotMet",
                            "The append position condition specified was not met.");
                }
                String maxs = r.header("x-ms-blob-condition-maxsize");
                if (maxs != null && b.size + in.size() > Long.parseLong(maxs)) {
                    throw new AzureException(412, "MaxBlobSizeConditionNotMet", "The max blob size condition specified was not met.");
                }
                if (b.segments.size() >= 50000) {
                    throw new AzureException(409, "BlockCountExceedsLimit", "The committed block count cannot exceed the "
                            + "maximum limit of 50,000 blocks.");
                }
                offset[0] = b.size;
                if (in.size() > 0) {
                    Seg s = new Seg();
                    s.d = in.dataId();
                    s.n = in.size();
                    b.segments.add(s);
                    BlobStore.adopt(c, in.dataId(), "C", "");
                } else {
                    BlobStore.dropData(c, List.of(in.dataId()));
                    Seg s = new Seg();
                    s.d = UUID.randomUUID().toString();
                    s.n = 0;
                    b.segments.add(s);
                }
                b.size += in.size();
                b.etag = AzHttp.newEtag();
                b.lastModified = now();
                BlobStore.upsertBlob(c, b);
                return b;
            });
            resp.setHeader("ETag", out.etag);
            resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
            resp.setHeader("x-ms-blob-append-offset", String.valueOf(offset[0]));
            resp.setHeader("x-ms-blob-committed-block-count", String.valueOf(out.segments.size()));
        } catch (RuntimeException e) {
            store.dropData(host, List.of(in.dataId()));
            throw e;
        }
        resp.setHeader("Content-MD5", in.md5());
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(201);
    }

    private void seal(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "SealAppendBlob";
        r.write = true;
        auth.authorize('o', "w");
        container(r, container);
        Blob out = store.shards.tx(host(r, container, name), c -> {
            Blob b = BlobStore.selectBlob(c, r.account, container, name, "", true);
            if (b == null) {
                throw AzErrors.blobNotFound();
            }
            if (!"AppendBlob".equals(b.type)) {
                throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
            }
            checkLease(r, b.lease, true, "blob");
            conditions(r, true, b.etag, b.lastModified, b.tags, false);
            if (b.sealed) {
                return b;
            }
            b.sealed = true;
            b.etag = AzHttp.newEtag();
            b.lastModified = now();
            BlobStore.upsertBlob(c, b);
            return b;
        });
        resp.setHeader("ETag", out.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
        resp.setHeader("x-ms-blob-sealed", "true");
        resp.setStatus(200);
    }

    // ------------------------------------------------------------------------------------------ page blobs

    private long[] pageRange(AzReq r, long size, boolean forWrite) {
        String rh = r.header("x-ms-range") != null ? r.header("x-ms-range") : r.header("Range");
        if (rh == null) {
            throw AzErrors.missingHeader("x-ms-range");
        }
        java.util.regex.Matcher m = Pattern.compile("^bytes=(\\d+)-(\\d+)$").matcher(rh.trim());
        if (!m.matches()) {
            throw AzErrors.invalidHeader("x-ms-range", rh);
        }
        long s = Long.parseLong(m.group(1));
        long e = Long.parseLong(m.group(2));
        if (forWrite && (s % 512 != 0 || (e + 1) % 512 != 0) || e < s) {
            throw new AzureException(416, "InvalidPageRange", "The page range specified is invalid.");
        }
        if (forWrite && e >= size) {
            throw new AzureException(416, "InvalidPageRange", "The page range specified is invalid.");
        }
        return new long[] {s, e};
    }

    private void putPage(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "PutPage";
        r.write = true;
        auth.authorize('o', "w");
        String action = r.header("x-ms-page-write");
        if (action == null) {
            throw AzErrors.missingHeader("x-ms-page-write");
        }
        if (!action.equalsIgnoreCase("update") && !action.equalsIgnoreCase("clear")) {
            throw AzErrors.invalidHeader("x-ms-page-write", action);
        }
        boolean clear = action.equalsIgnoreCase("clear");
        container(r, container);
        String host = host(r, container, name);
        Blob pre = store.getBlob(r.account, container, name, "");
        if (pre == null) {
            throw AzErrors.blobNotFound();
        }
        if (!"PageBlob".equals(pre.type)) {
            throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
        }
        checkLease(r, pre.lease, true, "blob");
        long[] rg = pageRange(r, pre.size, true);
        long len = rg[1] - rg[0] + 1;
        if (len > MAX_PAGE_WRITE) {
            throw new AzureException(413, "RequestBodyTooLarge", "The request body is too large and exceeds the maximum "
                    + "permissible limit.");
        }
        BlobStore.Ingested in = null;
        if (!clear) {
            String cl = r.header("Content-Length");
            if (cl == null || Long.parseLong(cl) != len) {
                throw AzErrors.invalidHeader("Content-Length", cl);
            }
            in = store.ingest(host, r.raw.getInputStream(), MAX_PAGE_WRITE, r.account, container, name, "", "T", null);
            verifyMd5(r, in, host);
        } else {
            drain(r);
        }
        final BlobStore.Ingested fin = in;
        try {
            Blob out = store.shards.tx(host, c -> {
                Blob b = BlobStore.selectBlob(c, r.account, container, name, "", true);
                if (b == null) {
                    throw AzErrors.blobNotFound();
                }
                checkLease(r, b.lease, true, "blob");
                conditions(r, true, b.etag, b.lastModified, b.tags, false);
                if (rg[1] >= b.size) {
                    throw new AzureException(416, "InvalidPageRange", "The page range specified is invalid.");
                }
                List<Seg> next = new ArrayList<>();
                for (Seg s : b.segments) {
                    long sEnd = s.o + s.n - 1;
                    if (sEnd < rg[0] || s.o > rg[1]) {
                        next.add(s);
                        continue;
                    }
                    if (s.o < rg[0]) {
                        Seg left = copySeg(s);
                        left.n = rg[0] - s.o;
                        next.add(left);
                    }
                    if (sEnd > rg[1]) {
                        Seg right = copySeg(s);
                        right.o = rg[1] + 1;
                        right.s = s.s + (rg[1] + 1 - s.o);
                        right.n = sEnd - rg[1];
                        next.add(right);
                    }
                }
                if (fin != null) {
                    Seg ns = new Seg();
                    ns.d = fin.dataId();
                    ns.o = rg[0];
                    ns.n = len;
                    ns.s = 0;
                    next.add(ns);
                    BlobStore.adopt(c, fin.dataId(), "P", "");
                }
                next.sort(java.util.Comparator.comparingLong(s -> s.o));
                Set<String> keep = new HashSet<>();
                for (Seg s : next) {
                    keep.add(s.d);
                }
                BlobStore.dropOwned(c, r.account, container, name, "", keep);
                b.segments = next;
                b.seq = b.seq;
                b.etag = AzHttp.newEtag();
                b.lastModified = now();
                BlobStore.upsertBlob(c, b);
                return b;
            });
            resp.setHeader("ETag", out.etag);
            resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
            resp.setHeader("x-ms-blob-sequence-number", String.valueOf(out.seq));
        } catch (RuntimeException e) {
            if (fin != null) {
                store.dropData(host, List.of(fin.dataId()));
            }
            throw e;
        }
        if (in != null) {
            resp.setHeader("Content-MD5", in.md5());
        }
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(201);
    }

    private static Seg copySeg(Seg s) {
        Seg n = new Seg();
        n.id = s.id;
        n.d = s.d;
        n.n = s.n;
        n.o = s.o;
        n.s = s.s;
        return n;
    }

    private void getPageRanges(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) throws IOException {
        r.op = "GetPageRanges";
        Cont k = container(r, container);
        checkBlobRead(r, auth, k);
        Blob b = store.getBlob(r.account, container, name, snap(r));
        if (b == null) {
            throw AzErrors.blobNotFound();
        }
        if (!"PageBlob".equals(b.type)) {
            throw new AzureException(409, "InvalidBlobType", "The blob type is invalid for this operation.");
        }
        checkLease(r, b.lease, false, "blob");
        conditions(r, true, b.etag, b.lastModified, b.tags, true);
        long lo = 0;
        long hi = Math.max(0, b.size - 1);
        String rh = r.header("x-ms-range") != null ? r.header("x-ms-range") : r.header("Range");
        if (rh != null) {
            long[] rg = pageRange(r, b.size, false);
            lo = rg[0];
            hi = rg[1];
        }
        List<long[]> ranges = new ArrayList<>();
        List<Seg> segs = new ArrayList<>(b.segments);
        segs.sort(java.util.Comparator.comparingLong(s -> s.o));
        for (Seg s : segs) {
            long a = Math.max(s.o, lo);
            long e = Math.min(s.o + s.n - 1, hi);
            if (a > e) {
                continue;
            }
            if (!ranges.isEmpty() && ranges.get(ranges.size() - 1)[1] + 1 == a) {
                ranges.get(ranges.size() - 1)[1] = e;
            } else {
                ranges.add(new long[] {a, e});
            }
        }
        resp.setHeader("Last-Modified", AzHttp.httpDate(b.lastModified));
        resp.setHeader("ETag", b.etag);
        resp.setHeader("x-ms-blob-content-length", String.valueOf(b.size));
        AzXml x = new AzXml().open("PageList");
        for (long[] rg : ranges) {
            x.open("PageRange").text("Start", String.valueOf(rg[0])).text("End", String.valueOf(rg[1])).close();
        }
        x.close();
        AzHttp.xml(resp, 200, x.toString());
    }

    // ------------------------------------------------------------------------------------------ properties / metadata / lease

    /** Locks a blob row, applies lease + conditions, lets {@code fn} mutate it, stores and returns it. */
    private Blob mutate(AzReq r, String container, String name, boolean checkLease, boolean conds,
            java.util.function.Function<Blob, Blob> fn) {
        container(r, container);
        return store.shards.tx(host(r, container, name), c -> {
            Blob b = BlobStore.selectBlob(c, r.account, container, name, "", true);
            if (b == null) {
                throw AzErrors.blobNotFound();
            }
            if (checkLease) {
                checkLease(r, b.lease, true, "blob");
            }
            if (conds) {
                conditions(r, true, b.etag, b.lastModified, b.tags, false);
            }
            Blob out = fn.apply(b);
            BlobStore.upsertBlob(c, out);
            return out;
        });
    }

    private void setProperties(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "SetBlobProperties";
        r.write = true;
        auth.authorize('o', "w");
        String cl = r.header("x-ms-blob-content-length");
        Blob out = mutate(r, container, name, true, true, b -> {
            applyProperties(r, b, false);
            if (cl != null) {
                if (!"PageBlob".equals(b.type)) {
                    throw AzErrors.invalidHeader("x-ms-blob-content-length", cl);
                }
                long n;
                try {
                    n = Long.parseLong(cl);
                } catch (NumberFormatException e) {
                    throw AzErrors.invalidHeader("x-ms-blob-content-length", cl);
                }
                if (n < 0 || n % 512 != 0) {
                    throw AzErrors.invalidHeader("x-ms-blob-content-length", cl);
                }
                if (n < b.size) {
                    List<Seg> keep = new ArrayList<>();
                    for (Seg s : b.segments) {
                        if (s.o >= n) {
                            continue;
                        }
                        if (s.o + s.n > n) {
                            Seg t = copySeg(s);
                            t.n = n - s.o;
                            keep.add(t);
                        } else {
                            keep.add(s);
                        }
                    }
                    b.segments = keep;
                }
                b.size = n;
            }
            String sn = r.header("x-ms-sequence-number-action");
            if (sn != null && "PageBlob".equals(b.type)) {
                String v = r.header("x-ms-blob-sequence-number");
                long val = v == null ? 0 : Long.parseLong(v);
                switch (sn) {
                    case "update" -> b.seq = val;
                    case "max" -> b.seq = Math.max(b.seq, val);
                    case "increment" -> b.seq++;
                    default -> throw AzErrors.invalidHeader("x-ms-sequence-number-action", sn);
                }
            }
            b.etag = AzHttp.newEtag();
            b.lastModified = now();
            return b;
        });
        resp.setHeader("ETag", out.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
        if ("PageBlob".equals(out.type)) {
            resp.setHeader("x-ms-blob-sequence-number", String.valueOf(out.seq));
        }
        resp.setStatus(200);
    }

    private void setMetadata(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "SetBlobMetadata";
        r.write = true;
        auth.authorize('o', "w");
        Map<String, String> md = metadata(r);
        Blob out = mutate(r, container, name, true, true, b -> {
            b.metadata = md;
            b.etag = AzHttp.newEtag();
            b.lastModified = now();
            return b;
        });
        resp.setHeader("ETag", out.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(200);
    }

    private void leaseBlob(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "BlobLease";
        r.write = true;
        auth.authorize('o', "w");
        if (r.q("snapshot") != null) {
            throw new AzureException(400, "InvalidQueryParameterValue", "Leases cannot be taken on snapshots.")
                    .extra("QueryParameterName", "snapshot").extra("QueryParameterValue", r.q("snapshot"));
        }
        int[] status = new int[1];
        Map<String, String> hdr = new LinkedHashMap<>();
        Blob out = mutate(r, container, name, false, true, b -> {
            status[0] = leaseAction(r, new HeaderCapture(resp, hdr), b.lease, "blob");
            return b;
        });
        hdr.forEach(resp::setHeader);
        resp.setHeader("ETag", out.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(out.lastModified));
        resp.setStatus(status[0]);
    }

    private void setTier(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "SetBlobTier";
        r.write = true;
        auth.authorize('o', "w");
        String t = r.header("x-ms-access-tier");
        if (t == null) {
            throw AzErrors.missingHeader("x-ms-access-tier");
        }
        mutate(r, container, name, true, false, b -> {
            validateTier(b.type, t);
            b.tier = t;
            b.tierInferred = false;
            b.tierChanged = now();
            return b;
        });
        resp.setStatus(200);
    }

    // ------------------------------------------------------------------------------------------ tags

    private void setTags(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "SetBlobTags";
        r.write = true;
        auth.authorize('o', "t");
        Map<String, String> tags = AzServiceProps.parseTags(r.raw.getInputStream().readAllBytes());
        BlobTags.validate(tags);
        mutate(r, container, name, false, false, b -> {
            String ifTags = r.header("x-ms-if-tags");
            if (ifTags != null && !BlobTags.matches(ifTags, b.tags)) {
                throw AzErrors.conditionNotMet();
            }
            checkLeaseOptional(r, b);
            b.tags = tags;
            return b;
        });
        resp.setStatus(204);
    }

    private static void checkLeaseOptional(AzReq r, Blob b) {
        if (r.header("x-ms-lease-id") != null) {
            checkLease(r, b.lease, false, "blob");
        }
    }

    private void getTags(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name)
            throws IOException {
        r.op = "GetBlobTags";
        auth.authorize('o', "t");
        container(r, container);
        Blob b = store.getBlob(r.account, container, name, snap(r));
        if (b == null) {
            throw AzErrors.blobNotFound();
        }
        String ifTags = r.header("x-ms-if-tags");
        if (ifTags != null && !BlobTags.matches(ifTags, b.tags)) {
            throw AzErrors.conditionNotMet();
        }
        AzXml x = new AzXml().open("Tags").open("TagSet");
        b.tags.forEach((k, v) -> x.open("Tag").text("Key", k).text("Value", v).close());
        x.close().close();
        AzHttp.xml(resp, 200, x.toString());
    }

    // ------------------------------------------------------------------------------------------ snapshot

    static String snapshotTime(Instant t) {
        long ticks = t.getNano() / 100;
        return java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.US)
                .withZone(java.time.ZoneOffset.UTC).format(t) + "." + String.format("%07d", ticks) + "Z";
    }

    private void snapshot(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "SnapshotBlob";
        r.write = true;
        auth.authorize('o', "cw");
        container(r, container);
        Map<String, String> md = metadata(r);
        String host = host(r, container, name);
        String[] snapTs = new String[1];
        Blob base = store.shards.tx(host, c -> {
            Blob b = BlobStore.selectBlob(c, r.account, container, name, "", true);
            if (b == null) {
                throw AzErrors.blobNotFound();
            }
            checkLease(r, b.lease, false, "blob");
            if (r.header("x-ms-lease-id") != null) {
                checkLease(r, b.lease, true, "blob");
            }
            conditions(r, true, b.etag, b.lastModified, b.tags, false);
            Instant t = Instant.now();
            String ts = snapshotTime(t);
            while (BlobStore.selectBlob(c, r.account, container, name, ts, false) != null) {
                t = t.plusNanos(100);
                ts = snapshotTime(t);
            }
            Blob sn = b.copyShallow();
            sn.snapshot = ts;
            sn.segments = BlobStore.duplicateSegments(c, b, r.account, container, name, ts);
            if (!md.isEmpty()) {
                sn.metadata = md;
            }
            BlobStore.upsertBlob(c, sn);
            snapTs[0] = ts;
            return b;
        });
        resp.setHeader("x-ms-snapshot", snapTs[0]);
        resp.setHeader("ETag", base.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(base.lastModified));
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(201);
    }

    // ------------------------------------------------------------------------------------------ delete

    private void deleteBlob(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "DeleteBlob";
        r.write = true;
        auth.authorize('o', "d");
        container(r, container);
        String snapshot = snap(r);
        String mode = r.header("x-ms-delete-snapshots");
        if (mode != null && !mode.equals("include") && !mode.equals("only")) {
            throw AzErrors.invalidHeader("x-ms-delete-snapshots", mode);
        }
        store.shards.tx(host(r, container, name), c -> {
            Blob b = BlobStore.selectBlob(c, r.account, container, name, snapshot, true);
            if (b == null) {
                throw AzErrors.blobNotFound();
            }
            if (snapshot.isEmpty()) {
                checkLease(r, b.lease, true, "blob");
                conditions(r, true, b.etag, b.lastModified, b.tags, false);
                int snaps = 0;
                try (java.sql.PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM warp_azblob_blobs WHERE "
                        + "account=? AND container=? AND name=? AND snapshot<>''")) {
                    ps.setString(1, r.account);
                    ps.setString(2, container);
                    ps.setString(3, name);
                    try (java.sql.ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        snaps = rs.getInt(1);
                    }
                }
                if (mode == null && snaps > 0) {
                    throw new AzureException(409, "SnapshotsPresent", "This operation is not permitted while the blob has "
                            + "snapshots.");
                }
                if ("only".equals(mode)) {
                    BlobStore.exec(c, "DELETE FROM warp_azblob_data WHERE data_id IN (SELECT data_id FROM warp_azblob_data_owner "
                            + "WHERE account=? AND container=? AND name=? AND snapshot<>'')", r.account, container, name);
                    BlobStore.exec(c, "DELETE FROM warp_azblob_data_owner WHERE account=? AND container=? AND name=? "
                            + "AND snapshot<>''", r.account, container, name);
                    BlobStore.exec(c, "DELETE FROM warp_azblob_blobs WHERE account=? AND container=? AND name=? AND snapshot<>''",
                            r.account, container, name);
                    return null;
                }
                BlobStore.dropAllOwned(c, r.account, container, name);
                BlobStore.exec(c, "DELETE FROM warp_azblob_blobs WHERE account=? AND container=? AND name=?", r.account,
                        container, name);
            } else {
                if ("only".equals(mode) || "include".equals(mode)) {
                    // deleting one snapshot explicitly: the header is not meaningful
                }
                BlobStore.dropOwned(c, r.account, container, name, snapshot, null);
                BlobStore.exec(c, "DELETE FROM warp_azblob_blobs WHERE account=? AND container=? AND name=? AND snapshot=?",
                        r.account, container, name, snapshot);
            }
            return null;
        });
        resp.setStatus(202);
    }

    // ------------------------------------------------------------------------------------------ copy

    private void copyBlob(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String container, String name) {
        r.op = "CopyBlob";
        r.write = true;
        auth.authorize('o', "cw");
        container(r, container);
        String src = r.header("x-ms-copy-source");
        java.net.URI uri;
        try {
            uri = java.net.URI.create(src);
        } catch (IllegalArgumentException e) {
            throw AzErrors.invalidHeader("x-ms-copy-source", src);
        }
        String rawPath = uri.getRawPath() == null ? "" : uri.getRawPath();
        String srcAccount;
        String below;
        String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
        String suffix = ".blob." + cfg.domain();
        if (host.endsWith(suffix) && host.length() > suffix.length()) {
            srcAccount = host.substring(0, host.length() - suffix.length());
            below = rawPath;
        } else {
            String p = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
            int sl = p.indexOf('/');
            srcAccount = AzReq.decode(sl < 0 ? p : p.substring(0, sl), false);
            below = sl < 0 ? "" : p.substring(sl);
        }
        String bp = AzReq.decode(below, false);
        String[] parts = bp.startsWith("/") ? bp.substring(1).split("/", 2) : bp.split("/", 2);
        if (parts.length < 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
            throw AzErrors.invalidHeader("x-ms-copy-source", src);
        }
        String srcContainer = parts[0];
        String srcName = parts[1];
        Map<String, String> q = new LinkedHashMap<>();
        if (uri.getRawQuery() != null) {
            for (String pair : uri.getRawQuery().split("&")) {
                int eq = pair.indexOf('=');
                q.put(AzReq.decode(eq < 0 ? pair : pair.substring(0, eq), true), eq < 0 ? "" : AzReq.decode(pair.substring(eq + 1), true));
            }
        }
        String srcSnap = q.getOrDefault("snapshot", "");
        if (cfg.account(srcAccount) == null) {
            throw new AzureException(404, "CannotVerifyCopySource", "Could not verify the copy source within the specified "
                    + "time (cross-server copy is not supported by Warp azurewire).");
        }
        // cross-account: the source must be readable anonymously or through a valid SAS in the URL
        Cont srcCont = store.getContainer(srcAccount, srcContainer);
        if (srcCont == null) {
            throw new AzureException(404, "CannotVerifyCopySource", "The specified container does not exist.");
        }
        if (!srcAccount.equalsIgnoreCase(r.account)) {
            authorizeCopySource(srcAccount, srcContainer, srcName, q, srcCont);
        }
        Blob source = store.getBlob(srcAccount, srcContainer, srcName, srcSnap);
        if (source == null) {
            throw new AzureException(404, "CannotVerifyCopySource", "The specified blob does not exist.");
        }
        Instant sim = null;
        if (r.header("x-ms-source-if-match") != null && !etagMatches(r.header("x-ms-source-if-match"), source.etag)
                || r.header("x-ms-source-if-none-match") != null && etagMatches(r.header("x-ms-source-if-none-match"), source.etag)) {
            throw new AzureException(412, "SourceConditionNotMet", "The source condition specified using HTTP conditional "
                    + "header(s) is not met.");
        }
        Map<String, String> md = metadata(r);
        String destHost = host(r, container, name);
        String srcHost = store.ownerHost(srcAccount, srcContainer, srcName);
        String copyId = UUID.randomUUID().toString();
        Blob dest = source.copyShallow();
        dest.account = r.account;
        dest.container = container;
        dest.name = name;
        dest.snapshot = "";
        dest.tags = r.header("x-ms-tags") != null ? parseTagsHeader(r.header("x-ms-tags")) : new LinkedHashMap<>();
        if (!md.isEmpty()) {
            dest.metadata = md;
        }
        dest.etag = AzHttp.newEtag();
        dest.createdAt = now();
        dest.lastModified = dest.createdAt;
        dest.copyId = copyId;
        dest.copySource = src.contains("?") ? src.substring(0, src.indexOf('?')) : src;
        dest.copyStatus = "success";
        dest.copyCompletion = dest.createdAt;
        dest.lease = new Lease();
        dest.sealed = false;
        applyTier(r, dest);
        Blob existing = store.getBlob(r.account, container, name, "");
        if (existing != null) {
            checkLease(r, existing.lease, true, "blob");
        }
        conditions(r, existing != null, existing == null ? null : existing.etag, existing == null ? null : existing.lastModified,
                existing == null ? null : existing.tags, false);
        List<Seg> newSegs;
        if (srcHost.equals(destHost)) {
            newSegs = store.shards.tx(destHost, c -> BlobStore.duplicateSegments(c, source, r.account, container, name, ""));
            // segments are owned as committed data of the new blob; adopt happens in the tx below via keep set
        } else {
            newSegs = copyAcross(source, destHost, r.account, container, name);
        }
        dest.segments = newSegs;
        Set<String> keep = new HashSet<>();
        for (Seg s : newSegs) {
            keep.add(s.d);
        }
        store.shards.tx(destHost, c -> {
            Blob cur = BlobStore.selectBlob(c, r.account, container, name, "", true);
            if (cur != null) {
                dest.lease = cur.lease;
            }
            // remove the previous blob's data (all owned data of the base blob except the freshly copied)
            BlobStore.dropOwned(c, r.account, container, name, "", keep);
            BlobStore.upsertBlob(c, dest);
            return null;
        });
        resp.setHeader("ETag", dest.etag);
        resp.setHeader("Last-Modified", AzHttp.httpDate(dest.lastModified));
        resp.setHeader("x-ms-copy-id", copyId);
        resp.setHeader("x-ms-copy-status", "success");
        resp.setHeader("x-ms-request-server-encrypted", "true");
        resp.setStatus(202);
    }

    private void authorizeCopySource(String srcAccount, String srcContainer, String srcName, Map<String, String> q, Cont k) {
        if (q.containsKey("sig")) {
            // validate the SAS through the normal authenticator with a synthetic GET on the source
            AzReq sr = SubRequest.forUrl(srcAccount, "/" + srcContainer + "/" + srcName, q, cfg);
            AzureAuth.Result res = authn.authenticate(sr, id -> {
                for (AzureAuth.Policy p : k.acl) {
                    if (p.id().equals(id)) {
                        return p;
                    }
                }
                return null;
            }, srcContainer, srcName);
            res.authorize('o', "r");
            return;
        }
        if (!"blob".equals(k.publicAccess) && !"container".equals(k.publicAccess)) {
            throw new AzureException(404, "CannotVerifyCopySource", "Could not verify the copy source within the specified "
                    + "time.");
        }
    }

    /** Copies a blob's data to another shard chunk by chunk (bounded memory) and returns the new segments. */
    private List<Seg> copyAcross(Blob source, String destHost, String account, String container, String name) {
        List<Seg> out = new ArrayList<>();
        try {
            if ("PageBlob".equals(source.type)) {
                for (Seg s : source.segments) {
                    Seg n = copySeg(s);
                    n.d = pump(source, s.o, s.o + s.n - 1, destHost, account, container, name, "P");
                    n.s = 0;
                    out.add(n);
                }
                return out;
            }
            long off = 0;
            for (Seg s : source.segments) {
                Seg n = copySeg(s);
                n.d = s.n == 0 ? UUID.randomUUID().toString() : pump(source, off, off + s.n - 1, destHost, account, container,
                        name, "C");
                off += s.n;
                out.add(n);
            }
            return out;
        } catch (IOException e) {
            throw new AzureException(500, "InternalError", "copy failed: " + e.getMessage());
        }
    }

    private String pump(Blob source, long from, long to, String destHost, String account, String container, String name,
            String kind) throws IOException {
        java.io.PipedOutputStream po = new java.io.PipedOutputStream();
        java.io.PipedInputStream pi = new java.io.PipedInputStream(po, 1 << 20);
        Throwable[] err = new Throwable[1];
        Thread t = new Thread(() -> {
            try {
                store.stream(source, from, to, po);
            } catch (IOException | RuntimeException e) {
                err[0] = e;
            } finally {
                try {
                    po.close();
                } catch (IOException ignored) {
                    // reader sees EOF
                }
            }
        }, "azblob-copy");
        t.setDaemon(true);
        t.start();
        BlobStore.Ingested in = store.ingest(destHost, pi, -1, account, container, name, "", kind, null);
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (err[0] != null) {
            store.dropData(destHost, List.of(in.dataId()));
            throw new AzureException(500, "InternalError", "copy failed: " + err[0].getMessage());
        }
        return in.dataId();
    }
}

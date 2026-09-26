package com.sayonora.warp.azurewire;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.w3c.dom.Element;

/** The Azure Queue service REST API on top of {@link QueueStore}. */
final class QueueService {

    private static final Pattern QUEUE_NAME = Pattern.compile("^[a-z0-9](?:[a-z0-9]|-(?=[a-z0-9])){2,62}$");
    private static final int MAX_MESSAGE_BYTES = 64 * 1024;
    private static final long MAX_TTL = 7L * 24 * 3600;
    private static final long DEFAULT_TTL = MAX_TTL;

    final QueueStore store;
    final AzureConfig cfg;
    final AzureAuth authn;

    QueueService(QueueStore store, AzureConfig cfg, AzureAuth authn) {
        this.store = store;
        this.cfg = cfg;
        this.authn = authn;
    }

    private List<AzCors.Rule> corsRules(String account) {
        try {
            return AzCors.parse(AzServiceProps.cors(store.serviceProperties(account)));
        } catch (RuntimeException e) {
            return List.of();
        }
    }

    void handle(AzReq r, HttpServletResponse resp) throws IOException {
        String queue = r.first();
        if ("OPTIONS".equals(r.method)) {
            AzCors.preflight(r, resp, corsRules(r.account));
            return;
        }
        AzureAuth.Result auth = authn.authenticate(r, id -> {
            if (queue == null) {
                return null;
            }
            QueueStore.Queue q = store.get(r.account, queue);
            if (q == null) {
                return null;
            }
            for (AzureAuth.Policy p : q.acl()) {
                if (p.id().equals(id)) {
                    return p;
                }
            }
            return null;
        }, queue, null);
        AzCors.applyActual(r, resp, corsRules(r.account));
        if (auth.anonymous()) {
            throw BlobService.noAuthKeyed();
        }
        String comp = r.q("comp");
        if (queue == null) {
            serviceLevel(r, resp, auth, comp);
            return;
        }
        String rest = r.rest();
        if (rest == null) {
            queueLevel(r, resp, auth, queue, comp);
            return;
        }
        if (rest.equals("messages")) {
            messages(r, resp, auth, queue);
            return;
        }
        if (rest.startsWith("messages/") && rest.length() > 9) {
            messageById(r, resp, auth, queue, rest.substring(9));
            return;
        }
        throw AzErrors.invalidUri();
    }

    static void validateName(String name) {
        if (name.length() < 3 || name.length() > 63) {
            throw new AzureException(400, "OutOfRangeInput", "The specified resource name length is not within the permissible limits.");
        }
        if (!QUEUE_NAME.matcher(name).matches()) {
            throw new AzureException(400, "InvalidResourceName", "The specified resource name contains invalid characters.");
        }
    }

    private String endpoint(AzReq r) {
        return r.raw.getScheme() + "://" + r.header("Host") + (r.hostStyle ? "/" : "/" + r.account);
    }

    private void serviceLevel(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String comp) throws IOException {
        String restype = r.q("restype");
        if ("service".equals(restype) && "properties".equals(comp)) {
            if ("GET".equals(r.method)) {
                r.op = "GetServiceProperties";
                auth.authorize('s', "r");
                AzHttp.xml(resp, 200, AzServiceProps.render(store.serviceProperties(r.account), false));
                return;
            }
            BlobService.requireMethod(r, "PUT");
            r.op = "SetServiceProperties";
            r.write = true;
            auth.authorize('s', "w");
            store.setServiceProperties(r.account,
                    AzServiceProps.merge(store.serviceProperties(r.account), r.raw.getInputStream().readAllBytes(), false));
            resp.setStatus(202);
            return;
        }
        if ("service".equals(restype) && "stats".equals(comp)) {
            BlobService.requireMethod(r, "GET");
            r.op = "GetServiceStats";
            auth.authorize('s', "r");
            AzHttp.xml(resp, 200, new AzXml().open("StorageServiceStats").open("GeoReplication").text("Status", "live")
                    .text("LastSyncTime", AzHttp.httpDate(Instant.now())).close().close().toString());
            return;
        }
        if ("list".equals(comp)) {
            BlobService.requireMethod(r, "GET");
            r.op = "ListQueues";
            auth.authorize('s', "l");
            listQueues(r, resp);
            return;
        }
        throw AzErrors.invalidUri();
    }

    private void listQueues(AzReq r, HttpServletResponse resp) throws IOException {
        String prefix = r.q("prefix") == null ? "" : r.q("prefix");
        String marker = r.q("marker") == null ? "" : r.q("marker");
        int max = 5000;
        if (r.q("maxresults") != null) {
            try {
                max = Integer.parseInt(r.q("maxresults"));
            } catch (NumberFormatException e) {
                throw AzErrors.invalidQuery("maxresults", r.q("maxresults"));
            }
            if (max <= 0) {
                throw AzErrors.outOfRange("maxresults", r.q("maxresults"), "1", "2147483647");
            }
            max = Math.min(max, 5000);
        }
        boolean meta = BlobService.include(r).contains("metadata");
        List<QueueStore.Queue> l = store.list(r.account, prefix, marker, max + 1);
        AzXml x = new AzXml().openAttrs("EnumerationResults", "ServiceEndpoint", endpoint(r));
        x.textOrEmpty("Prefix", prefix).text("MaxResults", String.valueOf(max));
        x.open("Queues");
        for (int i = 0; i < Math.min(max, l.size()); i++) {
            QueueStore.Queue q = l.get(i);
            x.open("Queue").text("Name", q.name());
            if (meta) {
                x.open("Metadata");
                q.metadata().forEach(x::text);
                x.close();
            }
            x.close();
        }
        x.close();
        x.textOrEmpty("NextMarker", l.size() > max ? l.get(max).name() : "");
        x.close();
        AzHttp.xml(resp, 200, x.toString());
    }

    private void queueLevel(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String queue, String comp)
            throws IOException {
        if (comp == null) {
            switch (r.method) {
                case "PUT" -> {
                    r.op = "CreateQueue";
                    r.write = true;
                    auth.authorize('c', "c");
                    validateName(queue);
                    Map<String, String> md = BlobService.metadata(r);
                    resp.setStatus(store.create(r.account, queue, md) ? 201 : 204);
                }
                case "DELETE" -> {
                    r.op = "DeleteQueue";
                    r.write = true;
                    auth.authorize('c', "d");
                    store.delete(r.account, queue);
                    resp.setStatus(204);
                }
                default -> BlobService.requireMethod(r);
            }
            return;
        }
        switch (comp) {
            case "metadata" -> {
                if ("PUT".equals(r.method)) {
                    r.op = "SetQueueMetadata";
                    r.write = true;
                    auth.authorize('c', "uwa");
                    store.setMetadata(r.account, queue, BlobService.metadata(r));
                    resp.setStatus(204);
                } else {
                    BlobService.requireMethod(r, "GET", "HEAD");
                    r.op = "GetQueueMetadata";
                    auth.authorize('c', "r");
                    QueueStore.Queue q = store.get(r.account, queue);
                    if (q == null) {
                        throw QueueStore.queueNotFound();
                    }
                    BlobService.writeMetadata(resp, q.metadata());
                    resp.setHeader("x-ms-approximate-messages-count",
                            String.valueOf(store.approximateCount(r.account, queue, Instant.now())));
                    resp.setStatus(200);
                }
            }
            case "acl" -> {
                if (auth.kind != AzureAuth.Kind.KEY && auth.kind != AzureAuth.Kind.BEARER) {
                    throw new AzureException(403, "AuthorizationFailure", "This request is not authorized to perform this operation.");
                }
                if ("PUT".equals(r.method)) {
                    r.op = "SetQueueAcl";
                    r.write = true;
                    QueueStore.Queue q = store.get(r.account, queue);
                    if (q == null) {
                        throw QueueStore.queueNotFound();
                    }
                    store.setAcl(r.account, queue, AzServiceProps.parseSignedIdentifiers(r.raw.getInputStream().readAllBytes()));
                    resp.setStatus(204);
                } else {
                    BlobService.requireMethod(r, "GET");
                    r.op = "GetQueueAcl";
                    QueueStore.Queue q = store.get(r.account, queue);
                    if (q == null) {
                        throw QueueStore.queueNotFound();
                    }
                    AzXml x = new AzXml().open("SignedIdentifiers");
                    for (AzureAuth.Policy p : q.acl()) {
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
                    AzHttp.xml(resp, 200, x.toString());
                }
            }
            default -> throw new AzureException(400, "InvalidQueryParameterValue", "Value for one of the query parameters "
                    + "specified in the request URI is invalid.").extra("QueryParameterName", "comp")
                    .extra("QueryParameterValue", comp);
        }
    }

    private static int intParam(AzReq r, String name, int dflt, int min, int max) {
        String v = r.q(name);
        if (v == null) {
            return dflt;
        }
        int n;
        try {
            n = Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw AzErrors.invalidQuery(name, v);
        }
        if (n < min || n > max) {
            throw AzErrors.outOfRange(name, v, String.valueOf(min), String.valueOf(max));
        }
        return n;
    }

    private QueueStore.Queue requireQueue(AzReq r, String queue) {
        QueueStore.Queue q = store.get(r.account, queue);
        if (q == null) {
            throw QueueStore.queueNotFound();
        }
        return q;
    }

    private void messages(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String queue) throws IOException {
        Instant now = Instant.now();
        switch (r.method) {
            case "POST" -> {
                r.op = "PutMessage";
                r.write = true;
                auth.authorize('o', "a");
                byte[] raw = r.raw.getInputStream().readAllBytes();
                String text = parseMessage(raw, true);
                long ttl = DEFAULT_TTL;
                if (r.q("messagettl") != null) {
                    try {
                        ttl = Long.parseLong(r.q("messagettl"));
                    } catch (NumberFormatException e) {
                        throw AzErrors.invalidQuery("messagettl", r.q("messagettl"));
                    }
                    if (ttl != -1 && ttl < 1) {
                        throw AzErrors.invalidQuery("messagettl", r.q("messagettl")).extra("Reason",
                                "Value must be greater than or equal to 1, or -1 to indicate an infinite TTL.");
                    }
                }
                int vis = intParam(r, "visibilitytimeout", 0, 0, (int) MAX_TTL);
                if (ttl > 0 && vis >= ttl) {
                    throw new AzureException(400, "InvalidQueryParameterValue", "Value for one of the query parameters "
                            + "specified in the request URI is invalid.").extra("QueryParameterName", "visibilitytimeout")
                            .extra("QueryParameterValue", r.q("visibilitytimeout") == null ? "0" : r.q("visibilitytimeout"))
                            .extra("Reason", "messagettl must be greater than visibilitytimeout.");
                }
                requireQueue(r, queue);
                QueueStore.Message m = store.put(r.account, queue, text, now, vis, ttl);
                AzXml x = new AzXml().open("QueueMessagesList").open("QueueMessage").text("MessageId", m.id())
                        .text("InsertionTime", AzHttp.httpDate(m.insertedAt())).text("ExpirationTime", AzHttp.httpDate(m.expiresAt()))
                        .text("PopReceipt", m.popReceipt()).text("TimeNextVisible", AzHttp.httpDate(m.visibleAt())).close().close();
                AzHttp.xml(resp, 201, x.toString());
            }
            case "GET" -> {
                boolean peek = "true".equalsIgnoreCase(r.q("peekonly"));
                r.op = peek ? "PeekMessages" : "GetMessages";
                auth.authorize('o', peek ? "r" : "p");
                int num = intParam(r, "numofmessages", 1, 1, 32);
                int vis = peek ? 0 : intParam(r, "visibilitytimeout", 30, 1, (int) MAX_TTL);
                requireQueue(r, queue);
                List<QueueStore.Message> ms = peek ? store.peek(r.account, queue, num, now)
                        : store.get(r.account, queue, num, now, vis);
                AzXml x = new AzXml();
                if (ms.isEmpty()) {
                    x.raw("<QueueMessagesList />");
                } else {
                    x.open("QueueMessagesList");
                    for (QueueStore.Message m : ms) {
                        x.open("QueueMessage").text("MessageId", m.id()).text("InsertionTime", AzHttp.httpDate(m.insertedAt()))
                                .text("ExpirationTime", AzHttp.httpDate(m.expiresAt()));
                        if (!peek) {
                            x.text("PopReceipt", m.popReceipt()).text("TimeNextVisible", AzHttp.httpDate(m.visibleAt()));
                        }
                        x.text("DequeueCount", String.valueOf(m.dequeueCount())).textOrEmpty("MessageText", m.body()).close();
                    }
                    x.close();
                }
                AzHttp.xml(resp, 200, x.toString());
            }
            case "DELETE" -> {
                r.op = "ClearMessages";
                r.write = true;
                auth.authorize('o', "p");
                requireQueue(r, queue);
                store.clear(r.account, queue);
                resp.setStatus(204);
            }
            default -> BlobService.requireMethod(r);
        }
    }

    private void messageById(AzReq r, HttpServletResponse resp, AzureAuth.Result auth, String queue, String id)
            throws IOException {
        Instant now = Instant.now();
        String pop = r.q("popreceipt");
        switch (r.method) {
            case "DELETE" -> {
                r.op = "DeleteMessage";
                r.write = true;
                auth.authorize('o', "p");
                if (pop == null) {
                    throw AzErrors.missingHeader("popreceipt").extra("QueryParameterName", "popreceipt");
                }
                requireQueue(r, queue);
                store.delete(r.account, queue, id, pop, now);
                resp.setStatus(204);
            }
            case "PUT" -> {
                r.op = "UpdateMessage";
                r.write = true;
                auth.authorize('o', "u");
                if (pop == null) {
                    throw AzErrors.missingHeader("popreceipt").extra("QueryParameterName", "popreceipt");
                }
                if (r.q("visibilitytimeout") == null) {
                    throw AzErrors.missingHeader("visibilitytimeout").extra("QueryParameterName", "visibilitytimeout");
                }
                int vis = intParam(r, "visibilitytimeout", 0, 0, (int) MAX_TTL);
                byte[] raw = r.raw.getInputStream().readAllBytes();
                String text = raw.length == 0 ? null : parseMessage(raw, false);
                requireQueue(r, queue);
                QueueStore.Message m = store.update(r.account, queue, id, pop, now, vis, text);
                resp.setHeader("x-ms-popreceipt", m.popReceipt());
                resp.setHeader("x-ms-time-next-visible", AzHttp.httpDate(m.visibleAt()));
                resp.setStatus(204);
            }
            default -> BlobService.requireMethod(r);
        }
    }

    static String parseMessage(byte[] raw, boolean required) {
        if (raw.length > MAX_MESSAGE_BYTES + 4096) {
            throw tooLarge();
        }
        if (raw.length == 0) {
            if (required) {
                throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
            }
            return "";
        }
        Element root = AzServiceProps.parse(raw).getDocumentElement();
        if (!"QueueMessage".equals(root.getTagName())) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        String text = null;
        for (Element c : AzServiceProps.children(root)) {
            if ("MessageText".equals(c.getTagName())) {
                text = AzServiceProps.text(c);
            }
        }
        if (text == null) {
            throw new AzureException(400, "InvalidXmlDocument", "XML specified is not syntactically valid.");
        }
        if (text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_MESSAGE_BYTES) {
            throw tooLarge();
        }
        return text;
    }

    static AzureException tooLarge() {
        return new AzureException(400, "MessageTooLarge", "The request body is too large and exceeds the maximum permissible "
                + "limit.").extra("MaxLimit", "65536");
    }

    static Map<String, String> unused() {
        return new LinkedHashMap<>();
    }
}

package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.AzureToolSupport.*;
import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonObject;
import com.sayonora.warp.azurewire.AzureEmbedded;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Azure Queue Storage vocabulary (azqueue_* tools mirroring the queue and message operations of Azure's tooling: list, create
 * and delete queues, send, receive, peek, delete and clear messages, queue metadata). Each tool sends the equivalent Queue
 * REST request to azurewire's own {@code QueueService} in process (visibility timeouts, pop receipts, dequeue counts and
 * error codes are the wire protocol's).
 */
final class AzQueueToolProvider extends StoreToolProvider {

    AzQueueToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.AZQUEUE, describer, stores);
    }

    private AzureEmbedded az() {
        return stores.engine("azqueue", AzureEmbedded::queue);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject acct = str("Storage account (default: the only configured account)");
        JsonObject queue = str("Queue name");
        return List.of(
                new Tool("azqueue_list_queues", "List the queues of a storage account.",
                        schema(List.of(), "account", acct, "prefix", str("Only names starting with this prefix"),
                                "maxResults", num("Max queues (default 100, max 1000)"), "marker", str("nextMarker of a truncated listing")), false),
                new Tool("azqueue_create_queue", "Create a queue.", schema(List.of("queue"), "account", acct, "queue", queue), true),
                new Tool("azqueue_delete_queue", "Delete a queue and its messages.", schema(List.of("queue"), "account", acct, "queue", queue), true),
                new Tool("azqueue_get_queue_metadata", "A queue's metadata and approximate message count.",
                        schema(List.of("queue"), "account", acct, "queue", queue), false),
                new Tool("azqueue_send_message", "Add a message to the back of a queue.",
                        schema(List.of("queue", "message"), "account", acct, "queue", queue, "message", str("Message text"),
                                "visibilityTimeout", num("Seconds the message stays invisible (default 0)"),
                                "timeToLive", num("Seconds the message lives (default 604800, -1 = never expires)")), true),
                new Tool("azqueue_receive_messages", "Receive messages (they become invisible for the visibility timeout; delete them with "
                        + "azqueue_delete_message and the returned popReceipt).",
                        schema(List.of("queue"), "account", acct, "queue", queue, "maxMessages", num("1-32 (default 1)"),
                                "visibilityTimeout", num("Seconds (default 30)")), true),
                new Tool("azqueue_peek_messages", "Look at messages at the front of the queue without changing them.",
                        schema(List.of("queue"), "account", acct, "queue", queue, "maxMessages", num("1-32 (default 1)")), false),
                new Tool("azqueue_delete_message", "Delete one message using the id and popReceipt from azqueue_receive_messages.",
                        schema(List.of("queue", "messageId", "popReceipt"), "account", acct, "queue", queue,
                                "messageId", str("Message id"), "popReceipt", str("Pop receipt of the last receive")), true),
                new Tool("azqueue_clear_messages", "Delete every message of a queue.", schema(List.of("queue"), "account", acct,
                        "queue", queue), true));
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) throws Exception {
        String account = account(a);
        String qp = a.has("queue") ? "/" + enc(requireString(a, "queue")) : "";
        switch (tool) {
            case "azqueue_list_queues": {
                AzureEmbedded.Response r = az().request(account, "GET", "", queryOf(List.of(q("comp", "list"), q("prefix", optString(a, "prefix")),
                        q("maxresults", String.valueOf(limit(a, "maxResults", 100, 1000))), q("marker", optString(a, "marker")))), Map.of(), null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                var doc = parse(r.text());
                JsonObject o = new JsonObject();
                o.add("queues", all(doc, "Queue", 1000));
                o.addProperty("nextMarker", childText(doc.getDocumentElement(), "NextMarker"));
                return json(o);
            }
            case "azqueue_create_queue":
            case "azqueue_delete_queue": {
                AzureEmbedded.Response r = az().request(account, tool.endsWith("create_queue") ? "PUT" : "DELETE", qp, null, Map.of(), null);
                return r.status() >= 300 ? failure(r) : json(ok(r));
            }
            case "azqueue_get_queue_metadata": {
                AzureEmbedded.Response r = az().request(account, "GET", qp, "comp=metadata", Map.of(), null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.addProperty("approximateMessagesCount", r.headers().get("x-ms-approximate-messages-count"));
                JsonObject meta = new JsonObject();
                r.headers().forEach((k, v) -> {
                    if (k.toLowerCase(java.util.Locale.ROOT).startsWith("x-ms-meta-")) {
                        meta.addProperty(k.substring(10), v);
                    }
                });
                o.add("metadata", meta);
                return json(o);
            }
            case "azqueue_send_message": {
                String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?><QueueMessage><MessageText>" + xmlEscape(requireString(a, "message"))
                        + "</MessageText></QueueMessage>";
                AzureEmbedded.Response r = az().request(account, "POST", qp + "/messages", queryOf(List.of(
                        q("visibilitytimeout", optLong(a, "visibilityTimeout") == null ? null : String.valueOf(optLong(a, "visibilityTimeout"))),
                        q("messagettl", optLong(a, "timeToLive") == null ? null : String.valueOf(optLong(a, "timeToLive"))))),
                        Map.of("Content-Type", "application/xml"), xml.getBytes(StandardCharsets.UTF_8));
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.add("messages", all(parse(r.text()), "QueueMessage", 32));
                return json(o);
            }
            case "azqueue_receive_messages":
            case "azqueue_peek_messages": {
                boolean peek = tool.endsWith("peek_messages");
                AzureEmbedded.Response r = az().request(account, "GET", qp + "/messages", queryOf(List.of(
                        q("numofmessages", String.valueOf(limit(a, "maxMessages", 1, 32))),
                        q("peekonly", peek ? "true" : null),
                        q("visibilitytimeout", peek ? null : String.valueOf(optLong(a, "visibilityTimeout") == null ? 30 : optLong(a, "visibilityTimeout"))))),
                        Map.of(), null);
                if (r.status() >= 300) {
                    return failure(r);
                }
                JsonObject o = new JsonObject();
                o.add("messages", all(parse(r.text()), "QueueMessage", 32));
                return json(o);
            }
            case "azqueue_delete_message": {
                AzureEmbedded.Response r = az().request(account, "DELETE", qp + "/messages/" + enc(requireString(a, "messageId")),
                        queryOf(java.util.Collections.singletonList(q("popreceipt", requireString(a, "popReceipt")))), Map.of(), null);
                return r.status() >= 300 ? failure(r) : json(ok(r));
            }
            case "azqueue_clear_messages": {
                AzureEmbedded.Response r = az().request(account, "DELETE", qp + "/messages", null, Map.of(), null);
                return r.status() >= 300 ? failure(r) : json(ok(r));
            }
            default:
                return Outcome.error("unknown azqueue tool: " + tool);
        }
    }

    private static JsonObject ok(AzureEmbedded.Response r) {
        JsonObject o = new JsonObject();
        o.addProperty("ok", true);
        o.addProperty("status", r.status());
        return o;
    }
}

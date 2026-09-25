// Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors.
// See Warp/NOTICE for the licence text.
package com.sayonora.wire.sqswire;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The AWS Query protocol for SQS (form-encoded request, XML response) -- what older SDKs, AWS CLI v1
 * and Terraform speak. It is a thin translation layer over {@link SqsOperations}: {@link #toJson} turns the
 * flattened {@code Name.N.Member} form parameters into the JSON-protocol request shape, and
 * {@link #renderResponse}/{@link #renderError} turn the JSON-shaped result back into XML. The request and
 * response parameter naming follows the SQS API reference and Floci's SqsQueryHandler
 * (https://github.com/floci-io/floci, MIT License).
 */
public final class SqsQueryProtocol {

    static final String NS = "http://queue.amazonaws.com/doc/2012-11-05/";
    private static final Pattern INDEXED = Pattern.compile("^([A-Za-z]+)\\.(\\d+)(?:\\.(.+))?$");
    private static final Set<String> INT_PARAMS = Set.of("MaxNumberOfMessages", "VisibilityTimeout", "WaitTimeSeconds",
            "DelaySeconds", "MaxResults", "MaxNumberOfMessagesPerSecond");
    private static final Map<String, String> SCALAR_LISTS = Map.of(
            "AttributeName", "AttributeNames",
            "MessageAttributeName", "MessageAttributeNames",
            "MessageSystemAttributeName", "MessageSystemAttributeNames",
            "TagKey", "TagKeys",
            "AWSAccountId", "AWSAccountIds",
            "ActionName", "Actions");

    private SqsQueryProtocol() {
    }

    /** Converts flattened Query parameters into the JSON-protocol request object. */
    public static JsonObject toJson(Map<String, String> params) {
        JsonObject out = new JsonObject();
        Map<String, TreeMap<Integer, Object>> indexed = new java.util.LinkedHashMap<>();
        for (var e : params.entrySet()) {
            String key = e.getKey();
            if (key.equals("Action") || key.equals("Version") || key.startsWith("X-Amz-") || key.equals("AWSAccessKeyId")
                    || key.equals("Signature") || key.equals("SignatureMethod") || key.equals("SignatureVersion")
                    || key.equals("Timestamp") || key.equals("Expires")) {
                continue;
            }
            Matcher m = INDEXED.matcher(key);
            if (!m.matches()) {
                if (INT_PARAMS.contains(key)) {
                    try {
                        out.addProperty(key, Integer.parseInt(e.getValue().trim()));
                    } catch (NumberFormatException ex) {
                        throw SqsException.invalidParam("Value " + e.getValue() + " for parameter " + key
                                + " is invalid. Reason: Must be an integer.");
                    }
                } else {
                    out.addProperty(key, e.getValue());
                }
                continue;
            }
            String group = m.group(1);
            int idx = Integer.parseInt(m.group(2));
            String rest = m.group(3);
            TreeMap<Integer, Object> slot = indexed.computeIfAbsent(group, k -> new TreeMap<>());
            if (rest == null) {
                slot.put(idx, e.getValue());
            } else {
                @SuppressWarnings("unchecked")
                Map<String, String> sub = (Map<String, String>) slot.computeIfAbsent(idx, k -> new java.util.LinkedHashMap<String, String>());
                sub.put(rest, e.getValue());
            }
        }
        for (var g : indexed.entrySet()) {
            String group = g.getKey();
            if (SCALAR_LISTS.containsKey(group)) {
                JsonArray arr = new JsonArray();
                g.getValue().values().forEach(v -> arr.add(String.valueOf(v)));
                out.add(SCALAR_LISTS.get(group), arr);
            } else if (group.equals("Attribute") || group.equals("Tag")) {
                JsonObject map = new JsonObject();
                for (Object v : g.getValue().values()) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> sub = (Map<String, String>) v;
                    String k = sub.get(group.equals("Tag") ? "Key" : "Name");
                    if (k != null) {
                        map.addProperty(k, sub.getOrDefault("Value", ""));
                    }
                }
                out.add(group.equals("Tag") ? "Tags" : "Attributes", map);
            } else if (group.equals("MessageAttribute") || group.equals("MessageSystemAttribute")) {
                out.add(group + "s", typedAttributes(g.getValue()));
            } else if (group.endsWith("Entry")) {
                JsonArray entries = new JsonArray();
                for (Object v : g.getValue().values()) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> sub = (Map<String, String>) v;
                    entries.add(toJson(sub));
                }
                out.add("Entries", entries);
            } else {
                // an unrecognised indexed parameter: keep it as a list of its values
                JsonArray arr = new JsonArray();
                g.getValue().values().forEach(v -> arr.add(String.valueOf(v)));
                out.add(group, arr);
            }
        }
        return out;
    }

    private static JsonObject typedAttributes(TreeMap<Integer, Object> entries) {
        JsonObject attrs = new JsonObject();
        for (Object v : entries.values()) {
            @SuppressWarnings("unchecked")
            Map<String, String> sub = (Map<String, String>) v;
            String name = sub.get("Name");
            if (name == null) {
                continue;
            }
            JsonObject val = new JsonObject();
            if (sub.containsKey("Value.DataType")) {
                val.addProperty("DataType", sub.get("Value.DataType"));
            }
            if (sub.containsKey("Value.StringValue")) {
                val.addProperty("StringValue", sub.get("Value.StringValue"));
            }
            if (sub.containsKey("Value.BinaryValue")) {
                val.addProperty("BinaryValue", sub.get("Value.BinaryValue"));
            }
            attrs.add(name, val);
        }
        return attrs;
    }

    // ------------------------------------------------------------------------------------------
    // XML rendering
    // ------------------------------------------------------------------------------------------

    static String escapeXml(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 16);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '&' -> sb.append("&amp;");
                case '<' -> sb.append("&lt;");
                case '>' -> sb.append("&gt;");
                case '"' -> sb.append("&quot;");
                case '\'' -> sb.append("&apos;");
                case '\r' -> sb.append("&#xD;");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

    private static void tag(StringBuilder sb, String name, JsonObject src, String key) {
        if (src.has(key) && !src.get(key).isJsonNull()) {
            tag(sb, name, src.get(key).getAsString());
        }
    }

    private static void tag(StringBuilder sb, String name, String value) {
        sb.append('<').append(name).append('>').append(escapeXml(value)).append("</").append(name).append('>');
    }

    private static void nameValues(StringBuilder sb, String wrapper, String nameTag, String valueTag, JsonObject map) {
        if (map == null) {
            return;
        }
        for (var e : map.entrySet()) {
            sb.append('<').append(wrapper).append('>');
            tag(sb, nameTag, e.getKey());
            tag(sb, valueTag, e.getValue().getAsString());
            sb.append("</").append(wrapper).append('>');
        }
    }

    private static void batchEntries(StringBuilder sb, JsonObject result, String successTag, List<String> fields) {
        if (result.has("Successful")) {
            for (JsonElement e : result.getAsJsonArray("Successful")) {
                JsonObject o = e.getAsJsonObject();
                sb.append('<').append(successTag).append('>');
                tag(sb, "Id", o, "Id");
                for (String f : fields) {
                    tag(sb, f, o, f);
                }
                sb.append("</").append(successTag).append('>');
            }
        }
        if (result.has("Failed")) {
            for (JsonElement e : result.getAsJsonArray("Failed")) {
                JsonObject o = e.getAsJsonObject();
                sb.append("<BatchResultErrorEntry>");
                tag(sb, "Id", o, "Id");
                tag(sb, "SenderFault", String.valueOf(o.get("SenderFault").getAsBoolean()));
                tag(sb, "Code", SqsException.queryCodeFor(o.get("Code").getAsString()));
                tag(sb, "Message", o, "Message");
                sb.append("</BatchResultErrorEntry>");
            }
        }
    }

    /** XML for a successful action result. */
    public static String renderResponse(String action, JsonObject result, String requestId) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("<?xml version=\"1.0\"?><").append(action).append("Response xmlns=\"").append(NS).append("\">");
        StringBuilder r = new StringBuilder();
        switch (action) {
            case "CreateQueue", "GetQueueUrl" -> tag(r, "QueueUrl", result, "QueueUrl");
            case "ListQueues" -> {
                if (result.has("QueueUrls")) {
                    result.getAsJsonArray("QueueUrls").forEach(u -> tag(r, "QueueUrl", u.getAsString()));
                }
                tag(r, "NextToken", result, "NextToken");
            }
            case "ListDeadLetterSourceQueues" -> {
                if (result.has("queueUrls")) {
                    result.getAsJsonArray("queueUrls").forEach(u -> tag(r, "QueueUrl", u.getAsString()));
                }
                tag(r, "NextToken", result, "NextToken");
            }
            case "GetQueueAttributes" -> nameValues(r, "Attribute", "Name", "Value", obj(result, "Attributes"));
            case "SendMessage" -> {
                tag(r, "MD5OfMessageBody", result, "MD5OfMessageBody");
                tag(r, "MD5OfMessageAttributes", result, "MD5OfMessageAttributes");
                tag(r, "MD5OfMessageSystemAttributes", result, "MD5OfMessageSystemAttributes");
                tag(r, "MessageId", result, "MessageId");
                tag(r, "SequenceNumber", result, "SequenceNumber");
            }
            case "SendMessageBatch" -> batchEntries(r, result, "SendMessageBatchResultEntry", List.of("MessageId",
                    "MD5OfMessageBody", "MD5OfMessageAttributes", "MD5OfMessageSystemAttributes", "SequenceNumber"));
            case "DeleteMessageBatch" -> batchEntries(r, result, "DeleteMessageBatchResultEntry", List.of());
            case "ChangeMessageVisibilityBatch" -> batchEntries(r, result, "ChangeMessageVisibilityBatchResultEntry", List.of());
            case "ReceiveMessage" -> {
                if (result.has("Messages")) {
                    for (JsonElement e : result.getAsJsonArray("Messages")) {
                        JsonObject m = e.getAsJsonObject();
                        r.append("<Message>");
                        tag(r, "MessageId", m, "MessageId");
                        tag(r, "ReceiptHandle", m, "ReceiptHandle");
                        tag(r, "MD5OfBody", m, "MD5OfBody");
                        tag(r, "Body", m, "Body");
                        nameValues(r, "Attribute", "Name", "Value", obj(m, "Attributes"));
                        tag(r, "MD5OfMessageAttributes", m, "MD5OfMessageAttributes");
                        JsonObject ma = obj(m, "MessageAttributes");
                        if (ma != null) {
                            for (var a : ma.entrySet()) {
                                JsonObject v = a.getValue().getAsJsonObject();
                                r.append("<MessageAttribute>");
                                tag(r, "Name", a.getKey());
                                r.append("<Value>");
                                tag(r, "DataType", v, "DataType");
                                tag(r, "StringValue", v, "StringValue");
                                tag(r, "BinaryValue", v, "BinaryValue");
                                r.append("</Value></MessageAttribute>");
                            }
                        }
                        r.append("</Message>");
                    }
                }
            }
            case "ListQueueTags" -> nameValues(r, "Tag", "Key", "Value", obj(result, "Tags"));
            case "StartMessageMoveTask" -> tag(r, "TaskHandle", result, "TaskHandle");
            case "CancelMessageMoveTask" -> tag(r, "ApproximateNumberOfMessagesMoved", result, "ApproximateNumberOfMessagesMoved");
            case "ListMessageMoveTasks" -> {
                if (result.has("Results")) {
                    for (JsonElement e : result.getAsJsonArray("Results")) {
                        JsonObject t = e.getAsJsonObject();
                        r.append("<ListMessageMoveTasksResultEntry>");
                        for (String f : List.of("TaskHandle", "Status", "SourceArn", "DestinationArn", "MaxNumberOfMessagesPerSecond",
                                "ApproximateNumberOfMessagesMoved", "ApproximateNumberOfMessagesToMove", "FailureReason",
                                "StartedTimestamp")) {
                            tag(r, f, t, f);
                        }
                        r.append("</ListMessageMoveTasksResultEntry>");
                    }
                }
            }
            default -> { /* DeleteQueue, DeleteMessage, SetQueueAttributes, ChangeMessageVisibility, PurgeQueue, TagQueue,
                            UntagQueue, AddPermission, RemovePermission: no result element */ }
        }
        if (r.length() > 0) {
            sb.append('<').append(action).append("Result>").append(r).append("</").append(action).append("Result>");
        }
        sb.append("<ResponseMetadata><RequestId>").append(requestId).append("</RequestId></ResponseMetadata></")
                .append(action).append("Response>");
        return sb.toString();
    }

    private static JsonObject obj(JsonObject o, String key) {
        return o.has(key) && o.get(key).isJsonObject() ? o.getAsJsonObject(key) : null;
    }

    /** The Query-protocol error document ({@code ErrorResponse/Error/{Type,Code,Message}} + RequestId). */
    public static String renderError(String code, String message, String requestId, boolean senderFault) {
        return "<?xml version=\"1.0\"?><ErrorResponse xmlns=\"" + NS + "\"><Error><Type>" + (senderFault ? "Sender" : "Receiver")
                + "</Type><Code>" + escapeXml(code) + "</Code><Message>" + escapeXml(message == null ? "" : message)
                + "</Message><Detail/></Error><RequestId>" + requestId + "</RequestId></ErrorResponse>";
    }
}

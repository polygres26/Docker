package com.sayonora.warp.mcp;

import static com.sayonora.warp.mcp.ToolSchemas.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;

/**
 * Amazon SNS vocabulary (sns_* tools following the AWS SNS/SQS MCP server's names: list, create and delete topics, topic
 * attributes, subscribe/unsubscribe, list subscriptions, publish). Each tool invokes the same {@code SnsService} operation
 * (CreateTopic, Publish, ...) awswire runs for an SNS SDK client, so filter policies, FIFO rules and delivery to SQS
 * subscriptions are the wire protocol's.
 */
final class SnsToolProvider extends StoreToolProvider {

    SnsToolProvider(StoreDescribeProvider describer, EmulatedStores stores) {
        super(BackendKind.SNS, describer, stores);
    }

    @Override
    protected List<Tool> defineTools() {
        JsonObject arn = str("Topic ARN");
        return List.of(
                new Tool("sns_list_topics", "List topics (up to 100 per page).", schema(List.of(), "nextToken", str("NextToken of the previous page")), false),
                new Tool("sns_create_topic", "Create a topic (idempotent for the same name and attributes).",
                        schema(List.of("name"), "name", str("Topic name (end with .fifo for a FIFO topic)"),
                                "attributes", obj("Topic attributes, e.g. {\"FifoTopic\":\"true\"}")), true),
                new Tool("sns_delete_topic", "Delete a topic and its subscriptions.", schema(List.of("topicArn"), "topicArn", arn), true),
                new Tool("sns_get_topic_attributes", "Topic attributes: policy, subscription counts, display name, FIFO settings.",
                        schema(List.of("topicArn"), "topicArn", arn), false),
                new Tool("sns_set_topic_attributes", "Set one topic attribute.", schema(List.of("topicArn", "attributeName", "attributeValue"),
                        "topicArn", arn, "attributeName", str("Attribute name, e.g. DisplayName"), "attributeValue", str("Attribute value")), true),
                new Tool("sns_subscribe", "Subscribe an endpoint (sqs, http(s), email, sms, lambda, ...) to a topic.",
                        schema(List.of("topicArn", "protocol"), "topicArn", arn, "protocol", str("sqs, http, https, email, sms, ..."),
                                "endpoint", str("Endpoint (queue ARN, URL, address)"),
                                "attributes", obj("Subscription attributes, e.g. FilterPolicy, RawMessageDelivery")), true),
                new Tool("sns_unsubscribe", "Remove a subscription.", schema(List.of("subscriptionArn"), "subscriptionArn", str("Subscription ARN")), true),
                new Tool("sns_list_subscriptions", "List subscriptions, for one topic or for the whole account.",
                        schema(List.of(), "topicArn", str("Only this topic's subscriptions"), "nextToken", str("NextToken of the previous page")), false),
                new Tool("sns_publish", "Publish a message to a topic.",
                        schema(List.of("topicArn", "message"), "topicArn", arn, "message", str("Message body"), "subject", str("Subject"),
                                "messageAttributes", obj("Attribute name -> string value (or {DataType, StringValue})"),
                                "messageGroupId", str("FIFO topics: message group"), "messageDeduplicationId", str("FIFO topics: dedup id")), true));
    }

    @Override
    protected Outcome run(String tool, JsonObject a, Ctx ctx) throws Exception {
        JsonObject req = new JsonObject();
        String op;
        switch (tool) {
            case "sns_list_topics" -> {
                op = "ListTopics";
                put(req, "NextToken", optString(a, "nextToken"));
            }
            case "sns_create_topic" -> {
                op = "CreateTopic";
                req.addProperty("Name", requireString(a, "name"));
                stringMap(req, "Attributes", a, "attributes");
            }
            case "sns_delete_topic" -> {
                op = "DeleteTopic";
                req.addProperty("TopicArn", requireString(a, "topicArn"));
            }
            case "sns_get_topic_attributes" -> {
                op = "GetTopicAttributes";
                req.addProperty("TopicArn", requireString(a, "topicArn"));
            }
            case "sns_set_topic_attributes" -> {
                op = "SetTopicAttributes";
                req.addProperty("TopicArn", requireString(a, "topicArn"));
                req.addProperty("AttributeName", requireString(a, "attributeName"));
                req.addProperty("AttributeValue", requireString(a, "attributeValue"));
            }
            case "sns_subscribe" -> {
                op = "Subscribe";
                req.addProperty("TopicArn", requireString(a, "topicArn"));
                req.addProperty("Protocol", requireString(a, "protocol"));
                put(req, "Endpoint", optString(a, "endpoint"));
                stringMap(req, "Attributes", a, "attributes");
                req.addProperty("ReturnSubscriptionArn", true);
            }
            case "sns_unsubscribe" -> {
                op = "Unsubscribe";
                req.addProperty("SubscriptionArn", requireString(a, "subscriptionArn"));
            }
            case "sns_list_subscriptions" -> {
                op = optString(a, "topicArn") == null ? "ListSubscriptions" : "ListSubscriptionsByTopic";
                put(req, "TopicArn", optString(a, "topicArn"));
                put(req, "NextToken", optString(a, "nextToken"));
            }
            case "sns_publish" -> {
                op = "Publish";
                req.addProperty("TopicArn", requireString(a, "topicArn"));
                req.addProperty("Message", requireString(a, "message"));
                put(req, "Subject", optString(a, "subject"));
                put(req, "MessageGroupId", optString(a, "messageGroupId"));
                put(req, "MessageDeduplicationId", optString(a, "messageDeduplicationId"));
                if (a.has("messageAttributes") && a.get("messageAttributes").isJsonObject()) {
                    JsonObject attrs = new JsonObject();
                    for (Map.Entry<String, JsonElement> e : a.getAsJsonObject("messageAttributes").entrySet()) {
                        if (e.getValue().isJsonObject()) {
                            attrs.add(e.getKey(), e.getValue());
                        } else {
                            JsonObject v = new JsonObject();
                            v.addProperty("DataType", e.getValue().getAsJsonPrimitive().isNumber() ? "Number" : "String");
                            v.addProperty("StringValue", e.getValue().getAsString());
                            attrs.add(e.getKey(), v);
                        }
                    }
                    req.add("MessageAttributes", attrs);
                }
            }
            default -> {
                return Outcome.error("unknown sns tool: " + tool);
            }
        }
        return json(AwsToolSupport.invoke(stores, "sns", op, req));
    }

    static void put(JsonObject o, String k, String v) {
        if (v != null) {
            o.addProperty(k, v);
        }
    }

    static void stringMap(JsonObject req, String key, JsonObject a, String argKey) {
        if (a.has(argKey) && a.get(argKey).isJsonObject()) {
            JsonObject m = new JsonObject();
            a.getAsJsonObject(argKey).entrySet().forEach(e -> m.addProperty(e.getKey(), e.getValue().isJsonPrimitive()
                    ? e.getValue().getAsString() : e.getValue().toString()));
            req.add(key, m);
        }
    }
}

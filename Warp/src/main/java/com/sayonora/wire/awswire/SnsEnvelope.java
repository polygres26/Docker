// Portions adapted from Floci (https://github.com/floci-io/floci), MIT License, Copyright (c) 2025 Floci and its contributors.
// See Warp/NOTICE for the licence text.
package com.sayonora.wire.awswire;

import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

/**
 * The JSON documents SNS delivers to non-raw subscribers (SQS, HTTP/S, Lambda, email-json): {@code Notification},
 * {@code SubscriptionConfirmation} and {@code UnsubscribeConfirmation}, with {@code MessageId}, {@code TopicArn},
 * {@code Timestamp} and the {@code SignatureVersion}/{@code Signature}/{@code SigningCertURL} triple. The signature is a
 * real SHA1withRSA signature over the canonical string-to-sign of the SNS documentation, made with a per-process key
 * that no certificate published by Warp vouches for: it has the right shape (base64, verifiable in principle) but cannot
 * be verified against AWS's certificate, so a subscriber that verifies signatures must be configured to skip that.
 */
public final class SnsEnvelope {

    private SnsEnvelope() {
    }

    private static volatile KeyPair key;

    private static KeyPair key() {
        KeyPair k = key;
        if (k == null) {
            synchronized (SnsEnvelope.class) {
                if (key == null) {
                    try {
                        KeyPairGenerator g = KeyPairGenerator.getInstance("RSA");
                        g.initialize(2048);
                        key = g.generateKeyPair();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
                k = key;
            }
        }
        return k;
    }

    public static String certUrl(String region) {
        return "https://sns." + region + ".amazonaws.com/SimpleNotificationService-0000000000000000000000000000000.pem";
    }

    static String sign(String stringToSign) {
        try {
            Signature s = Signature.getInstance("SHA1withRSA");
            s.initSign(key().getPrivate());
            s.update(stringToSign.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(s.sign());
        } catch (Exception e) {
            return "";
        }
    }

    /** Canonical string SNS signs for a Notification (documented "Signature Version 1" layout). */
    static String stringToSignNotification(String message, String messageId, String subject, String timestamp, String topicArn) {
        StringBuilder sb = new StringBuilder();
        sb.append("Message\n").append(message).append('\n');
        sb.append("MessageId\n").append(messageId).append('\n');
        if (subject != null) {
            sb.append("Subject\n").append(subject).append('\n');
        }
        sb.append("Timestamp\n").append(timestamp).append('\n');
        sb.append("TopicArn\n").append(topicArn).append('\n');
        sb.append("Type\nNotification\n");
        return sb.toString();
    }

    public static JsonObject attributesJson(Map<String, SnsFilterPolicy.Attr> attrs, Map<String, String> binary) {
        JsonObject o = new JsonObject();
        for (var e : attrs.entrySet()) {
            JsonObject a = new JsonObject();
            a.addProperty("Type", e.getValue().dataType());
            a.addProperty("Value", e.getValue().stringValue());
            o.add(e.getKey(), a);
        }
        return o;
    }

    /** A {@code Notification}. {@code attributes} is name to {Type, Value}; omitted when empty. */
    public static String notification(String region, String topicArn, String messageId, String subject, String message,
            JsonObject attributes, String unsubscribeUrl, Instant now) {
        String ts = now.toString();
        JsonObject o = new JsonObject();
        o.addProperty("Type", "Notification");
        o.addProperty("MessageId", messageId);
        o.addProperty("TopicArn", topicArn);
        if (subject != null) {
            o.addProperty("Subject", subject);
        }
        o.addProperty("Message", message);
        o.addProperty("Timestamp", ts);
        o.addProperty("SignatureVersion", "1");
        o.addProperty("Signature", sign(stringToSignNotification(message, messageId, subject, ts, topicArn)));
        o.addProperty("SigningCertURL", certUrl(region));
        o.addProperty("UnsubscribeURL", unsubscribeUrl);
        if (attributes != null && attributes.size() > 0) {
            o.add("MessageAttributes", attributes);
        }
        return o.toString();
    }

    public static String subscriptionConfirmation(String region, String topicArn, String messageId, String token,
            String subscribeUrl, Instant now) {
        String ts = now.toString();
        String message = "You have chosen to subscribe to the topic " + topicArn
                + ".\nTo confirm the subscription, visit the SubscribeURL included in this message.";
        JsonObject o = new JsonObject();
        o.addProperty("Type", "SubscriptionConfirmation");
        o.addProperty("MessageId", messageId);
        o.addProperty("Token", token);
        o.addProperty("TopicArn", topicArn);
        o.addProperty("Message", message);
        o.addProperty("SubscribeURL", subscribeUrl);
        o.addProperty("Timestamp", ts);
        o.addProperty("SignatureVersion", "1");
        String sts = "Message\n" + message + "\nMessageId\n" + messageId + "\nSubscribeURL\n" + subscribeUrl + "\nTimestamp\n"
                + ts + "\nToken\n" + token + "\nTopicArn\n" + topicArn + "\nType\nSubscriptionConfirmation\n";
        o.addProperty("Signature", sign(sts));
        o.addProperty("SigningCertURL", certUrl(region));
        return o.toString();
    }

    public static String unsubscribeConfirmation(String region, String topicArn, String messageId, String token,
            String subscribeUrl, Instant now) {
        String ts = now.toString();
        String message = "You have chosen to deactivate subscription " + topicArn
                + ".\nTo cancel this operation and restore the subscription, visit the SubscribeURL included in this message.";
        JsonObject o = new JsonObject();
        o.addProperty("Type", "UnsubscribeConfirmation");
        o.addProperty("MessageId", messageId);
        o.addProperty("Token", token);
        o.addProperty("TopicArn", topicArn);
        o.addProperty("Message", message);
        o.addProperty("SubscribeURL", subscribeUrl);
        o.addProperty("Timestamp", ts);
        o.addProperty("SignatureVersion", "1");
        String sts = "Message\n" + message + "\nMessageId\n" + messageId + "\nSubscribeURL\n" + subscribeUrl + "\nTimestamp\n"
                + ts + "\nToken\n" + token + "\nTopicArn\n" + topicArn + "\nType\nUnsubscribeConfirmation\n";
        o.addProperty("Signature", sign(sts));
        o.addProperty("SigningCertURL", certUrl(region));
        return o.toString();
    }

    /** The Lambda event {@code Records[0]} shape for a Notification. */
    public static String lambdaEvent(String subscriptionArn, JsonObject sns) {
        JsonObject rec = new JsonObject();
        rec.addProperty("EventVersion", "1.0");
        rec.addProperty("EventSubscriptionArn", subscriptionArn);
        rec.addProperty("EventSource", "aws:sns");
        rec.add("Sns", sns);
        com.google.gson.JsonArray arr = new com.google.gson.JsonArray();
        arr.add(rec);
        JsonObject root = new JsonObject();
        root.add("Records", arr);
        return root.toString();
    }
}

package com.sayonora.warp.awswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.time.Instant;
import java.util.Base64;
import org.junit.jupiter.api.Test;

class SnsEnvelopeTest {

    private static final Instant T = Instant.parse("2026-09-25T12:00:00Z");

    @Test
    void notificationCarriesTheDocumentedFields() {
        JsonObject attrs = new JsonObject();
        JsonObject a = new JsonObject();
        a.addProperty("Type", "String");
        a.addProperty("Value", "blue");
        attrs.add("color", a);
        JsonObject o = JsonParser.parseString(SnsEnvelope.notification("us-east-1", "arn:aws:sns:us-east-1:000000000000:t", "mid-1",
                "subj", "hello", attrs, "https://sns/unsub", T)).getAsJsonObject();
        assertEquals("Notification", o.get("Type").getAsString());
        assertEquals("mid-1", o.get("MessageId").getAsString());
        assertEquals("arn:aws:sns:us-east-1:000000000000:t", o.get("TopicArn").getAsString());
        assertEquals("subj", o.get("Subject").getAsString());
        assertEquals("hello", o.get("Message").getAsString());
        assertEquals("2026-09-25T12:00:00Z", o.get("Timestamp").getAsString());
        assertEquals("1", o.get("SignatureVersion").getAsString());
        assertTrue(Base64.getDecoder().decode(o.get("Signature").getAsString()).length >= 256, "an RSA-2048 signature");
        assertTrue(o.get("SigningCertURL").getAsString().startsWith("https://sns.us-east-1.amazonaws.com/SimpleNotificationService-"));
        assertEquals("https://sns/unsub", o.get("UnsubscribeURL").getAsString());
        assertEquals("blue", o.getAsJsonObject("MessageAttributes").getAsJsonObject("color").get("Value").getAsString());
    }

    @Test
    void subjectAndAttributesAreOmittedWhenAbsent() {
        JsonObject o = JsonParser.parseString(SnsEnvelope.notification("us-east-1", "arn", "m", null, "x", new JsonObject(), "u", T))
                .getAsJsonObject();
        assertFalse(o.has("Subject"));
        assertFalse(o.has("MessageAttributes"));
    }

    @Test
    void subscriptionConfirmationHasTokenAndSubscribeUrl() {
        JsonObject o = JsonParser.parseString(SnsEnvelope.subscriptionConfirmation("us-east-1", "arn:t", "m", "tok", "http://x/?Token=tok", T))
                .getAsJsonObject();
        assertEquals("SubscriptionConfirmation", o.get("Type").getAsString());
        assertEquals("tok", o.get("Token").getAsString());
        assertEquals("http://x/?Token=tok", o.get("SubscribeURL").getAsString());
        assertNotNull(o.get("Signature"));
    }

    @Test
    void stringToSignFollowsTheDocumentedLayout() {
        assertEquals("Message\nhi\nMessageId\nm\nSubject\ns\nTimestamp\nt\nTopicArn\narn\nType\nNotification\n",
                SnsEnvelope.stringToSignNotification("hi", "m", "s", "t", "arn"));
        assertEquals("Message\nhi\nMessageId\nm\nTimestamp\nt\nTopicArn\narn\nType\nNotification\n",
                SnsEnvelope.stringToSignNotification("hi", "m", null, "t", "arn"));
    }

    @Test
    void lambdaEventWrapsTheNotificationInRecords() {
        JsonObject sns = new JsonObject();
        sns.addProperty("Message", "hi");
        JsonObject o = JsonParser.parseString(SnsEnvelope.lambdaEvent("arn:sub", sns)).getAsJsonObject();
        JsonObject rec = o.getAsJsonArray("Records").get(0).getAsJsonObject();
        assertEquals("aws:sns", rec.get("EventSource").getAsString());
        assertEquals("arn:sub", rec.get("EventSubscriptionArn").getAsString());
        assertEquals("hi", rec.getAsJsonObject("Sns").get("Message").getAsString());
    }
}

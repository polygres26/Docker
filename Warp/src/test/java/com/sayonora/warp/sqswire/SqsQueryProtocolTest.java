package com.sayonora.warp.sqswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Query-protocol request parsing and XML rendering (no server, no database). */
class SqsQueryProtocolTest {

    private static Map<String, String> params(String... kv) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void flattenedParametersBecomeTheJsonShape() {
        JsonObject req = SqsQueryProtocol.toJson(params(
                "Action", "CreateQueue", "Version", "2012-11-05", "QueueName", "q1",
                "Attribute.1.Name", "VisibilityTimeout", "Attribute.1.Value", "45",
                "Attribute.2.Name", "DelaySeconds", "Attribute.2.Value", "5",
                "Tag.1.Key", "env", "Tag.1.Value", "test"));
        assertEquals("q1", req.get("QueueName").getAsString());
        assertEquals("45", req.getAsJsonObject("Attributes").get("VisibilityTimeout").getAsString());
        assertEquals("5", req.getAsJsonObject("Attributes").get("DelaySeconds").getAsString());
        assertEquals("test", req.getAsJsonObject("Tags").get("env").getAsString());
        assertFalse(req.has("Action"));
    }

    @Test
    void receiveParametersAreTypedAndListed() {
        JsonObject req = SqsQueryProtocol.toJson(params(
                "QueueUrl", "http://h/000000000000/q", "MaxNumberOfMessages", "5", "WaitTimeSeconds", "3",
                "AttributeName.1", "All", "MessageAttributeName.1", "a", "MessageAttributeName.2", "b.*"));
        assertEquals(5, req.get("MaxNumberOfMessages").getAsInt());
        assertEquals(3, req.get("WaitTimeSeconds").getAsInt());
        assertEquals("All", req.getAsJsonArray("AttributeNames").get(0).getAsString());
        assertEquals(2, req.getAsJsonArray("MessageAttributeNames").size());
    }

    @Test
    void messageAttributesAndBatchEntriesNest() {
        JsonObject req = SqsQueryProtocol.toJson(params(
                "SendMessageBatchRequestEntry.1.Id", "a", "SendMessageBatchRequestEntry.1.MessageBody", "one",
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name", "k",
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType", "String",
                "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.StringValue", "v",
                "SendMessageBatchRequestEntry.2.Id", "b", "SendMessageBatchRequestEntry.2.MessageBody", "two",
                "SendMessageBatchRequestEntry.2.DelaySeconds", "7"));
        JsonArray entries = req.getAsJsonArray("Entries");
        assertEquals(2, entries.size());
        JsonObject first = entries.get(0).getAsJsonObject();
        assertEquals("one", first.get("MessageBody").getAsString());
        assertEquals("v", first.getAsJsonObject("MessageAttributes").getAsJsonObject("k").get("StringValue").getAsString());
        assertEquals(7, entries.get(1).getAsJsonObject().get("DelaySeconds").getAsInt());
    }

    @Test
    void deleteBatchEntriesAndTagKeysParse() {
        JsonObject req = SqsQueryProtocol.toJson(params(
                "DeleteMessageBatchRequestEntry.1.Id", "d1", "DeleteMessageBatchRequestEntry.1.ReceiptHandle", "rh1",
                "TagKey.1", "x", "TagKey.2", "y"));
        assertEquals("rh1", req.getAsJsonArray("Entries").get(0).getAsJsonObject().get("ReceiptHandle").getAsString());
        assertEquals(2, req.getAsJsonArray("TagKeys").size());
    }

    @Test
    void sendMessageResultRendersXmlWithNamespaceAndRequestId() {
        JsonObject result = JsonParser.parseString("{\"MessageId\":\"m-1\",\"MD5OfMessageBody\":\"abc\",\"MD5OfMessageAttributes\":\"def\","
                + "\"SequenceNumber\":\"000000000000000042\"}").getAsJsonObject();
        String xml = SqsQueryProtocol.renderResponse("SendMessage", result, "req-1");
        assertTrue(xml.contains("<SendMessageResponse xmlns=\"" + SqsQueryProtocol.NS + "\">"));
        assertTrue(xml.contains("<SendMessageResult>"));
        assertTrue(xml.contains("<MessageId>m-1</MessageId>"));
        assertTrue(xml.contains("<MD5OfMessageAttributes>def</MD5OfMessageAttributes>"));
        assertTrue(xml.contains("<SequenceNumber>000000000000000042</SequenceNumber>"));
        assertTrue(xml.contains("<ResponseMetadata><RequestId>req-1</RequestId></ResponseMetadata>"));
    }

    @Test
    void receiveMessageRendersAttributesAndEscapesBody() {
        JsonObject result = JsonParser.parseString("{\"Messages\":[{\"MessageId\":\"m\",\"ReceiptHandle\":\"r\",\"MD5OfBody\":\"b\","
                + "\"Body\":\"a<b>&\\\"c\\\"\",\"Attributes\":{\"SentTimestamp\":\"1\"},\"MD5OfMessageAttributes\":\"x\","
                + "\"MessageAttributes\":{\"k\":{\"DataType\":\"String\",\"StringValue\":\"v\"}}}]}").getAsJsonObject();
        String xml = SqsQueryProtocol.renderResponse("ReceiveMessage", result, "r1");
        assertTrue(xml.contains("<Body>a&lt;b&gt;&amp;&quot;c&quot;</Body>"), xml);
        assertTrue(xml.contains("<Attribute><Name>SentTimestamp</Name><Value>1</Value></Attribute>"));
        assertTrue(xml.contains("<MessageAttribute><Name>k</Name><Value><DataType>String</DataType><StringValue>v</StringValue></Value></MessageAttribute>"));
    }

    @Test
    void batchResultRendersSuccessAndErrorEntries() {
        JsonObject result = JsonParser.parseString("{\"Successful\":[{\"Id\":\"a\"}],\"Failed\":[{\"Id\":\"b\",\"SenderFault\":true,"
                + "\"Code\":\"ReceiptHandleIsInvalid\",\"Message\":\"bad\"}]}").getAsJsonObject();
        String xml = SqsQueryProtocol.renderResponse("DeleteMessageBatch", result, "r");
        assertTrue(xml.contains("<DeleteMessageBatchResultEntry><Id>a</Id></DeleteMessageBatchResultEntry>"));
        assertTrue(xml.contains("<BatchResultErrorEntry><Id>b</Id><SenderFault>true</SenderFault><Code>ReceiptHandleIsInvalid</Code>"
                + "<Message>bad</Message></BatchResultErrorEntry>"));
    }

    @Test
    void actionsWithoutAResultOmitTheResultElement() {
        String xml = SqsQueryProtocol.renderResponse("DeleteQueue", new JsonObject(), "r");
        assertFalse(xml.contains("DeleteQueueResult"));
        assertTrue(xml.contains("<DeleteQueueResponse"));
    }

    @Test
    void errorDocumentFollowsTheQueryProtocolShape() {
        String xml = SqsQueryProtocol.renderError("AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.", "rid", true);
        assertTrue(xml.contains("<ErrorResponse"));
        assertTrue(xml.contains("<Error><Type>Sender</Type><Code>AWS.SimpleQueueService.NonExistentQueue</Code>"
                + "<Message>The specified queue does not exist.</Message>"));
        assertTrue(xml.contains("<RequestId>rid</RequestId>"));
    }

    @Test
    void queryCodesForJsonTypesMatchRealSqs() {
        assertEquals("AWS.SimpleQueueService.NonExistentQueue", SqsException.queryCodeFor("QueueDoesNotExist"));
        assertEquals("QueueAlreadyExists", SqsException.queryCodeFor("QueueNameExists"));
        assertEquals("AWS.SimpleQueueService.BatchEntryIdsNotDistinct", SqsException.queryCodeFor("BatchEntryIdsNotDistinct"));
        assertEquals("InvalidParameterValue", SqsException.queryCodeFor("InvalidParameterValue"));
    }
}

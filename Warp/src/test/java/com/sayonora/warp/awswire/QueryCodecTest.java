package com.sayonora.warp.awswire;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class QueryCodecTest {

    @Test
    void decodesListsMapsAndNestedStructures() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put("Action", "PublishBatch");
        p.put("TopicArn", "arn:t");
        p.put("PublishBatchRequestEntries.member.1.Id", "a");
        p.put("PublishBatchRequestEntries.member.1.Message", "m1");
        p.put("PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Name", "k");
        p.put("PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.DataType", "String");
        p.put("PublishBatchRequestEntries.member.1.MessageAttributes.entry.1.Value.StringValue", "v");
        p.put("PublishBatchRequestEntries.member.2.Id", "b");
        p.put("PublishBatchRequestEntries.member.2.Message", "m2");
        p.put("Attributes.entry.1.key", "RawMessageDelivery");
        p.put("Attributes.entry.1.value", "true");
        p.put("Tags.member.1.Key", "env");
        p.put("Tags.member.1.Value", "test");
        JsonObject o = QueryCodec.toJson(p);
        assertEquals("arn:t", o.get("TopicArn").getAsString());
        assertTrue(!o.has("Action"));
        var entries = o.getAsJsonArray("PublishBatchRequestEntries");
        assertEquals(2, entries.size());
        assertEquals("m1", entries.get(0).getAsJsonObject().get("Message").getAsString());
        assertEquals("v", entries.get(0).getAsJsonObject().getAsJsonObject("MessageAttributes").getAsJsonObject("k").get("StringValue").getAsString());
        assertEquals("b", entries.get(1).getAsJsonObject().get("Id").getAsString());
        assertEquals("true", o.getAsJsonObject("Attributes").get("RawMessageDelivery").getAsString());
        assertEquals("env", o.getAsJsonArray("Tags").get(0).getAsJsonObject().get("Key").getAsString());
    }

    @Test
    void listsAreOrderedNumericallyNotLexically() {
        Map<String, String> p = new LinkedHashMap<>();
        for (int i = 12; i >= 1; i--) {
            p.put("Names.member." + i, "n" + i);
        }
        var a = QueryCodec.toJson(p).getAsJsonArray("Names");
        assertEquals("n1", a.get(0).getAsString());
        assertEquals("n10", a.get(9).getAsString());
        assertEquals("n12", a.get(11).getAsString());
    }

    @Test
    void emptyRequestDecodesToAnEmptyObject() {
        assertEquals(0, QueryCodec.toJson(Map.of("Action", "GetCallerIdentity", "Version", "2011-06-15")).size());
    }

    private static final AwsService SVC = new AwsService() {
        @Override
        public String id() {
            return "x";
        }

        @Override
        public String metricsProtocol() {
            return "x";
        }

        @Override
        public Set<String> signingNames() {
            return Set.of();
        }

        @Override
        public Set<String> targetPrefixes() {
            return Set.of();
        }

        @Override
        public String xmlNamespace() {
            return "http://x/";
        }

        @Override
        public Set<String> mapFields() {
            return Set.of("Attributes");
        }

        @Override
        public JsonObject invoke(String op, JsonObject req, Call call) {
            return null;
        }
    };

    @Test
    void rendersListsAsMemberAndMapsAsEntries() {
        JsonObject r = JsonParser.parseString("{\"Topics\":[{\"TopicArn\":\"a&b\"}],\"Attributes\":{\"k\":\"v\"},\"N\":3}").getAsJsonObject();
        String xml = QueryCodec.renderResponse(SVC, "ListTopics", r, "rid");
        assertTrue(xml.contains("<ListTopicsResponse xmlns=\"http://x/\">"));
        assertTrue(xml.contains("<Topics><member><TopicArn>a&amp;b</TopicArn></member></Topics>"));
        assertTrue(xml.contains("<Attributes><entry><key>k</key><value>v</value></entry></Attributes>"));
        assertTrue(xml.contains("<N>3</N>"));
        assertTrue(xml.contains("<ResponseMetadata><RequestId>rid</RequestId></ResponseMetadata>"));
    }

    @Test
    void emptyResultKeepsAnEmptyResultElementAndErrorsAreEscaped() {
        assertTrue(QueryCodec.renderResponse(SVC, "UntagResource", new JsonObject(), "r").contains("<UntagResourceResult></UntagResourceResult>"));
        String err = QueryCodec.renderError(SVC, "NotFound", "a < b", "rid", true);
        assertTrue(err.contains("<Type>Sender</Type><Code>NotFound</Code><Message>a &lt; b</Message>"));
    }
}

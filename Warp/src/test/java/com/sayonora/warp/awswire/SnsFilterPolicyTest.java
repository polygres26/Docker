package com.sayonora.warp.awswire;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SnsFilterPolicyTest {

    private static Map<String, SnsFilterPolicy.Attr> attrs(String... kv) {
        Map<String, SnsFilterPolicy.Attr> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 3) {
            m.put(kv[i], new SnsFilterPolicy.Attr(kv[i + 1], kv[i + 2]));
        }
        return m;
    }

    private static boolean attr(String policy, Map<String, SnsFilterPolicy.Attr> a) {
        return SnsFilterPolicy.matchesAttributes(SnsFilterPolicy.parse(policy, false), a);
    }

    private static boolean body(String policy, String body) {
        return SnsFilterPolicy.matchesBody(SnsFilterPolicy.parse(policy, true), body);
    }

    @Test
    void exactStringMatchIsOrWithinAKeyAndAndAcrossKeys() {
        assertTrue(attr("{\"color\":[\"blue\",\"red\"]}", attrs("color", "String", "red")));
        assertFalse(attr("{\"color\":[\"blue\",\"red\"]}", attrs("color", "String", "green")));
        assertTrue(attr("{\"color\":[\"blue\"],\"size\":[\"L\"]}", attrs("color", "String", "blue", "size", "String", "L")));
        assertFalse(attr("{\"color\":[\"blue\"],\"size\":[\"L\"]}", attrs("color", "String", "blue", "size", "String", "S")));
        assertFalse(attr("{\"color\":[\"blue\"]}", attrs()), "a missing attribute never matches a value");
    }

    @Test
    void numericMatching() {
        assertTrue(attr("{\"n\":[{\"numeric\":[\">\",5,\"<=\",10]}]}", attrs("n", "Number", "10")));
        assertFalse(attr("{\"n\":[{\"numeric\":[\">\",5,\"<=\",10]}]}", attrs("n", "Number", "11")));
        assertTrue(attr("{\"n\":[{\"numeric\":[\"=\",42]}]}", attrs("n", "Number", "42.0")));
        assertTrue(attr("{\"n\":[42]}", attrs("n", "Number", "42")), "exact number equals by value");
        assertFalse(attr("{\"n\":[{\"numeric\":[\">\",1]}]}", attrs("n", "String", "5")), "numeric rules need a Number attribute");
    }

    @Test
    void prefixSuffixIgnoreCaseAndCidr() {
        assertTrue(attr("{\"k\":[{\"prefix\":\"ab\"}]}", attrs("k", "String", "abc")));
        assertFalse(attr("{\"k\":[{\"prefix\":\"ab\"}]}", attrs("k", "String", "cab")));
        assertTrue(attr("{\"k\":[{\"suffix\":\"bc\"}]}", attrs("k", "String", "abc")));
        assertTrue(attr("{\"k\":[{\"equals-ignore-case\":\"ABC\"}]}", attrs("k", "String", "abc")));
        assertTrue(attr("{\"ip\":[{\"cidr\":\"10.0.0.0/8\"}]}", attrs("ip", "String", "10.1.2.3")));
        assertFalse(attr("{\"ip\":[{\"cidr\":\"10.0.0.0/8\"}]}", attrs("ip", "String", "11.1.2.3")));
    }

    @Test
    void existsAndAnythingBut() {
        assertTrue(attr("{\"k\":[{\"exists\":true}]}", attrs("k", "String", "x")));
        assertFalse(attr("{\"k\":[{\"exists\":true}]}", attrs()));
        assertTrue(attr("{\"k\":[{\"exists\":false}]}", attrs()));
        assertFalse(attr("{\"k\":[{\"exists\":false}]}", attrs("k", "String", "x")));
        assertTrue(attr("{\"k\":[{\"anything-but\":[\"a\",\"b\"]}]}", attrs("k", "String", "c")));
        assertFalse(attr("{\"k\":[{\"anything-but\":[\"a\",\"b\"]}]}", attrs("k", "String", "a")));
        assertFalse(attr("{\"k\":[{\"anything-but\":\"a\"}]}", attrs()), "anything-but needs the attribute to exist");
        assertTrue(attr("{\"k\":[{\"anything-but\":{\"prefix\":\"x\"}}]}", attrs("k", "String", "abc")));
    }

    @Test
    void stringArrayAttributeMatchesWhenAnyElementDoes() {
        assertTrue(attr("{\"tags\":[\"b\"]}", attrs("tags", "String.Array", "[\"a\",\"b\"]")));
        assertFalse(attr("{\"tags\":[\"z\"]}", attrs("tags", "String.Array", "[\"a\",\"b\"]")));
    }

    @Test
    void orOperator() {
        String p = "{\"$or\":[{\"a\":[\"1\"]},{\"b\":[\"2\"]}]}";
        assertTrue(attr(p, attrs("b", "String", "2")));
        assertFalse(attr(p, attrs("a", "String", "9", "b", "String", "9")));
    }

    @Test
    void messageBodyScopeDescendsIntoNestedKeys() {
        assertTrue(body("{\"event\":[\"order\"]}", "{\"event\":\"order\"}"));
        assertFalse(body("{\"event\":[\"order\"]}", "{\"event\":\"refund\"}"));
        assertTrue(body("{\"store\":{\"city\":[\"seattle\"]}}", "{\"store\":{\"city\":\"seattle\"}}"));
        assertFalse(body("{\"store\":{\"city\":[\"seattle\"]}}", "{\"store\":{\"city\":\"boston\"}}"));
        assertTrue(body("{\"n\":[{\"numeric\":[\">\",1]}]}", "{\"n\":2}"));
        assertTrue(body("{\"tags\":[\"b\"]}", "{\"tags\":[\"a\",\"b\"]}"), "an array value matches when any element does");
        assertTrue(body("{\"ok\":[true]}", "{\"ok\":true}"));
        assertFalse(body("{\"ok\":[true]}", "{\"ok\":\"true\"}"), "booleans and strings differ in a body");
        assertFalse(body("{\"event\":[\"order\"]}", "not json"));
        assertTrue(body("{\"items\":{\"sku\":[\"a\"]}}", "{\"items\":[{\"sku\":\"z\"},{\"sku\":\"a\"}]}"), "nested policy over an array of objects");
    }

    @Test
    void invalidPoliciesAreRefused() {
        assertThrows(IllegalArgumentException.class, () -> SnsFilterPolicy.parse("[1]", false));
        assertThrows(IllegalArgumentException.class, () -> SnsFilterPolicy.parse("{\"a\":\"x\"}", false));
        assertThrows(IllegalArgumentException.class, () -> SnsFilterPolicy.parse("{\"a\":[{\"bogus\":1}]}", false));
        assertThrows(IllegalArgumentException.class, () -> SnsFilterPolicy.parse("{\"a\":{\"b\":[\"x\"]}}", false),
                "nested policies need the MessageBody scope");
        assertThrows(IllegalArgumentException.class, () -> SnsFilterPolicy.parse("{\"a\":[{\"numeric\":[\">\"]}]}", false));
        SnsFilterPolicy.parse("{\"a\":{\"b\":[\"x\"]}}", true);
    }

    @Test
    void envelopeAttributeJsonHasTypeAndValue() {
        JsonObject o = SnsEnvelope.attributesJson(attrs("color", "String", "blue"), Map.of());
        assertTrue(o.getAsJsonObject("color").get("Type").getAsString().equals("String"));
        assertTrue(o.getAsJsonObject("color").get("Value").getAsString().equals("blue"));
    }
}

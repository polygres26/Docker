package com.nexagres.migration.connectors.dynamo;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.util.Base64;
import java.util.Map;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A faithful reproduction of dynamowire's own item-attribute serialization convention (see {@code
 * com.nexagres.wire.dynamowire.AttributeValue#toJson}/{@code PgItemStore#itemToJson} in the {@code
 * wire} module) -- NOT independent reinvention, same principle as {@code MongoBsonJson} in the
 * Mongo connector. This has to produce byte-identical {@code item} jsonb content to what
 * dynamowire itself writes for the same item, or a table populated partly by live dynamowire
 * traffic and partly by this connector would have two different serializations of equivalent
 * attribute values sitting in the same column.
 *
 * <p>Works against the AWS SDK's OWN {@link AttributeValue} type (from a real {@code
 * DynamoDbClient}/{@code DynamoDbStreamsClient} response) rather than wire's internal one --
 * they're structurally the same DynamoDB data model (S/N/B/BOOL/NULL/M/L/SS/NS/BS), but wire's
 * {@code AttributeValue} is that module's own wire-protocol-parsing type, not meant to be reused
 * across the module boundary (this connector has no dependency on wire's internal classes at all,
 * only its generated gRPC stubs -- see {@code migration/pom.xml}'s own dependency comment).
 *
 * <p>DynamoDB's JSON wire protocol represents {@code B}/{@code BS} (binary) values as base64 text
 * -- that's exactly what wire's own {@code AttributeValue} stores verbatim as its {@code scalar}
 * for a {@code B} value (whatever base64 text arrived over the wire, no re-encoding). The AWS SDK
 * instead decodes binary values into real {@link SdkBytes}, so this class re-encodes them back to
 * base64 to match what dynamowire itself would have stored.
 */
final class DynamoJson {

    private DynamoJson() {
    }

    static JsonObject itemToJson(Map<String, AttributeValue> item) {
        JsonObject obj = new JsonObject();
        item.forEach((name, value) -> obj.add(name, attributeToJson(value)));
        return obj;
    }

    static JsonObject attributeToJson(AttributeValue v) {
        JsonObject obj = new JsonObject();
        if (v.s() != null) {
            obj.add("S", new JsonPrimitive(v.s()));
        } else if (v.n() != null) {
            obj.add("N", new JsonPrimitive(v.n()));
        } else if (v.b() != null) {
            obj.add("B", new JsonPrimitive(base64(v.b())));
        } else if (v.bool() != null) {
            obj.add("BOOL", new JsonPrimitive(v.bool()));
        } else if (Boolean.TRUE.equals(v.nul())) {
            obj.add("NULL", new JsonPrimitive(true));
        } else if (v.hasM()) {
            obj.add("M", itemToJson(v.m()));
        } else if (v.hasL()) {
            JsonArray arr = new JsonArray();
            v.l().forEach(e -> arr.add(attributeToJson(e)));
            obj.add("L", arr);
        } else if (v.hasSs()) {
            obj.add("SS", stringArray(v.ss()));
        } else if (v.hasNs()) {
            obj.add("NS", stringArray(v.ns()));
        } else if (v.hasBs()) {
            JsonArray arr = new JsonArray();
            v.bs().forEach(b -> arr.add(base64(b)));
            obj.add("BS", arr);
        } else {
            throw new IllegalArgumentException("AttributeValue with no recognized value set: " + v);
        }
        return obj;
    }

    private static JsonArray stringArray(Iterable<String> values) {
        JsonArray arr = new JsonArray();
        values.forEach(arr::add);
        return arr;
    }

    private static String base64(SdkBytes bytes) {
        return Base64.getEncoder().encodeToString(bytes.asByteArray());
    }

    /** The raw text form of a key attribute (partition key or sort key), matching wire's own
     * {@code PgItemStore#keyToken} exactly: {@code S} and {@code N} keep their literal text as-is
     * (a Number key's decimal text, unparsed), {@code B} is base64-encoded the same way any other
     * binary value is (see this class's own javadoc). Real DynamoDB key types are always S, N, or
     * B -- no other {@link AttributeValue} shape is valid as a key, so anything else here is a
     * genuine schema violation, not a case to silently handle. */
    static String keyText(AttributeValue v) {
        if (v.s() != null) {
            return v.s();
        }
        if (v.n() != null) {
            return v.n();
        }
        if (v.b() != null) {
            return base64(v.b());
        }
        throw new IllegalArgumentException("Key attribute value must be S, N, or B, got: " + v);
    }
}
